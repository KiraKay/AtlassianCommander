# path: main_updated.py
# -----------------------------------------------------------------------------
# Jira Cleanup & Recovery Tool
#
# WHAT THIS APP DOES (high-level)
# ------------------------------
# - GUI (Tkinter) with 4 tabs:
#     1) Cleanup: find and delete *unused* Jira items (e.g., custom fields).
#     2) Recover: list *deleted* items (from Jira APIs + your JSON backups)
#                 and offer best-effort recovery (where Jira Cloud allows it).
#     3) Logs & Backups: quick access to folders.
#     4) Authentication: set domain/email/token, save/load profiles.
#
# HOW IT WORKS (core flow)
# ------------------------
# - All *slow* work (HTTP calls, backup scans) runs in background threads
#   via self._thread(...). Only the *main thread* touches Tk widgets.
# - Background task functions (e.g., _load_unused_items, _load_deleted_items)
#   do the work, then schedule UI updates with self.after(0, ...).
# - HTTP calls go through a pooled requests.Session (self._http) with retries
#   for GET/HEAD/OPTIONS. Auth uses email/token (HTTPBasicAuth).
# - Before deleting anything, the app makes a *full backup bundle* for the
#   checked items via BackupManager.backup_items(...).
# - The “Recover” tab pulls from Jira “deleted” APIs and optionally *merges*
#   those lists with any names/IDs found in your JSON backups so you see
#   more complete/pretty names when possible.
#
# KEY CALL CHAINS (who calls whom)
# --------------------------------
# [Cleanup tab]
#   Button "Find Unused"      → _thread(_load_unused_items, item_type)
#                              → get_unused_* (per type)
#                              → (Custom Fields) _field_is_unused (parallel)
#                              → self.after(..., _render_items)
#   Button "Delete Checked"   → _thread(_delete_unused_items, item_type)
#                              → BackupManager.backup_items(...)
#                              → delete_* (per type)    [or DRY RUN]
#
# [Recover tab]
#   "Refresh List" / toggles  → _thread(_load_deleted_items, item_type)
#                              → get_deleted_* (per type, API + backups merge)
#                              → self.after(..., _render_items)
#   "Recover Checked"         → _thread(_recover_selected, item_type)
#                              → recover_deleted_* (per type)  [or DRY RUN]
#
# THREADING RULE
# --------------
# - Never touch Tk UI from worker threads. This file uses self.after(0, fn)
#   to marshal UI updates to the main thread.
#
# NOTES
# -----
# - Jira Cloud limitations: Status delete/recover is not available via REST.
# - “Unused” logic is conservative for most types (not locked ≈ candidate).
#   For Custom Fields we *actually* check screen placement (faster & useful).
# - The “Include backups” toggle on Recover lets you combine API results with
#   your on-disk JSON backup bundles for better names/coverage.
# -----------------------------------------------------------------------------


from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import json
import os
import platform
import subprocess
import threading
from datetime import datetime
from typing import Any, Dict, List, Callable, Optional, Tuple, Set
from queue import Queue, Empty

import requests
import tkinter as tk
from requests.auth import HTTPBasicAuth
from tkinter import ttk, messagebox

try:
    from openpyxl import Workbook
except Exception:
    Workbook = None

BACKUP_DIR = "../backup/backups"
EXPORT_DIR = "../backup/exports"
LOG_DIR = "../logs"
PROFILES_FILE = "../auth_profiles.json"
TIMEOUT = 30

os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


def open_folder(path: str) -> None:
    """Open a folder in the OS file explorer."""
    if platform.system() == "Windows":
        os.startfile(path)  # nosec
    elif platform.system() == "Darwin":
        subprocess.call(["open", path])  # nosec
    else:
        subprocess.call(["xdg-open", path])  # nosec


def load_auth_profiles() -> Dict[str, Dict[str, str]]:
    """Load saved auth profiles (domain/email/token) from disk."""
    try:
        if not os.path.exists(PROFILES_FILE):
            return {}
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            out: Dict[str, Dict[str, str]] = {}
            for i, entry in enumerate(data):
                if isinstance(entry, dict):
                    name = entry.get("name") or f"profile_{i+1}"
                    out[name] = {
                        "domain": entry.get("domain", ""),
                        "email": entry.get("email", ""),
                        "token": entry.get("token", ""),
                    }
            return out
    except Exception:
        pass
    return {}


def save_auth_profiles(profiles: Dict[str, Dict[str, str]]) -> None:
    """Save auth profiles to disk."""
    with open(PROFILES_FILE, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2)


def log_to_file(job_name: str, message: str) -> None:
    """Append a timestamped line to a log file in ./logs."""
    log_file = os.path.join(LOG_DIR, f"{job_name}_{datetime.now().strftime('%Y%m%d')}.log")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")


def backup_to_file(data: Any, name: str, item_type: str) -> str:
    """
    Write a JSON bundle into a per-type folder under backup/backups.
    Returns the file path.
    """
    folder = os.path.join(BACKUP_DIR, item_type.replace(" ", "_").lower())
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return filename


class BackupManager:
    """
    BackupManager: fetches full object graphs before deletion.
    Called by: JiraToolGUI._delete_unused_items → backup_items(...)

    For each selected ID, calls the matching _collect_* to assemble a bundle.
    """

    def __init__(self, base_url: str, auth: HTTPBasicAuth) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = auth

    def _get_json(self, path: str) -> Optional[Any]:
        """Small helper for GET JSON with base_url + auth."""
        try:
            url = path if path.startswith("http") else f"{self.base_url}{path}"
            resp = requests.get(url, auth=self.auth, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log_to_file("backup", f"GET {path} failed: {e}")
            return None

    # ---- collectors (called by backup_items) ----

    def _collect_custom_field(self, field_id: str) -> Dict[str, Any]:
        # Field metadata + contexts + options + screens containing this field
        field = self._get_json(f"/rest/api/3/field/{field_id}") or {}
        contexts = self._get_json(f"/rest/api/3/field/{field_id}/contexts") or {}
        context_items = contexts.get("values", contexts) if isinstance(contexts, dict) else contexts
        options_by_ctx: Dict[str, Any] = {}
        if isinstance(context_items, list):
            for ctx in context_items:
                ctx_id = ctx.get("id")
                if ctx_id:
                    opts = self._get_json(f"/rest/api/3/field/{field_id}/context/{ctx_id}/option") or {}
                    options_by_ctx[str(ctx_id)] = opts
        screens = self._get_json(f"/rest/api/3/screens?fieldId={field_id}") or {}
        return {"field": field, "contexts": contexts, "optionsByContext": options_by_ctx, "screens": screens}

    def _collect_workflow(self, workflow_name: str) -> Dict[str, Any]:
        # All workflows + which schemes reference a given workflow name (best-effort)
        workflows_all = self._get_json("/rest/api/3/workflow") or {}
        schemes = self._get_json("/rest/api/3/workflowscheme") or {}
        schemes_list = schemes.get("values", []) if isinstance(schemes, dict) else []
        target = (workflow_name or "").lower()
        schemes_referencing = [s for s in schemes_list if target in json.dumps(s).lower()]
        return {"workflowsAll": workflows_all, "referencedBySchemes": schemes_referencing}

    def _collect_workflow_scheme(self, scheme_id: str) -> Dict[str, Any]:
        scheme = self._get_json(f"/rest/api/3/workflowscheme/{scheme_id}") or {}
        assoc = self._get_json(f"/rest/api/2/workflowscheme/project?workflowSchemeId={scheme_id}") or {}
        return {"scheme": scheme, "projectAssociations": assoc}

    def _collect_screen(self, screen_id: str) -> Dict[str, Any]:
        # Screen + tabs + fields per tab
        screen = self._get_json(f"/rest/api/3/screens/{screen_id}") or {}
        tabs = self._get_json(f"/rest/api/3/screens/{screen_id}/tabs") or []
        tab_fields: Dict[str, Any] = {}
        if isinstance(tabs, list):
            for t in tabs:
                tid = t.get("id")
                if tid:
                    fields = self._get_json(f"/rest/api/3/screens/{screen_id}/tabs/{tid}/fields") or []
                    tab_fields[str(tid)] = fields
        return {"screen": screen, "tabs": tabs, "tabFields": tab_fields}

    def _collect_screen_scheme(self, scheme_id: str) -> Dict[str, Any]:
        scheme = self._get_json(f"/rest/api/3/screenscheme/{scheme_id}") or {}
        return {"screenScheme": scheme}

    def _collect_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        dash = self._get_json(f"/rest/api/3/dashboard/{dashboard_id}") or {}
        gadgets = self._get_json(f"/rest/api/3/dashboard/{dashboard_id}/gadget") or {}
        return {"dashboard": dash, "gadgets": gadgets}

    def _collect_filter(self, filter_id: str) -> Dict[str, Any]:
        filt = self._get_json(f"/rest/api/3/filter/{filter_id}") or {}
        shares = self._get_json(f"/rest/api/2/filter/{filter_id}/permission") or {}
        return {"filter": filt, "sharePermissions": shares}

    def _collect_status(self, status_id_or_name: str) -> Dict[str, Any]:
        status = self._get_json(f"/rest/api/3/status/{status_id_or_name}") or {}
        return {"status": status}

    def _resolve_name(self, item_type: str, _id: str, bundle: Dict[str, Any]) -> str:
        """Extract a display name for the backup bundle's meta."""
        try:
            if item_type == "Custom Fields":
                return (bundle.get("field") or {}).get("name") or _id
            if item_type == "Workflows":
                return _id
            if item_type == "Workflow Schemes":
                return (bundle.get("scheme") or {}).get("name") or _id
            if item_type == "Screens":
                return (bundle.get("screen") or {}).get("name") or _id
            if item_type == "Screen Schemes":
                return (bundle.get("screenScheme") or {}).get("name") or _id
            if item_type == "Dashboards":
                return (bundle.get("dashboard") or {}).get("name") or _id
            if item_type == "Filters":
                return (bundle.get("filter") or {}).get("name") or _id
            if item_type == "Workflow Statuses":
                s = bundle.get("status") or {}
                return s.get("name") or _id
        except Exception:
            pass
        return _id

    def backup_items(self, item_type: str, ids: List[str]) -> str:
        """
        Build a single JSON bundle for the given item_type and list of IDs.
        Called by: JiraToolGUI._delete_unused_items
        """
        collectors: Dict[str, Callable[[str], Dict[str, Any]]] = {
            "Custom Fields": self._collect_custom_field,
            "Workflows": self._collect_workflow,
            "Workflow Schemes": self._collect_workflow_scheme,
            "Screens": self._collect_screen,
            "Screen Schemes": self._collect_screen_scheme,
            "Dashboards": self._collect_dashboard,
            "Filters": self._collect_filter,
            "Workflow Statuses": self._collect_status,
        }
        coll = collectors.get(item_type)
        payload: Dict[str, Any] = {
            "itemType": item_type,
            "collectedAt": datetime.now().isoformat() + "Z",
            "items": {},
        }
        if not coll:
            payload["note"] = "No collector implemented for this type."
        else:
            for _id in ids:
                try:
                    bundle = coll(_id)
                except Exception as e:
                    log_to_file("backup", f"Collector failed for {item_type}:{_id}: {e}")
                    bundle = {"error": str(e)}
                name = self._resolve_name(item_type, _id, bundle)
                payload["items"][_id] = {"meta": {"id": _id, "name": name}, **bundle}
        fname = backup_to_file(payload, f"full_backup_{item_type.replace(' ', '_').lower()}", item_type)
        log_to_file("backup", f"Backed up {len(ids)} {item_type} -> {fname}")
        return fname


class JiraToolGUI(tk.Tk):
    """
    Main application window.
    Constructs tabs and wires up handlers that kick off background work.
    """

    def __init__(self) -> None:
        super().__init__()
        self.title("Atlassian Commander by KIngram")
        self.geometry("1150x800")
        style = ttk.Style(self)
        style.theme_use("clam")

        # ---- global state & auth (used by HTTP helpers) ----
        self.dry_run = tk.BooleanVar(value=True)
        self.current_domain = ""  # <- indentation FIX ensured
        self.current_email = ""   # <- indentation FIX ensured
        self.current_token = ""   # <- indentation FIX ensured

        # Root notebook (tabs)
        self.tabs = ttk.Notebook(self)
        self.tabs.pack(fill="both", expand=True)

        # Tab frames
        self.cleanup_tab = ttk.Frame(self.tabs)
        self.recover_tab = ttk.Frame(self.tabs)
        self.logs_tab = ttk.Frame(self.tabs)
        self.auth_tab = ttk.Frame(self.tabs)

        # Add tabs to notebook
        self.tabs.add(self.cleanup_tab, text="Cleanup")
        self.tabs.add(self.recover_tab, text="Recover")
        self.tabs.add(self.logs_tab, text="Logs & Backups")
        self.tabs.add(self.auth_tab, text="Authentication")

        # ---- per-tab state containers (tree widgets + selections etc.) ----
        self.cleanup_trees: Dict[str, ttk.Treeview] = {}
        self.cleanup_checked: Dict[str, set[str]] = {}
        self.cleanup_sort_orders: Dict[str, tk.BooleanVar] = {}
        self.cleanup_delete_flags: Dict[str, tk.BooleanVar] = {}
        self.cleanup_search_vars: Dict[str, tk.StringVar] = {}
        self.cleanup_last_items: Dict[str, List[Dict[str, Any]]] = {}
        self.cleanup_hidden_locked_labels: Dict[str, ttk.Label] = {}
        self.cleanup_hidden_counts: Dict[str, int] = {}

        self.recover_trees: Dict[str, ttk.Treeview] = {}
        self.recover_checked: Dict[str, set[str]] = {}
        self.recover_sort_orders: Dict[str, tk.BooleanVar] = {}
        self.recover_search_vars: Dict[str, tk.StringVar] = {}
        self.recover_last_items: Dict[str, List[Dict[str, Any]]] = {}

        # Recover toggles
        self.recover_include_backups = tk.BooleanVar(value=True)
        self.recover_deep_scan = tk.BooleanVar(value=True)

        # ---- status bar: background thread messages → queue → label ----
        self.status_queue: Queue[str] = Queue()
        self.status_var = tk.StringVar(value="Ready.")
        status_frame = ttk.Frame(self)
        status_frame.pack(fill="x", side="bottom")
        ttk.Separator(status_frame, orient="horizontal").pack(fill="x")
        self.status_label = ttk.Label(status_frame, textvariable=self.status_var, anchor="w")
        self.status_label.pack(fill="x", padx=8, pady=4)
        self.after(100, lambda *args: self._pump_status_queue())  # start the status pump loop

        # ---- HTTP session with pooling + safe retries (GET/HEAD/OPTIONS) ----
        self._http = requests.Session()
        self._http.headers.update({"Accept": "application/json"})
        _retries = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
            respect_retry_after_header=True,
        )
        _adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=_retries)
        self._http.mount("https://", _adapter)
        self._http.mount("http://", _adapter)

        # Session cache: field_id -> bool (True if unused)
        self._screens_for_field_cache: Dict[str, bool] = {}

        # Build all tabs (also wires up button commands → background tasks)
        self._build_cleanup_tab()
        self._build_recover_tab()
        self._build_logs_tab()
        self._build_auth_tab()

    # ---------- thread-safe message boxes ----------
    def _show_message(self, kind: str, title: str, text: str) -> None:
        """Schedule a messagebox on the main thread."""
        def _do():
            if kind == "info":
                messagebox.showinfo(title, text)
            elif kind == "warning":
                messagebox.showwarning(title, text)
            elif kind == "error":
                messagebox.showerror(title, text)
        self.after(0, lambda *args: _do())

    # ---------- usage/lock helpers ----------

    def _workflows_in_use(self) -> set[str]:
        """
        Collect workflow names that appear in any workflow scheme mapping.
        Why: Only show workflows not referenced by any scheme.
        """
        used: set[str] = set()
        data = self._get(f"{self.current_domain}/rest/api/3/workflowscheme", "workflow schemes (usage scan)")
        schemes = data.get("values", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        for sch in schemes:
            # Common shapes seen in Cloud responses
            wf_mappings = sch.get("issueTypeMappings") or sch.get("mappings") or []
            default_wf = sch.get("defaultWorkflow") or sch.get("default") or {}
            if isinstance(default_wf, dict):
                name = str(default_wf.get("workflowName") or default_wf.get("name") or "").strip()
                if name:
                    used.add(name)
            for m in wf_mappings:
                name = str(m.get("workflowName") or m.get("name") or "").strip()
                if name:
                    used.add(name)
        return used

    def _screen_schemes_in_use(self) -> set[str]:
        """
        Collect Screen Scheme IDs that are referenced by any Issue Type Screen Scheme.
        Why: A screen scheme is 'used' if any issue type screen scheme points to it.
        """
        used: set[str] = set()
        data = self._get(f"{self.current_domain}/rest/api/3/issuetypescreenscheme", "issue type screen schemes (usage scan)")
        values = data.get("values", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        for itss in values:
            # typical: {"id": "...","issueTypeMappings":[{"issueTypeId":"...","screenSchemeId":"123"}], ...}
            for m in itss.get("issueTypeMappings") or []:
                ssid = str(m.get("screenSchemeId") or "").strip()
                if ssid:
                    used.add(ssid)
        return used

    def _screens_in_use(self) -> set[str]:
        """
        Collect Screen IDs referenced by any Screen Scheme (create/edit/view/default).
        Why: Only show screens not referenced by any scheme variant.
        """
        used: set[str] = set()
        data = self._get(f"{self.current_domain}/rest/api/3/screenscheme", "screen schemes (usage scan)")
        schemes = data.get("values", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        for sch in schemes:
            # common: {"id":"..","name":"..","screens":{"default":"1","create":"2","edit":"3","view":"4"}}
            screens = sch.get("screens") or {}
            for key in ("default", "create", "edit", "view"):
                sid = screens.get(key)
                if sid is not None:
                    used.add(str(sid))
        return used

    def _field_is_unused(self, field_id: str) -> bool:
        """
        Returns True if a custom field is not placed on any screen.
        Used by: get_unused_custom_fields (parallel).
        """
        if field_id in self._screens_for_field_cache:
            return self._screens_for_field_cache[field_id]
        try:
            url = f"{self.current_domain}/rest/api/3/screens?fieldId={field_id}"
            data = self._get(url, f"screens for field {field_id}")
            # If _get() yielded an error dict, assume 'used' (conservative)
            if isinstance(data, dict) and data.get("__error__"):
                self._screens_for_field_cache[field_id] = False
                return False
            count = len(data.get("values", [])) if isinstance(data, dict) else (
                len(data) if isinstance(data, list) else 0)
            is_unused = (count == 0)
        except Exception:
            self._screens_for_field_cache[field_id] = False
            return False
        self._screens_for_field_cache[field_id] = is_unused
        return is_unused

    def get_unused_custom_fields(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/field"
        fields = self._get(url, "custom fields")
        return [f for f in fields if isinstance(f, dict) and f.get("custom") and not f.get("screens")]


    # ---------- threading ----------
    def _thread(self, fn: Callable, *args) -> None:
        """
        Run a task in a background thread. This is used by all buttons that
        would otherwise block the UI. The task itself must not touch Tk;
        schedule UI updates with self.after(0, ...).
        """
        def runner():
            try:
                self._emit_status(f"Started: {getattr(fn, '__name__', 'task')}")
                result = fn(*args)  # background work only
                self.after(0, lambda *args: self._emit_status(f"Done: {getattr(fn, '__name__', 'task')}"))
                return result
            except Exception as e:
                self._emit_status(f"Error: {e}")
                log_to_file("runtime", f"Threaded task error: {e}")

        threading.Thread(target=runner, daemon=True).start()

    # ---------- status ----------
    def _emit_status(self, text: str) -> None:
        """Queue a status line; _pump_status_queue displays it periodically."""
        ts = datetime.now().strftime("%H:%M:%S")
        self.status_queue.put(f"[{ts}] {text}")

    def _pump_status_queue(self) -> None:
        """
        Periodic main-thread loop that drains the status queue
        and updates the status label. Called first in __init__.
        """
        try:
            while True:
                msg = self.status_queue.get_nowait()
                self.status_var.set(msg)
        except Empty:
            pass
        finally:
            self.after(150, lambda *args: self._pump_status_queue())

    # ---------- backups helpers (used by Recover UI to pretty-up names) ----------
    def _is_id_like(self, item_type: str, name: str) -> bool:
        """Heuristic to detect ID-looking strings; used to pick nicer names."""
        if not name:
            return True
        name = str(name).strip().lower()
        if item_type == "Custom Fields":
            return name.startswith("customfield_")
        return False

    def _collect_backup_name_map(self, item_type: str) -> Dict[str, str]:
        """
        Scan backup JSONs and build {id: best_name} to repair ugly names
        (IDs) in UI lists. Called by: _repair_names_from_cache.
        """
        name_by_id: Dict[str, str] = {}
        for path in self._iter_backup_json_paths():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # only use matching itemType bundles (or untyped)
                if not isinstance(data, dict) or (data.get("itemType") and data.get("itemType") != item_type):
                    continue
                items = data.get("items")
                if not isinstance(items, dict):
                    continue
                for key, bundle in items.items():
                    if not isinstance(bundle, dict):
                        continue
                    meta_name = (bundle.get("meta") or {}).get("name")
                    typed_name = None
                    if item_type == "Custom Fields":
                        typed_name = (bundle.get("field") or {}).get("name")
                        _id = (bundle.get("field") or {}).get("id") or key
                    elif item_type == "Workflows":
                        typed_name = key
                        _id = key  # workflow name-as-id
                    elif item_type == "Workflow Schemes":
                        typed_name = (bundle.get("scheme") or {}).get("name")
                        _id = (bundle.get("scheme") or {}).get("id") or key
                    elif item_type == "Screens":
                        typed_name = (bundle.get("screen") or {}).get("name")
                        _id = (bundle.get("screen") or {}).get("id") or key
                    elif item_type == "Screen Schemes":
                        typed_name = (bundle.get("screenScheme") or {}).get("name")
                        _id = (bundle.get("screenScheme") or {}).get("id") or key
                    elif item_type == "Dashboards":
                        typed_name = (bundle.get("dashboard") or {}).get("name")
                        _id = (bundle.get("dashboard") or {}).get("id") or key
                    elif item_type == "Filters":
                        typed_name = (bundle.get("filter") or {}).get("name")
                        _id = (bundle.get("filter") or {}).get("id") or key
                    elif item_type == "Workflow Statuses":
                        typed_name = (bundle.get("status") or {}).get("name")
                        _id = (bundle.get("status") or {}).get("id") or key
                    else:
                        _id = key
                    best = meta_name or typed_name
                    if not _id or not best:
                        continue
                    if _id not in name_by_id or self._is_id_like(item_type, name_by_id[_id]):
                        name_by_id[_id] = best
            except Exception:
                continue
        return name_by_id

    def _repair_names_from_cache(self, item_type: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Replace ID-ish names with nicer names found in backups."""
        cache = self._collect_backup_name_map(item_type)
        if not cache:
            return rows
        fixed: List[Dict[str, Any]] = []
        for r in rows:
            rid = str(r.get("id") or r.get("name") or "")
            rname = str(r.get("name") or r.get("id") or "")
            if self._is_id_like(item_type, rname):
                pretty = cache.get(rid)
                if pretty and not self._is_id_like(item_type, pretty):
                    r = {**r, "name": pretty}
            fixed.append(r)
        return fixed

    def _latest_full_backup_path(self, item_type: str) -> Optional[str]:
        """Locate the latest full_backup_* file (mtime-based) for a type."""
        import glob as _glob
        base_pat = f"full_backup_{item_type.replace(' ', '_').lower()}_*.json"
        patterns = [
            os.path.join(BACKUP_DIR, base_pat),
            os.path.join(BACKUP_DIR, item_type.replace(" ", "_").lower(), base_pat),
        ]
        files: List[str] = []
        for pat in patterns:
            files.extend(_glob.glob(pat))
        if not files:
            return None
        return max(files, key=os.path.getmtime)

    def _load_full_backup(self, item_type: str) -> Dict[str, Any]:
        """Load JSON from the most recent full backup for this type."""
        path = self._latest_full_backup_path(item_type)
        if not path:
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log_to_file("recover", f"Failed to read backup for {item_type}: {e}")
            return {}

    def _iter_backup_json_paths(self) -> List[str]:
        """Return all .json files under backup/backups (recursive)."""
        paths: List[str] = []
        for root, _dirs, files in os.walk(BACKUP_DIR):
            for fn in files:
                if fn.lower().endswith(".json"):
                    paths.append(os.path.join(root, fn))
        return paths

    def _extract_normalized_from_backup(self, item_type: str, data: Any) -> List[Dict[str, str]]:
        """Convert backup JSON structure into rows [{'id','name'}, ...] for UI."""
        out: List[Dict[str, str]] = []
        if not isinstance(data, dict):
            return out

        def add(id_val: Optional[str], name_val: Optional[str]) -> None:
            if not id_val and not name_val:
                return
            out.append({"id": str(id_val or name_val or "unknown"),
                        "name": str(name_val or id_val or "unknown")})

        if data.get("items") and isinstance(data["items"], dict):
            items = data["items"]
            file_type = data.get("itemType")
            if file_type and file_type != item_type:
                return out

            def meta_name(b: Dict[str, Any]) -> Optional[str]:
                return (b.get("meta") or {}).get("name")

            if item_type == "Custom Fields":
                for key, bundle in items.items():
                    fld = (bundle or {}).get("field") or {}
                    fid = fld.get("id") or key
                    fname = fld.get("name") or meta_name(bundle)
                    add(fid, fname)
                return out

            if item_type == "Workflows":
                for wf_name, bundle in items.items():
                    add(wf_name, meta_name(bundle) or wf_name)
                return out

            if item_type == "Workflow Schemes":
                for _, bundle in items.items():
                    sch = (bundle or {}).get("scheme") or {}
                    add(sch.get("id"), sch.get("name") or meta_name(bundle))
                return out

            if item_type == "Screens":
                for _, bundle in items.items():
                    scr = (bundle or {}).get("screen") or {}
                    add(scr.get("id"), scr.get("name") or meta_name(bundle))
                return out

            if item_type == "Screen Schemes":
                for _, bundle in items.items():
                    ssch = (bundle or {}).get("screenScheme") or {}
                    add(ssch.get("id"), ssch.get("name") or meta_name(bundle))
                return out

            if item_type == "Dashboards":
                for _, bundle in items.items():
                    d = (bundle or {}).get("dashboard") or {}
                    add(d.get("id"), d.get("name") or meta_name(bundle))
                return out

            if item_type == "Filters":
                for old_id, bundle in items.items():
                    filt = (bundle or {}).get("filter") or {}
                    add(old_id or filt.get("id"), filt.get("name") or meta_name(bundle))
                return out

            if item_type == "Workflow Statuses":
                for key, bundle in items.items():
                    s = (bundle or {}).get("status") or {}
                    sid = s.get("id") or key
                    sname = s.get("name") or meta_name(bundle)
                    add(sid, sname)
                return out

        return out

    def _list_from_backups(self, item_type: str) -> List[Dict[str, Any]]:
        """Collect normalized rows for Recover list from backups (deep or latest)."""
        rows: List[Dict[str, Any]] = []

        def add_many(data: Any) -> None:
            if not data:
                return
            rows.extend(self._extract_normalized_from_backup(item_type, data))

        if self.recover_deep_scan.get():
            for path in self._iter_backup_json_paths():
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        add_many(json.load(f))
                except Exception as e:
                    log_to_file("recover", f"Skip unreadable backup {path}: {e}")
        else:
            data = self._load_full_backup(item_type)
            add_many(data)

        return self._dedupe_rows_preferring_names(item_type, rows)

    def _dedupe_rows_preferring_names(self, item_type: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prefer entries with human-friendly names when deduping by ID/key."""
        def score(d: Dict[str, Any]) -> int:
            name = str(d.get("name") or "").strip()
            if not name:
                return 0
            return 2 if not self._is_id_like(item_type, name) else 1

        best: Dict[str, Dict[str, Any]] = {}
        for r in rows or []:
            rid = str(r.get("id") or r.get("name") or "")
            if not rid:
                continue
            cur = best.get(rid)
            if cur is None or score(r) > score(cur):
                best[rid] = r
        return list(best.values())

    def _merge_unique(self, items: List[Dict[str, Any]], extra: List[Dict[str, Any]], id_key: str, item_type: str) -> List[Dict[str, Any]]:
        """
        Merge two lists (API results + backups) on id_key, preferring rows
        with nicer names per _is_id_like. Used by: get_deleted_*.
        """
        def get_id(d: Dict[str, Any]) -> str:
            return str(d.get(id_key) or d.get("id") or d.get("name") or "")

        def score(d: Dict[str, Any]) -> int:
            name = str(d.get("name") or "").strip()
            if not name:
                return 0
            return 2 if not self._is_id_like(item_type, name) else 1

        best: Dict[str, Dict[str, Any]] = {}
        for src in (items or [], extra or []):
            for it in src:
                rid = get_id(it)
                if not rid:
                    continue
                cur = best.get(rid)
                if cur is None or score(it) > score(cur):
                    best[rid] = it
        return list(best.values())

    # ---------- HTTP ----------
    def jira_auth(self) -> HTTPBasicAuth:
        """Build HTTP Basic auth from current email/token."""
        return HTTPBasicAuth(self.current_email, self.current_token)

    def _get(self, url: str, item_type: str) -> Any:
        """GET JSON with retries (safe methods). Emits status, logs errors."""
        try:
            if not self.current_domain:
                raise RuntimeError("Jira domain is not set. Click Authentication → Apply.")
            self._emit_status(f"Fetching {item_type} …")
            resp = self._http.get(url, auth=self.jira_auth(), timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            self._emit_status(f"Fetch error for {item_type}: {e}")
            log_to_file("fetch", f"Failed to fetch {item_type} @ {url}: {e}")
            return {"__error__": str(e)}  # caller decides UI

    def _delete(self, url: str) -> bool:
        """DELETE helper (no auto-retry to avoid duplicate destructive calls)."""
        try:
            resp = self._http.delete(url, auth=self.jira_auth(), timeout=TIMEOUT)
            return resp.status_code in (200, 202, 204)
        except Exception as e:
            log_to_file("cleanup", f"Delete error at {url}: {e}")
            return False

    def _post(self, url: str, json_body: Any = None) -> Tuple[bool, Optional[dict]]:
        """POST helper (no global retry; handle per-call if needed)."""
        try:
            resp = self._http.post(url, json=json_body, auth=self.jira_auth(), timeout=TIMEOUT)
            return (resp.status_code in (200, 201, 202, 204),
                    resp.json() if resp.headers.get("content-type", "").startswith("application/json") else None)
        except Exception as e:
            log_to_file("recover", f"Post error at {url}: {e}")
            return False, None

    def _put(self, url: str, json_body: Any = None) -> Tuple[bool, Optional[dict]]:
        """PUT helper (no global retry; handle per-call if needed)."""
        try:
            resp = self._http.put(url, json=json_body, auth=self.jira_auth(), timeout=TIMEOUT)
            return (resp.status_code in (200, 201, 202, 204),
                    resp.json() if resp.headers.get("content-type", "").startswith("application/json") else None)
        except Exception as e:
            log_to_file("recover", f"Put error at {url}: {e}")
            return False, None

    def test_connection(self) -> bool:
        """Called by 'Test Connection' button → shows messagebox via _test_conn_clicked."""
        try:
            url = f"{self.current_domain}/rest/api/3/myself"
            self._emit_status("Testing connection …")
            resp = self._http.get(url, auth=self.jira_auth(), timeout=TIMEOUT)
            resp.raise_for_status()
            self._emit_status("Connection OK")
            return True
        except Exception as e:
            self._emit_status("Connection FAILED")
            self._show_message("error", "Connection Failed", str(e))
            return False

    # ---------- Fetchers for Cleanup (unused candidates) ----------
    # NOTE: For non-custom-field types, “unused” is mostly “not locked”.

    def get_unused_workflows(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/workflow", "workflows")
        values = [w for w in (data.get("values", []) if isinstance(data, dict) else data) if isinstance(w, dict)]
        hidden = self._count_hidden_locked_generic(values)
        self.cleanup_hidden_counts["Workflows"] = hidden

        used_names = self._workflows_in_use()
        out = []
        for w in values:
            if w.get("isLocked"):
                continue
            name = str(w.get("name") or "").strip()
            if name and name not in used_names:
                out.append(w)
        return out

    def get_unused_workflow_schemes(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/workflowscheme", "workflow schemes")
        values = [w for w in (data.get("values", []) if isinstance(data, dict) else data) if isinstance(w, dict)]
        hidden = self._count_hidden_locked_generic(values)
        self.cleanup_hidden_counts["Workflow Schemes"] = hidden

        # Consider a scheme 'used' if any project is associated with it.
        # Many responses include "projects" or "projectIds" or an "associations" count; be permissive.
        out = []
        for sch in values:
            if sch.get("isLocked"):
                continue
            projects = sch.get("projects") or sch.get("projectIds") or sch.get("projectAssociation") or []
            assoc_count = len(projects) if isinstance(projects, list) else (projects.get("count", 0) if isinstance(projects, dict) else 0)
            if assoc_count == 0:
                out.append(sch)
        return out

    def get_unused_screens(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/screens", "screens")
        values = [w for w in (data.get("values", []) if isinstance(data, dict) else data) if isinstance(w, dict)]
        hidden = self._count_hidden_locked_generic(values)
        self.cleanup_hidden_counts["Screens"] = hidden

        used_screen_ids = self._screens_in_use()
        out = []
        for s in values:
            if s.get("isLocked"):
                continue
            sid = str(s.get("id") or "").strip()
            if sid and sid not in used_screen_ids:
                out.append(s)
        return out

    def get_unused_screen_schemes(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/screenscheme", "screen schemes")
        values = [w for w in (data.get("values", []) if isinstance(data, dict) else data) if isinstance(w, dict)]
        hidden = self._count_hidden_locked_generic(values)
        self.cleanup_hidden_counts["Screen Schemes"] = hidden

        used_ss_ids = self._screen_schemes_in_use()
        out = []
        for ss in values:
            if ss.get("isLocked"):
                continue
            sid = str(ss.get("id") or "").strip()
            if sid and sid not in used_ss_ids:
                out.append(ss)
        return out

    def get_unused_dashboards(self) -> List[Dict[str, Any]]:
        # NOTE: unchanged logic (no reliable 'usage' signal via REST without org policy).
        data = self._get(f"{self.current_domain}/rest/api/3/dashboard", "dashboards")
        values = [w for w in (data.get("dashboards", []) if isinstance(data, dict) else data) if isinstance(w, dict)]
        hidden = self._count_hidden_locked_generic(values)
        self.cleanup_hidden_counts["Dashboards"] = hidden
        return [w for w in values if not w.get("isLocked")]

    def get_unused_filters(self) -> List[Dict[str, Any]]:
        # NOTE: unchanged logic; current list is favourites, not 'all' nor 'unused'.
        data = self._get(f"{self.current_domain}/rest/api/3/filter/favourite", "filters")
        values = data if isinstance(data, list) \
            else [w for w in (data.get("values", []) if isinstance(data, dict) else []) if isinstance(w, dict)]
        hidden = self._count_hidden_locked_generic(values)
        self.cleanup_hidden_counts["Filters"] = hidden
        return [w for w in values if not w.get("isLocked")]

    # ---- Workflow Statuses (finder only; Jira Cloud has no delete/restore API) ----

    def _get_all_statuses(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/status", "workflow statuses")
        return data if isinstance(data, list) else []

    def _get_status_ids_used_in_workflows(self) -> Set[str]:
        data = self._get(f"{self.current_domain}/rest/api/3/workflow", "workflows")
        workflows = data.get("values", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        used: Set[str] = set()
        for wf in workflows or []:
            statuses = wf.get("statuses") or []
            for s in statuses:
                sid = str(s.get("id") or s.get("name") or "").strip()
                if sid:
                    used.add(sid)
        return used

    def get_unused_workflow_statuses(self) -> List[Dict[str, Any]]:
        """Compute Workflow Statuses not referenced in any workflow (read-only list)."""
        all_statuses = self._get_all_statuses()
        used_ids = self._get_status_ids_used_in_workflows()
        unused: List[Dict[str, Any]] = []
        for s in all_statuses:
            sid = str(s.get("id") or s.get("name") or "")
            if sid and sid not in used_ids:
                s["isLocked"] = False
                unused.append(s)
        return unused

    # ---------- Deleters (called by _delete_unused_items per type) ----------
    def delete_custom_field(self, field_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete custom field: {field_id}")
            return True
        return self._delete(f"{self.current_domain}/rest/api/3/field/{field_id}")

    def delete_workflow(self, workflow_name: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete workflow: {workflow_name}")
            return True
        return self._delete(f"{self.current_domain}/rest/api/3/workflow/{workflow_name}")

    def delete_workflow_scheme(self, scheme_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete workflow scheme: {scheme_id}")
            return True
        return self._delete(f"{self.current_domain}/rest/api/3/workflowscheme/{scheme_id}")

    def delete_screen(self, screen_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete screen: {screen_id}")
            return True
        return self._delete(f"{self.current_domain}/rest/api/3/screens/{screen_id}")

    def delete_screen_scheme(self, scheme_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete screen scheme: {scheme_id}")
            return True
        return self._delete(f"{self.current_domain}/rest/api/3/screenscheme/{scheme_id}")

    def delete_dashboard(self, dashboard_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete dashboard: {dashboard_id}")
            return True
        return self._delete(f"{self.current_domain}/rest/api/3/dashboard/{dashboard_id}")

    def delete_filter(self, filter_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete filter: {filter_id}")
            return True
        return self._delete(f"{self.current_domain}/rest/api/3/filter/{filter_id}")

    def delete_status(self, status_id_or_name: str) -> bool:
        """Not supported by Jira Cloud; show info instead."""
        self._show_message(
            "info",
            "Not Supported by Jira Cloud API",
            "Deleting workflow statuses is not available via REST in Jira Cloud.\n"
            "Use Jira Admin → Issues → Statuses to delete manually."
        )
        log_to_file("cleanup", f"Status delete requested but not supported: {status_id_or_name}")
        return False

    # ---------- Deleted fetchers (for Recover tab lists) ----------
    # Merge API “deleted” lists + backup-derived rows (if enabled).

    def get_deleted_fields(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/field/search?status=deleted", "deleted fields")
        api_items = data.get("values", []) if isinstance(data, dict) else []
        if not self.recover_include_backups.get():
            self._emit_status(f"Recover: Custom Fields from API only: {len(api_items)}")
            return self._repair_names_from_cache("Custom Fields", api_items)
        backup_norm = self._list_from_backups("Custom Fields")
        merged = self._merge_unique(api_items, backup_norm, "id", "Custom Fields")
        repaired = self._repair_names_from_cache("Custom Fields", merged)
        self._emit_status(f"Recover: Custom Fields API {len(api_items)} + Backups {len(backup_norm)} -> {len(merged)}")
        return repaired

    def get_deleted_workflows(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/workflow?deleted=true", "deleted workflows")
        api_items = data["values"] if isinstance(data, dict) and "values" in data else []
        if not self.recover_include_backups.get():
            return api_items
        backup_norm = self._list_from_backups("Workflows")
        backup_norm = [{"name": it.get("name", "")} for it in backup_norm]
        return self._merge_unique(api_items, backup_norm, "name", "Workflows")

    def get_deleted_workflow_schemes(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/workflowscheme?deleted=true", "deleted workflow schemes")
        api_items = data["values"] if isinstance(data, dict) and "values" in data else []
        if not self.recover_include_backups.get():
            return api_items
        backup_norm = self._list_from_backups("Workflow Schemes")
        return self._merge_unique(api_items, backup_norm, "id", "Workflow Schemes")

    def get_deleted_screens(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/screens?deleted=true", "deleted screens")
        api_items = data["values"] if isinstance(data, dict) and "values" in data else []
        if not self.recover_include_backups.get():
            return api_items
        backup_norm = self._list_from_backups("Screens")
        return self._merge_unique(api_items, backup_norm, "id", "Screens")

    def get_deleted_screen_schemes(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/screenscheme?deleted=true", "deleted screen schemes")
        api_items = data["values"] if isinstance(data, dict) and "values" in data else []
        if not self.recover_include_backups.get():
            return api_items
        backup_norm = self._list_from_backups("Screen Schemes")
        return self._merge_unique(api_items, backup_norm, "id", "Screen Schemes")

    def get_deleted_dashboards(self) -> List[Dict[str, Any]]:
        data = self._get(f"{self.current_domain}/rest/api/3/dashboard?deleted=true", "deleted dashboards")
        api_items = data["dashboards"] if isinstance(data, dict) and "dashboards" in data else []
        if not self.recover_include_backups.get():
            return api_items
        backup_norm = self._list_from_backups("Dashboards")
        return self._merge_unique(api_items, backup_norm, "id", "Dashboards")

    def get_deleted_filters(self) -> List[Dict[str, Any]]:
        api_part = []
        try:
            data = self._get(f"{self.current_domain}/rest/api/3/filter/search?deleted=true", "deleted filters")
            if isinstance(data, dict) and "values" in data:
                api_part = data["values"]
        except Exception:
            pass
        if not self.recover_include_backups.get():
            return api_part
        backup_part = self._list_from_backups("Filters")
        return self._merge_unique(api_part, backup_part, "id", "Filters")

    def get_deleted_statuses(self) -> List[Dict[str, Any]]:
        """No API for deleted statuses; list only from backups."""
        return self._list_from_backups("Workflow Statuses")

    def _count_hidden_locked_generic(self, items: List[Dict[str, Any]]) -> int:
        """Helper: count locked items (we hide them from Unused UI)."""
        return sum(1 for it in items or [] if isinstance(it, dict) and it.get("isLocked"))




    def _build_cleanup_tab(self) -> None:
        """
        Build the Cleanup tab:
        - Wires "Find Unused" button → self._thread(self._load_unused_items, type)
        - Wires "Delete Checked" → self._thread(self._delete_unused_items, type)
        - Renders per-type trees and sort/search.
        """
        notebook = ttk.Notebook(self.cleanup_tab)
        notebook.pack(fill="both", expand=True)

        # Map type → finder function (called by _load_unused_items)
        self.cleanup_types = {
            "Custom Fields": self.get_unused_custom_fields(),
            "Workflows": self.get_unused_workflows,
            "Workflow Schemes": self.get_unused_workflow_schemes,
            "Workflow Statuses": self.get_unused_workflow_statuses,  # finder-only
            "Screens": self.get_unused_screens,
            "Screen Schemes": self.get_unused_screen_schemes,
            "Dashboards": self.get_unused_dashboards,
            "Filters": self.get_unused_filters,
        }
        # Type → deleter (called by _delete_unused_items)
        self.cleanup_delete_funcs = {
            "Custom Fields": self.delete_custom_field,
            "Workflows": self.delete_workflow,
            "Workflow Schemes": self.delete_workflow_scheme,
            "Workflow Statuses": self.delete_status,  # not supported; shows message
            "Screens": self.delete_screen,
            "Screen Schemes": self.delete_screen_scheme,
            "Dashboards": self.delete_dashboard,
            "Filters": self.delete_filter,
        }

        for name in self.cleanup_types.keys():
            frame = ttk.Frame(notebook)
            notebook.add(frame, text=name)

            top = ttk.Frame(frame)
            top.pack(fill="x", pady=4)
            ttk.Label(top, text=f"Unused {name}").pack(side="left", padx=4)

            sv = tk.StringVar()
            self.cleanup_search_vars[name] = sv
            search_box = ttk.Entry(top, textvariable=sv, width=28)
            search_box.pack(side="left", padx=6)
            ttk.Button(top, text="Clear", command=lambda v=sv, n=name: (v.set(""), self._render_items("cleanup", n))).pack(side="left", padx=2)

            ttk.Checkbutton(top, text="Include backups", state="enabled").pack(side="left", padx=10)

            self.cleanup_sort_orders[name] = tk.BooleanVar(value=True)
            ttk.Radiobutton(top, text="Ascending", variable=self.cleanup_sort_orders[name], value=True,
                            command=lambda n=name: self._render_items("cleanup", n)).pack(side="right", padx=2)
            ttk.Radiobutton(top, text="Descending", variable=self.cleanup_sort_orders[name], value=False,
                            command=lambda n=name: self._render_items("cleanup", n)).pack(side="right", padx=2)
            ttk.Label(top, text="Sort:").pack(side="right")

            tree = self._build_checkbox_tree(frame)
            tree.pack(fill="both", expand=True, padx=4, pady=4)
            self.cleanup_trees[name] = tree
            self.cleanup_checked[name] = set()

            actions = ttk.Frame(frame)
            actions.pack(fill="x", pady=4)

            # BUTTON → background task → UI update via after()
            btn_find = ttk.Button(actions, text="Find Unused", command=lambda n=name: self._thread(self._load_unused_items, n))
            btn_find.pack(side="left", padx=2)

            hidden_lbl = ttk.Label(actions, text="Hidden (locked): 0")
            hidden_lbl.pack(side="left", padx=10)
            self.cleanup_hidden_locked_labels[name] = hidden_lbl

            ttk.Button(actions, text="Check All", command=lambda n=name: self._check_all("cleanup", n)).pack(side="left", padx=2)
            ttk.Button(actions, text="Uncheck All", command=lambda n=name: self._uncheck_all("cleanup", n)).pack(side="left", padx=2)
            ttk.Button(actions, text="Invert", command=lambda n=name: self._invert_all("cleanup", n)).pack(side="left", padx=2)

            ttk.Button(actions, text="Export Checked to Excel",
                       command=lambda n=name: self._export_tree("cleanup", n, only_checked=True)).pack(side="right", padx=2)
            ttk.Button(actions, text="Export All to Excel",
                       command=lambda n=name: self._export_tree("cleanup", n, only_checked=False)).pack(side="right", padx=2)

            del_btn = ttk.Button(actions, text="Delete Checked", command=lambda n=name: self._thread(self._delete_unused_items, n))
            del_btn.pack(side="right", padx=12)

            if name == "Workflow Statuses":
                del_btn.state(["disabled"])
                del_btn.configure(text="Delete (Not supported in Jira Cloud)")
                ttk.Label(actions, text="Note: Status deletion via REST is not available in Jira Cloud.").pack(side="right", padx=8)

            del_flag = tk.BooleanVar(value=True)
            ttk.Checkbutton(actions, text="Enable Deletion", variable=del_flag).pack(side="right", padx=8)
            self.cleanup_delete_flags[name] = del_flag

            search_box.bind("<KeyRelease>", lambda _e, n=name: self._render_items("cleanup", n))

    def _build_recover_tab(self) -> None:
        notebook = ttk.Notebook(self.recover_tab)
        notebook.pack(fill="both", expand=True)

        self.recover_types = {
            "Custom Fields": {"fetch": self.get_deleted_fields, "recover": self.recover_deleted_fields, "id_key": "id", "name_key": "name"},
            "Workflows": {"fetch": self.get_deleted_workflows, "recover": self.recover_deleted_workflows, "id_key": "name", "name_key": "name"},
            "Workflow Schemes": {"fetch": self.get_deleted_workflow_schemes, "recover": self.recover_deleted_workflow_schemes, "id_key": "id", "name_key": "name"},
            "Workflow Statuses": {"fetch": self.get_deleted_statuses, "recover": self.recover_deleted_statuses, "id_key": "id", "name_key": "name"},
            "Screens": {"fetch": self.get_deleted_screens, "recover": self.recover_deleted_screens, "id_key": "id", "name_key": "name"},
            "Screen Schemes": {"fetch": self.get_deleted_screen_schemes, "recover": self.recover_deleted_screen_schemes, "id_key": "id", "name_key": "name"},
            "Dashboards": {"fetch": self.get_deleted_dashboards, "recover": self.recover_deleted_dashboards, "id_key": "id", "name_key": "name"},
            "Filters": {"fetch": self.get_deleted_filters, "recover": self.recover_deleted_filters, "id_key": "id", "name_key": "name"},
        }

        for name in self.recover_types.keys():
            frame = ttk.Frame(notebook)
            notebook.add(frame, text=name)

            top = ttk.Frame(frame)
            top.pack(fill="x", pady=4)
            ttk.Label(top, text=f"Select deleted {name.lower()} to recover").pack(side="left", padx=4)

            sv = tk.StringVar()
            self.recover_search_vars[name] = sv
            search_box = ttk.Entry(top, textvariable=sv, width=28)
            search_box.pack(side="left", padx=6)
            ttk.Button(top, text="Clear",
                       command=lambda v=sv, n=name: (v.set(""), self._render_items("recover", n))).pack(side="left", padx=2)

            ttk.Checkbutton(
                top, text="Include backups", variable=self.recover_include_backups,
                command=lambda n=name: self._thread(self._load_deleted_items, n)
            ).pack(side="left", padx=10)

            ttk.Checkbutton(
                top, text="Deep scan subfolders", variable=self.recover_deep_scan,
                command=lambda n=name: self._thread(self._load_deleted_items, n)
            ).pack(side="left", padx=6)

            self.recover_sort_orders[name] = tk.BooleanVar(value=True)
            ttk.Radiobutton(top, text="Ascending", variable=self.recover_sort_orders[name], value=True,
                            command=lambda n=name: self._render_items("recover", n)).pack(side="right", padx=2)
            ttk.Radiobutton(top, text="Descending", variable=self.recover_sort_orders[name], value=False,
                            command=lambda n=name: self._render_items("recover", n)).pack(side="right", padx=2)
            ttk.Label(top, text="Sort:").pack(side="right")

            tree = self._build_checkbox_tree(frame)
            tree.pack(fill="both", expand=True, padx=4, pady=4)
            self.recover_trees[name] = tree
            self.recover_checked[name] = set()

            actions = ttk.Frame(frame)
            actions.pack(fill="x", pady=4)
            ttk.Button(actions, text="Refresh List",
                       command=lambda n=name: self._thread(self._load_deleted_items, n)).pack(side="left", padx=2)
            ttk.Button(actions, text="Check All", command=lambda n=name: self._check_all("recover", n)).pack(
                side="left", padx=2)
            ttk.Button(actions, text="Uncheck All", command=lambda n=name: self._uncheck_all("recover", n)).pack(
                side="left", padx=2)
            ttk.Button(actions, text="Invert", command=lambda n=name: self._invert_all("recover", n)).pack(side="left", padx=2)

            ttk.Button(actions, text="Export Checked to Excel",
                       command=lambda n=name: self._export_tree("recover", n, only_checked=True)).pack(side="right", padx=2)
            ttk.Button(actions, text="Export All to Excel",
                       command=lambda n=name: self._export_tree("recover", n, only_checked=False)).pack(side="right", padx=2)

            flag = tk.BooleanVar(value=True)
            ttk.Checkbutton(actions, text="Enable Recovery", variable=flag).pack(side="right", padx=8)
            setattr(self, f"{name.lower().replace(' ', '_')}_recover_enabled", flag)

            rec_btn = ttk.Button(actions, text="Recover Checked",
                                 command=lambda n=name: self._thread(self._recover_selected, n))
            rec_btn.pack(side="right", padx=12)

            if name == "Workflow Statuses":
                rec_btn.state(["disabled"])
                rec_btn.configure(text="Recover (Not supported in Jira Cloud)")
                ttk.Label(actions, text="Note: Status recovery via REST is not available in Jira Cloud.").pack(
                    side="right", padx=8)

            search_box.bind("<KeyRelease>", lambda _e, n=name: self._render_items("recover", n))

    def _build_logs_tab(self) -> None:
        ttk.Label(self.logs_tab, text="Logs and backups").pack(pady=10)
        ttk.Button(self.logs_tab, text="Open Logs Folder", command=lambda: open_folder(LOG_DIR)).pack(pady=5)
        ttk.Button(self.logs_tab, text="Open Backups Folder", command=lambda: open_folder(BACKUP_DIR)).pack(pady=5)
        ttk.Button(self.logs_tab, text="Open Exports Folder", command=lambda: open_folder(EXPORT_DIR)).pack(pady=5)

    def _build_auth_tab(self) -> None:
        profiles = load_auth_profiles()
        top = ttk.Frame(self.auth_tab)
        top.pack(fill="x", pady=6)
        ttk.Label(top, text="Profile").pack(side="left", padx=4)
        self.profile_var = tk.StringVar()
        self.profile_dropdown = ttk.Combobox(top, textvariable=self.profile_var, values=list(profiles.keys()), width=30)
        self.profile_dropdown.pack(side="left", padx=4)
        ttk.Button(top, text="Load", command=self._load_selected_profile).pack(side="left", padx=4)

        form = ttk.Frame(self.auth_tab)
        form.pack(fill="x", pady=6)
        ttk.Label(form, text="Jira Domain (https://…atlassian.net)").grid(row=0, column=0, sticky="w")
        ttk.Label(form, text="Email").grid(row=1, column=0, sticky="w")
        ttk.Label(form, text="API Token").grid(row=2, column=0, sticky="w")

        self.domain_var = tk.StringVar()
        self.email_var = tk.StringVar()
        self.token_var = tk.StringVar()

        ttk.Entry(form, textvariable=self.domain_var, width=60).grid(row=0, column=1, padx=6, pady=3, sticky="we")
        ttk.Entry(form, textvariable=self.email_var, width=60).grid(row=1, column=1, padx=6, pady=3, sticky="we")
        ttk.Entry(form, textvariable=self.token_var, width=60, show="*").grid(row=2, column=1, padx=6, pady=3, sticky="we")

        actions = ttk.Frame(self.auth_tab)
        actions.pack(fill="x", pady=8)
        ttk.Checkbutton(actions, text="Dry Run Mode", variable=self.dry_run).pack(side="left", padx=6)

        ttk.Button(actions, text="Save Profile", command=self._save_profile).pack(side="left", padx=6)
        ttk.Button(actions, text="Apply", command=self._apply_profile_fields).pack(side="left", padx=6)
        ttk.Button(actions, text="Test Connection", command=self._test_conn_clicked).pack(side="left", padx=6)

        if profiles:
            first = next(iter(profiles))
            self.profile_var.set(first)
            self._load_selected_profile()
            self._apply_profile_fields()

    # ---------- Tree helpers & rendering ----------
    def _tree_sets(self, mode: str, item_type: str) -> Tuple[ttk.Treeview, set[str]]:
        if mode == "cleanup":
            return self.cleanup_trees[item_type], self.cleanup_checked[item_type]
        return self.recover_trees[item_type], self.recover_checked[item_type]

    def _toggle_tree_item(self, tree: ttk.Treeview, iid: str) -> None:
        is_cleanup = tree in self.cleanup_trees.values()
        item_type = self._find_item_type_by_tree(tree, "cleanup" if is_cleanup else "recover")
        checked_set = (self.cleanup_checked if is_cleanup else self.recover_checked)[item_type]
        if iid in checked_set:
            checked_set.remove(iid)
            tree.set(iid, "check", "☐")
        else:
            checked_set.add(iid)
            tree.set(iid, "check", "☑")

    def _find_item_type_by_tree(self, tree: ttk.Treeview, mode: str) -> str:
        mapping = self.cleanup_trees if mode == "cleanup" else self.recover_trees
        for k, v in mapping.items():
            if v is tree:
                return k
        raise KeyError("Tree not registered")

    def _render_items(self, mode: str, item_type: str) -> None:
        tree, checked = self._tree_sets(mode, item_type)
        if mode == "cleanup":
            raw = self.cleanup_last_items.get(item_type, [])
            q = (self.cleanup_search_vars.get(item_type, tk.StringVar()).get() or "").strip().lower()
            ascending = self.cleanup_sort_orders[item_type].get()
            id_key = "id"
            name_key = "name"
        else:
            raw = self.recover_last_items.get(item_type, [])
            q = (self.recover_search_vars.get(item_type, tk.StringVar()).get() or "").strip().lower()
            ascending = self.recover_sort_orders[item_type].get()
            spec = self.recover_types[item_type]
            id_key = spec["id_key"]
            name_key = spec["name_key"]

        def match(it: Dict[str, Any]) -> bool:
            sid = str(it.get(id_key) or it.get("id") or "")
            sname = str(it.get(name_key) or it.get("name") or "")
            if not q:
                return True
            return (q in sid.lower()) or (q in sname.lower())

        filtered = [it for it in raw if match(it)]
        filtered.sort(key=lambda x: str(x.get(name_key) or x.get("name") or "").lower(), reverse=not ascending)

        tree.delete(*tree.get_children(""))
        for it in filtered:
            _id = str(it.get(id_key) or it.get("id") or "unknown")
            name = it.get(name_key) or it.get("name") or _id
            tree.insert("", "end", iid=_id, values=("☑" if _id in checked else "☐", _id, name))

        self._emit_status(f"Listed {len(filtered)} / {len(raw)} {item_type} (filter: '{q or '∅'}')")

    def _resort_tree(self, mode: str, item_type: str) -> None:
        self._render_items(mode, item_type)

    def _check_all(self, mode: str, item_type: str) -> None:
        tree, checked = self._tree_sets(mode, item_type)
        for iid in tree.get_children(""):
            checked.add(iid)
            tree.set(iid, "check", "☑")

    def _uncheck_all(self, mode: str, item_type: str) -> None:
        tree, checked = self._tree_sets(mode, item_type)
        checked.clear()
        for iid in tree.get_children(""):
            tree.set(iid, "check", "☐")

    def _invert_all(self, mode: str, item_type: str) -> None:
        tree, checked = self._tree_sets(mode, item_type)
        for iid in tree.get_children(""):
            if iid in checked:
                checked.remove(iid)
                tree.set(iid, "check", "☐")
            else:
                checked.add(iid)
                tree.set(iid, "check", "☑")

    # ---------- Export helpers ----------
    def _gather_tree_rows(self, mode: str, item_type: str, only_checked: bool) -> List[Dict[str, str]]:
        tree, checked = self._tree_sets(mode, item_type)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows: List[Dict[str, str]] = []
        for iid in tree.get_children(""):
            if only_checked and iid not in checked:
                continue
            rows.append({
                "Type": item_type,
                "ID": tree.set(iid, "id"),
                "Name": tree.set(iid, "name"),
                "ExportedAt": now,
            })
        return rows

    def _export_rows(self, rows: List[Dict[str, str]], filename_base: str) -> str:
        if not rows:
            raise ValueError("No rows to export")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        xlsx_path = os.path.join(EXPORT_DIR, f"{filename_base}_{ts}.xlsx")

        if Workbook is not None:
            try:
                wb = Workbook()
                ws = wb.active
                ws.title = "Items"
                headers = list(rows[0].keys())
                ws.append(headers)
                for r in rows:
                    ws.append([r.get(h, "") for h in headers])
                wb.save(xlsx_path)
                log_to_file("export", f"Exported {len(rows)} rows -> {xlsx_path}")
                return xlsx_path
            except Exception as e:
                log_to_file("export", f"openpyxl failed, falling back to CSV: {e}")

        import csv
        csv_path = os.path.join(EXPORT_DIR, f"{filename_base}_{ts}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        log_to_file("export", f"Exported {len(rows)} rows -> {csv_path}")
        return csv_path

    def _export_tree(self, mode: str, item_type: str, only_checked: bool) -> None:
        try:
            self._emit_status(f"Exporting {item_type} ({'checked' if only_checked else 'all'}) …")
            rows = self._gather_tree_rows(mode, item_type, only_checked)
            if not rows:
                self._show_message("warning", "Nothing to export", "No items match the selection.")
                self._emit_status("Export skipped: nothing to export")
                return
            base = f"{mode}_{item_type.replace(' ', '_').lower()}" + ("_checked" if only_checked else "_all")
            out_path = self._export_rows(rows, base)
            self._emit_status(f"Export complete: {os.path.basename(out_path)}")
            self._show_message("info", "Export complete", f"Saved: {out_path}")
        except Exception as e:
            self._emit_status("Export failed")
            self._show_message("error", "Export failed", str(e))

    # ---------- Cleanup flows ----------
    def _normalize_unused_item(self, item_type: str, item: Dict[str, Any]) -> Dict[str, str]:
        if item_type == "Custom Fields":
            _id = str(item.get("id") or item.get("key") or item.get("name") or "unknown")
            name = item.get("name") or item.get("description") or _id
            return {"id": _id, "name": name}
        if item_type == "Workflows":
            name = item.get("name") or item.get("id") or "unknown"
            return {"id": str(name), "name": str(name)}
        if item_type in ("Workflow Schemes", "Screens", "Screen Schemes", "Dashboards", "Filters"):
            _id = str(item.get("id") or item.get("key") or item.get("name") or "unknown")
            name = item.get("name") or item.get("description") or _id
            return {"id": _id, "name": name}
        if item_type == "Workflow Statuses":
            _id = str(item.get("id") or item.get("name") or "unknown")
            name = item.get("name") or _id
            return {"id": _id, "name": name}
        _id = str(item.get("id") or item.get("name") or item.get("key") or "unknown")
        name = item.get("name") or item.get("description") or _id
        return {"id": _id, "name": name}

    def _repair_unused_names_from_cache(self, item_type: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cache = self._collect_backup_name_map(item_type)
        if not cache:
            return rows
        fixed = []
        for r in rows:
            rid = r["id"]
            name = r.get("name") or ""
            if self._is_id_like(item_type, name):
                pretty = cache.get(rid)
                if pretty and not self._is_id_like(item_type, pretty):
                    r = {**r, "name": pretty}
            fixed.append(r)
        return fixed

    def _load_unused_items(self, item_type: str) -> None:
        self._emit_status(f"Finding unused {item_type} …")
        fetcher = self.cleanup_types[item_type]
        items = fetcher() or []
        norm = [self._normalize_unused_item(item_type, it) for it in items]
        norm = self._repair_unused_names_from_cache(item_type, norm)

        hidden_cnt = self.cleanup_hidden_counts.get(item_type, 0)

        def _update_ui():
            self.cleanup_last_items[item_type] = norm
            if item_type in self.cleanup_hidden_locked_labels:
                self.cleanup_hidden_locked_labels[item_type]["text"] = f"Hidden (locked): {hidden_cnt}"
            self._render_items("cleanup", item_type)

        self.after(0, lambda *args: _update_ui())

    def _delete_unused_items(self, item_type: str) -> None:
        checked_ids = sorted(self.cleanup_checked.get(item_type, set()))
        if not checked_ids:
            self._show_message("warning", "No Selection", f"No {item_type} checked for deletion.")
            self._emit_status("Delete skipped: nothing checked")
            return

        if item_type == "Workflow Statuses":
            self._show_message(
                "info",
                "Not Supported by Jira Cloud API",
                "Deleting workflow statuses is not available via REST in Jira Cloud.\n"
                "Use Jira Admin → Issues → Statuses to delete manually."
            )
            return

        self._emit_status(f"Backing up {len(checked_ids)} {item_type} …")
        backup_mgr = BackupManager(self.current_domain, self.jira_auth())
        backup_file = backup_mgr.backup_items(item_type, checked_ids)
        log_to_file("cleanup", f"Pre-delete backup saved: {backup_file}")
        self._emit_status(f"Backup complete -> {os.path.basename(backup_file)}")

        if not self.cleanup_delete_flags.get(item_type, tk.BooleanVar(value=False)).get():
            log_to_file("cleanup", f"[DRY RUN] Checked {item_type} NOT deleted due to 'Enable Deletion' unchecked.")
            self._show_message("info", "Deletion Skipped", "Deletion skipped because 'Enable Deletion' is unchecked.\nBackup has been created.")
            self._emit_status("Deletion skipped (checkbox off)")
            return

        deleter = self.cleanup_delete_funcs[item_type]
        ok = 0
        for _id in checked_ids:
            if self.dry_run.get():
                log_to_file("cleanup", f"[DRY RUN] Would delete {item_type[:-1]}: {_id}")
                ok += 1
                continue
            if deleter(_id):
                ok += 1

        self._emit_status(f"Deletion finished: {ok}/{len(checked_ids)} {item_type}")
        self._show_message("info", "Cleanup Complete", f"Deleted {ok}/{len(checked_ids)} {item_type}")

    # ---------- Recover flows ----------
    def _load_deleted_items(self, item_type: str) -> None:
        self._emit_status(f"Loading deleted {item_type} …")
        spec = self.recover_types[item_type]
        items = spec["fetch"]() or []

        def _update_ui():
            self.recover_last_items[item_type] = items
            self._render_items("recover", item_type)

        self.after(0, lambda *args: _update_ui())

    # ---------- Recover actions ----------
    def _recover_selected(self, item_type: str) -> None:
        checked_ids = sorted(self.recover_checked[item_type])
        if not checked_ids:
            self._show_message("warning", "No Selection", f"No {item_type} checked for recovery.")
            self._emit_status("Recovery skipped: nothing checked")
            return
        flag = getattr(self, f"{item_type.lower().replace(' ', '_')}_recover_enabled").get()
        if not flag:
            log_to_file("recover", f"[DRY RUN] Recovery skipped for: {', '.join(checked_ids)}")
            self._show_message("info", "Dry Run", "Recovery skipped due to 'Enable Recovery' being unchecked.")
            self._emit_status("Recovery skipped (checkbox off)")
            return
        self._emit_status(f"Recovering {len(checked_ids)} {item_type} …")
        recovered = self.recover_types[item_type]["recover"](checked_ids)
        if recovered:
            self._emit_status(f"Recovered {len(recovered)} {item_type}")
            self._show_message("info", "Recovery Completed", f"Recovered: {', '.join(recovered)}")
        else:
            self._emit_status("No items were recovered")
            self._show_message("info", "Recovery Completed", "No items were recovered.")

    # ---------- Recovery impls ----------
    def recover_deleted_fields(self, field_ids: List[str]) -> List[str]:
        recovered: List[str] = []
        for fid in field_ids:
            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recover custom field {fid}")
                recovered.append(fid)
                continue
            url = f"{self.current_domain}/rest/api/3/field/{fid}"
            ok, _ = self._post(url)
            if ok:
                recovered.append(fid)
                log_to_file("recover", f"Recovered custom field {fid}")
        return recovered

    def recover_deleted_workflows(self, names: List[str]) -> List[str]:
        recovered: List[str] = []
        for name in names:
            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recover workflow {name}")
                recovered.append(name)
                continue
            url = f"{self.current_domain}/rest/api/3/workflow/{name}/restore"
            ok, _ = self._post(url)
            if ok:
                recovered.append(name)
                log_to_file("recover", f"Recovered workflow {name}")
        return recovered

    def recover_deleted_workflow_schemes(self, scheme_ids: List[str]) -> List[str]:
        recovered: List[str] = []
        for sid in scheme_ids:
            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recover workflow scheme {sid}")
                recovered.append(sid)
                continue
            url = f"{self.current_domain}/rest/api/3/workflowscheme/{sid}/restore"
            ok, _ = self._post(url)
            if ok:
                recovered.append(sid)
                log_to_file("recover", f"Recovered workflow scheme {sid}")
        return recovered

    def recover_deleted_screens(self, screen_ids: List[str]) -> List[str]:
        recovered: List[str] = []
        for sid in screen_ids:
            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recover screen {sid}")
                recovered.append(sid)
                continue
            url = f"{self.current_domain}/rest/api/3/screens/{sid}/restore"
            ok, _ = self._post(url)
            if ok:
                recovered.append(sid)
                log_to_file("recover", f"Recovered screen {sid}")
        return recovered

    def recover_deleted_screen_schemes(self, scheme_ids: List[str]) -> List[str]:
        recovered: List[str] = []
        for sid in scheme_ids:
            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recover screen scheme {sid}")
                recovered.append(sid)
                continue
            url = f"{self.current_domain}/rest/api/3/screenscheme/{sid}/restore"
            ok, _ = self._post(url)
            if ok:
                recovered.append(sid)
                log_to_file("recover", f"Recovered screen scheme {sid}")
        return recovered

    def recover_deleted_dashboards(self, dashboard_ids: List[str]) -> List[str]:
        recovered: List[str] = []
        for did in dashboard_ids:
            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recover dashboard {did}")
                recovered.append(did)
                continue
            url = f"{self.current_domain}/rest/api/3/dashboard/{did}/restore"
            ok, _ = self._post(url)
            if ok:
                recovered.append(did)
                log_to_file("recover", f"Recovered dashboard {did}")
        return recovered

    def recover_deleted_filters(self, filter_ids: List[str]) -> List[str]:
        recreated: List[str] = []
        if not filter_ids:
            return recreated
        collected: Dict[str, Dict[str, Any]] = {}
        for path in self._iter_backup_json_paths():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not (isinstance(data, dict) and isinstance(data.get("items"), dict)):
                    continue
                # Restrict to Filters only (if typed), else accept untyped bundles
                if data.get("itemType") and data.get("itemType") != "Filters":
                    continue
                for k, v in data["items"].items():
                    collected[k] = v
            except Exception:
                continue
        for old_id in filter_ids:
            bundle = collected.get(old_id) or {}
            filt = bundle.get("filter") or {}
            shares = bundle.get("sharePermissions") or []
            name = filt.get("name") or (bundle.get("meta") or {}).get("name")
            jql = filt.get("jql")
            description = filt.get("description") or ""
            favourite = bool(filt.get("favourite") or filt.get("isFavourite"))
            if not name or not jql:
                log_to_file("recover", f"Missing name/JQL for backup filter {old_id}; skipping.")
                continue
            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recreate filter '{name}' with saved shares.")
                recreated.append(name)
                continue
            # Create filter
            create_url = f"{self.current_domain}/rest/api/3/filter"
            create_body = {"name": name, "jql": jql, "description": description, "favourite": False}
            ok, new_filter = self._post(create_url, create_body)
            if not ok:
                log_to_file("recover", f"Create filter '{name}' failed.")
                continue
            new_id = (new_filter or {}).get("id")
            # Shares
            for perm in shares if isinstance(shares, list) else []:
                perm_url = f"{self.current_domain}/rest/api/2/filter/{new_id}/permission"
                ok2, _ = self._post(perm_url, perm)
                if not ok2:
                    log_to_file("recover", f"Share add failed for '{name}'")
            # Favourite
            if favourite:
                fav_url = f"{self.current_domain}/rest/api/3/filter/{new_id}/favourite"
                ok3, _ = self._put(fav_url)
                if not ok3:
                    log_to_file("recover", f"Favourite set failed for '{name}'")
            recreated.append(name)
            log_to_file("recover", f"Recreated filter '{name}' (old {old_id} -> new {new_id})")
        return recreated

    def recover_deleted_statuses(self, status_ids: List[str]) -> List[str]:
        for s in status_ids:
            log_to_file("recover", f"Status recovery requested but not supported: {s}")
        self._show_message(
            "info",
            "Not Supported by Jira Cloud API",
            "Recovering workflow statuses is not available via REST in Jira Cloud.\n"
            "Use Jira Admin → Issues → Statuses to recreate."
        )
        return []

    # ---------- Auth helpers ----------
    def _load_selected_profile(self) -> None:
        profiles = load_auth_profiles()
        prof = profiles.get(self.profile_var.get(), {})
        self.domain_var.set(prof.get("domain", ""))
        self.email_var.set(prof.get("email", ""))
        self.token_var.set(prof.get("token", ""))

    def _save_profile(self) -> None:
        name = self.profile_var.get().strip()
        if not name:
            self._show_message("error", "Error", "Profile name required.")
            return
        profiles = load_auth_profiles()
        profiles[name] = {
            "domain": self.domain_var.get().strip(),
            "email": self.email_var.get().strip(),
            "token": self.token_var.get().strip(),
        }
        save_auth_profiles(profiles)
        self.profile_dropdown["values"] = list(profiles.keys())
        self._show_message("info", "Saved", f"Profile '{name}' saved.")

    def _apply_profile_fields(self) -> None:
        raw = (self.domain_var.get() or "").strip()
        # Why: requests needs a scheme; many users paste "yourorg.atlassian.net".
        if raw and not raw.startswith(("http://", "https://")):
            raw = "https://" + raw
        self.current_domain = raw.rstrip("/")
        self.current_email = (self.email_var.get() or "").strip()
        self.current_token = (self.token_var.get() or "").strip()
        self._emit_status(f"Profile applied → {self.current_domain}")

    def _test_conn_clicked(self) -> None:
        self._apply_profile_fields()
        if self.test_connection():
            self._show_message("info", "Success", "Connection successful!")

if __name__ == "__main__":
    app = JiraToolGUI()
    app.mainloop()

