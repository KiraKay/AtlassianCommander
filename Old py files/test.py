


# path: main_updated.py
# Jira Cleanup & Recovery Tool – status bar (thread-safe), checkboxes, Excel export
import re  # add this
import json
import os
import platform
import subprocess
import threading
from datetime import datetime
from queue import Queue, Empty
from typing import Any, Dict, List, Callable, Optional, Tuple, Set

import requests
import tkinter as tk
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from tkinter import ttk, messagebox
from urllib3.util.retry import Retry  # correct import

# optional Excel support
try:
    from openpyxl import Workbook  # preferred
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
    if platform.system() == "Windows":
        os.startfile(path)  # nosec
    elif platform.system() == "Darwin":
        subprocess.call(["open", path])  # nosec
    else:
        subprocess.call(["xdg-open", path])  # nosec


def load_auth_profiles() -> Dict[str, Dict[str, str]]:
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
                    name = entry.get("name") or f"profile_{i + 1}"
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
    with open(PROFILES_FILE, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2)


def log_to_file(job_name: str, message: str) -> None:
    log_file = os.path.join(LOG_DIR, f"{job_name}_{datetime.now().strftime('%Y%m%d')}.log")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")


def backup_to_file(data: Any, name: str, item_type: str) -> str:
    type_dir = os.path.join(BACKUP_DIR, item_type.replace(" ", "_").lower())
    os.makedirs(type_dir, exist_ok=True)
    filename = os.path.join(type_dir, f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return filename


def load_backup_items(name_prefix: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    try:
        for root, _dirs, files in os.walk(BACKUP_DIR):
            for filename in files:
                if filename.startswith(name_prefix) and filename.endswith(".json"):
                    with open(os.path.join(root, filename), "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            items.extend(data)
                        elif isinstance(data, dict):
                            items.extend(data.get("values", []) or data.get("dashboards", []))
    except Exception as e:
        log_to_file("recover", f"Failed to load backup items for {name_prefix}: {e}")
    return items

# ---------------- Backup Manager ----------------
class BackupManager:
    """Collects full objects & associations prior to deletion."""

    def __init__(self, base_url: str, auth: HTTPBasicAuth) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth = auth

    def _get_json(self, path: str) -> Optional[Any]:
        try:
            url = path if path.startswith("http") else f"{self.base_url}{path}"
            resp = requests.get(url, auth=self.auth, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log_to_file("backup", f"GET {path} failed: {e}")
            return None

    def _collect_custom_field(self, field_id: str) -> Dict[str, Any]:
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
        workflows_all = self._get_json("/rest/api/3/workflow") or {}
        schemes = self._get_json("/rest/api/3/workflowscheme") or {}
        schemes_list = schemes.get("values", []) if isinstance(schemes, dict) else []
        target = (workflow_name or "").lower()
        schemes_referencing = [s for s in schemes_list if target in json.dumps(s).lower()]
        return {"workflowsAll": workflows_all, "referencedBySchemes": schemes_referencing}

    def _collect_workflow_scheme(self, scheme_id: str) -> Dict[str, Any]:
        scheme = self._get_json(f"/rest/api/3/workflowscheme/{scheme_id}") or {}
        return {"scheme": scheme}

    def _collect_screen(self, screen_id: str) -> Dict[str, Any]:
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

    def _collect_status(self, status_id_or_name: str) -> Dict[str, Any]:
        status = self._get_json(f"/rest/api/3/status/{status_id_or_name}") or {}
        return {"status": status}

    def _collect_filter(self, filter_id: str) -> Dict[str, Any]:
        filt = self._get_json(f"/rest/api/3/filter/{filter_id}") or {}
        shares = self._get_json(f"/rest/api/2/filter/{filter_id}/permission") or {}
        return {"filter": filt, "sharePermissions": shares}

    def backup_items(self, item_type: str, ids: List[str]) -> str:
        collectors: Dict[str, Callable[[str], Dict[str, Any]]] = {
            "Custom Fields": self._collect_custom_field,
            "Workflows": self._collect_workflow,
            "Workflow Schemes": self._collect_workflow_scheme,
            "Workflow Statuses": self._collect_status,
            "Screens": self._collect_screen,
            "Screen Schemes": self._collect_screen_scheme,
            "Dashboards": self._collect_dashboard,
            "Filters": self._collect_filter,
        }
        coll = collectors.get(item_type)
        payload: Dict[str, Any] = {"itemType": item_type, "collectedAt": datetime.now().isoformat() + "Z", "items": {}}
        if not coll:
            payload["note"] = "No collector implemented for this type."
        else:
            for _id in ids:
                try:
                    payload["items"][_id] = coll(_id)
                except Exception as e:
                    log_to_file("backup", f"Collector failed for {item_type}:{_id}: {e}")
                    payload["items"][_id] = {"error": str(e)}
        fname = backup_to_file(payload, f"full_backup_{item_type.replace(' ', '_').lower()}", item_type)
        log_to_file("backup", f"Backed up {len(ids)} {item_type} -> {fname}")
        return fname


# ---------------- GUI ----------------
class JiraToolGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Atlassian Commander by teester")
        self.geometry("1150x740")
        style = ttk.Style(self)
        style.theme_use("clam")

        self.dry_run = tk.BooleanVar(value=True)
        self.current_domain = ""
        self.current_email = ""
        self.current_token = ""

        self.tabs = ttk.Notebook(self)
        self.tabs.pack(fill="both", expand=True)

        self.cleanup_tab = ttk.Frame(self.tabs)
        self.recover_tab = ttk.Frame(self.tabs)
        self.logs_tab = ttk.Frame(self.tabs)
        self.auth_tab = ttk.Frame(self.tabs)

        self.tabs.add(self.cleanup_tab, text="Cleanup")
        self.tabs.add(self.recover_tab, text="Recover")
        self.tabs.add(self.logs_tab, text="Logs & Backups")
        self.tabs.add(self.auth_tab, text="Authentication")

        # State for checkbox trees
        self.cleanup_trees: Dict[str, ttk.Treeview] = {}
        self.cleanup_checked: Dict[str, set[str]] = {}
        self.cleanup_sort_orders: Dict[str, tk.BooleanVar] = {}
        self.cleanup_delete_flags: Dict[str, tk.BooleanVar] = {}
        self.cleanup_search_vars: Dict[str, tk.StringVar] = {}
        self.cleanup_hidden_locked_labels: Dict[str, ttk.Label] = {}

        self.recover_trees: Dict[str, ttk.Treeview] = {}
        self.recover_checked: Dict[str, set[str]] = {}
        self.recover_sort_orders: Dict[str, tk.BooleanVar] = {}
        self.recover_search_vars: Dict[str, tk.StringVar] = {}
        self.recover_include_backups = tk.BooleanVar(value=True)
        self.recover_deep_scan = tk.BooleanVar(value=False)

        # --- Status bar (thread-safe) ---
        self.status_queue: Queue[str] = Queue()
        self.status_var = tk.StringVar(value="Ready.")
        status_frame = ttk.Frame(self)
        status_frame.pack(fill="x", side="bottom")
        ttk.Separator(status_frame, orient="horizontal").pack(fill="x")
        self.status_label = ttk.Label(status_frame, textvariable=self.status_var, anchor="w")
        self.status_label.pack(fill="x", padx=8, pady=4)
        self.after(100, self._pump_status_queue, tuple())

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

        # Build UI
        self._build_cleanup_tab()
        self._build_recover_tab()
        self._build_logs_tab()
        self._build_auth_tab()

        # ---------- threading helper ----------

        def run_worker(self, action: str, domain: str, auth: HTTPBasicAuth, item_type: str, ids: List[str]) -> None:
            self.update_status_and_pump(f"Starting worker for action: {action}", True)
            try:
                # ... other actions ...

                if action == "list_unused_workflows":
                    self.current_domain = domain
                    self.jira_conn = auth
                    results = self.get_unused_workflows()
                    item_names = [f"[{w['name']}] {w['description']}" for w in results]
                    self.item_listbox.delete(0, tk.END)
                    for name in item_names:
                        self.item_listbox.insert(tk.END, name)
                    self.update_status_and_pump(f"Found {len(results)} inactive workflows.", False)
            except Exception as e:
                self.update_status_and_pump(f"Worker failed: {e}", False)
                log_to_file(action, f"Error: {e}")

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
                    self.after(0, lambda: self._emit_status(f"Done: {getattr(fn, '__name__', 'task')}") )
                    return result
                except Exception as e:
                    self._emit_status(f"Error: {e}")
                    log_to_file("runtime", f"Threaded task error: {e}")

            threading.Thread(target=runner, daemon=True).start()

    # ---------- thread-safe message boxes ----------
    def _show_message(self, level: str, title: str, text: str) -> None:
        """Central UI messaging; ensures calls land on Tk thread."""
        log_to_file("ui", f"{level.upper()}: {title} - {text.splitlines()[0][:140]}")

        def _do():
            if level == "error":
                messagebox.showerror(title, text)
            elif level == "warning":
                messagebox.showwarning(title, text)
            else:
                messagebox.showinfo(title, text)

        try:
            self.after(0, _do, ())
        except Exception:
            _do()

    # ---------- Status helpers ----------
    def _emit_status(self, text: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.status_queue.put(f"[{ts}] {text}")

    def _pump_status_queue(self, _event=None):
        try:
            while True:
                msg = self.status_queue.get_nowait()
                self.status_var.set(msg)
        except Empty:
            pass
        finally:
            self.after(150, self._pump_status_queue, tuple())

    # ---------- HTTP ----------
    def jira_auth(self) -> HTTPBasicAuth:
        return HTTPBasicAuth(self.current_email, self.current_token)

    def _get(self, url: str, item_type: str) -> Any:
        try:
            self._emit_status(f"Fetching {item_type} …")
            resp = self._http.get(url, auth=self.jira_auth(), timeout=TIMEOUT)
            resp.raise_for_status()
            if "application/json" in (resp.headers.get("content-type") or ""):
                return resp.json()
            return {}
        except Exception as e:
            self._emit_status(f"Fetch error for {item_type}")
            messagebox.showerror("Fetch Error", f"Failed to fetch {item_type}: {e}")
            return []

    def _delete(self, url: str) -> bool:
        try:
            resp = requests.delete(url, auth=self.jira_auth(), timeout=TIMEOUT)
            return resp.status_code in (200, 202, 204)
        except Exception as e:
            log_to_file("cleanup", f"Delete error at {url}: {e}")
            return False

    def _post(self, url: str, json_body: Optional[dict] = None) -> bool:
        try:
            resp = requests.post(url, auth=self.jira_auth(), json=json_body, timeout=TIMEOUT)
            return resp.status_code in (200, 201, 202, 204)
        except Exception as e:
            log_to_file("recover", f"Post error at {url}: {e}")
            return False

    def test_connection(self) -> bool:
        try:
            url = f"{self.current_domain}/rest/api/3/myself"
            self._emit_status("Testing connection …")
            resp = self._http.get(url, auth=self.jira_auth(), timeout=TIMEOUT)
            resp.raise_for_status()
            self._emit_status("Connection OK")
            return True
        except Exception as e:
            self._emit_status("Connection FAILED")
            messagebox.showerror("Connection Failed", str(e))
            return False

    # ---------- Background threading ----------
    def _thread(self, fn: Callable, *args) -> None:
        """Run blocking work off the Tk thread; UI updates via after()."""
        def runner():
            try:
                name = getattr(fn, "__name__", "task")
                self._emit_status(f"Started: {name}")
                result = fn(*args)
                self.after(0, lambda: self._emit_status(f"Done: {name}"))
                return result
            except Exception as e:
                self._emit_status(f"Error: {e}")
                log_to_file("runtime", f"Threaded task error: {e}")
        threading.Thread(target=runner, daemon=True).start()

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
        all_statuses = self._get_all_statuses()
        used_ids = self._get_status_ids_used_in_workflows()
        unused: List[Dict[str, Any]] = []
        for s in all_statuses:
            sid = str(s.get("id") or s.get("name") or "")
            if sid and sid not in used_ids:
                s["isLocked"] = True  # why: cloud can't delete via REST
                unused.append(s)
        return unused

    # ---------- Fetchers (unused lists) ----------
    def get_unused_custom_fields(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/field"
        fields = self._get(url, "custom fields")
        return [
            f for f in fields
            if isinstance(f, dict)
            and f.get("custom")
            and not f.get("screens")
            and not f.get("projects")
            and not f.get("isLocked", False)
        ]

    def get_unused_workflows(self) -> List[Dict[str, Any]]:
        """Return workflows not referenced by any scheme, including both ID and name.
        Uses the Jira Cloud workflow search endpoint and expands schemes.
        Paginates through all results and filters to only inactive (no schemes or usage)."""
        # Build paginated requests to the official workflow search endpoint
        base = f"{self.current_domain}/rest/api/3/workflow/search?expand=schemes&maxResults=200"
        self.update_status_and_pump("Fetching all workflows…", True)

        all_workflows: List[Dict[str, Any]] = []
        start_at = 0
        while True:
            url = f"{base}&startAt={start_at}"
            data = self._get(url, "workflows")
            if data is None:
                self.update_status_and_pump("Error: Failed to fetch workflows.", True)
                return []

            # Normalize to list
            if isinstance(data, dict) and isinstance(data.get("values"), list):
                chunk = data.get("values", [])
                total = int(data.get("total", 0)) if isinstance(data.get("total", 0), int) else None
            elif isinstance(data, list):
                chunk = data
                total = None
            else:
                self.update_status_and_pump("Error: Unexpected response when fetching workflows.", True)
                return []

            all_workflows.extend(chunk or [])

            # Stop if no pagination info or we've read all
            if not isinstance(data, dict):
                break
            is_last = bool(data.get("isLast", False))
            if is_last:
                break
            # Fallback: advance by returned size or 200
            size = len(chunk or [])
            if size == 0:
                break
            start_at += size

        inactive: List[Dict[str, Any]] = []
        for wf in all_workflows:
            # Some responses nest schemes under a dict or provide a raw list.
            raw_schemes = wf.get("schemes", {}) if isinstance(wf, dict) else {}
            if isinstance(raw_schemes, dict):
                # Accept either {"schemes": [...]} or {"values": [...]} shapes
                schemes_list = raw_schemes.get("schemes") or raw_schemes.get("values") or []
            elif isinstance(raw_schemes, list):
                schemes_list = raw_schemes
            else:
                schemes_list = []

            # Additional safety: some tenants expose other flags that imply usage
            in_use_flags = (
                bool(wf.get("inUse"))
                or bool(wf.get("isActive"))
                or bool(wf.get("includedInProjects"))
                or bool(wf.get("projects"))
            )

            # Consider a workflow inactive when it is not referenced by any scheme or usage flag
            if (not schemes_list) and (not in_use_flags):
                # Jira may return `id` as an object: {"name": "...", "entityId": "..."}
                raw_id = wf.get("id")
                default_name_from_id = None
                if isinstance(raw_id, dict):
                    default_name_from_id = raw_id.get("name")
                    wf_id = str(raw_id.get("entityId") or default_name_from_id or "")
                else:
                    wf_id = str(raw_id or "")

                wf_name = str(wf.get("name") or default_name_from_id or wf_id)

                if wf_id or wf_name:
                    inactive.append({
                        "id": wf_id,
                        "name": wf_name,
                        # keep description if present for export
                        "description": wf.get("description", ""),
                    })

        self.update_status_and_pump(f"Found {len(inactive)} inactive workflows.", True)
        return inactive

    def update_status_and_pump(self, message, is_success):
        # Placeholder for your UI/logging method
        print(f"Status: {message}")

    def get_unused_workflow_schemes(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/workflowscheme"
        data = self._get(url, "workflow schemes")
        return data.get("values", []) if isinstance(data, dict) else data

    def get_unused_screens(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/screens"
        data = self._get(url, "screens")
        return data.get("values", []) if isinstance(data, dict) else data

    def get_unused_screen_schemes(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/screenscheme"
        data = self._get(url, "screen schemes")
        return data.get("values", []) if isinstance(data, dict) else data

    def get_unused_dashboards(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/dashboard"
        all_dashboards_data = self._get(url, "dashboards")
        if not isinstance(all_dashboards_data, dict) or "dashboards" not in all_dashboards_data:
            return []
        all_dashboards = all_dashboards_data.get("dashboards", [])
        return [d for d in all_dashboards if not d.get("favourite", False)]

    def get_unused_filters(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/filter/search"
        all_filters_data = self._get(url, "filters")
        if not isinstance(all_filters_data, dict) or "values" not in all_filters_data:
            return []
        all_filters = all_filters_data.get("values", [])
        return [f for f in all_filters if not f.get("favourite", False)]

    # ---------- Deleters ----------
    def delete_custom_field(self, field_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete custom field: {field_id}")
            return True
        try:
            field_data = self._get(f"{self.current_domain}/rest/api/3/field/{field_id}", item_type="field")
            field_name = str((field_data or {}).get("name", "unnamed"))
            safe_name = re.sub(r'[^A-Za-z0-9_-]+', '_', field_name).strip('_') or 'unnamed'

            # Save to backup/backups/custom_fields/, letting backup_to_file append the timestamp
            backup_to_file(
                field_data,
                f"{safe_name}_Custom_Fields",  # <-- no timestamp here
                item_type="Custom Fields",  # <-- puts it in custom_fields/ folder
            )
        except Exception as e:
            log_to_file("backup", f"Failed to backup custom field {field_id}: {e}")


    def delete_workflow(self, workflow_name: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete workflow: {workflow_name}")
            return True
        url = f"{self.current_domain}/rest/api/3/workflow/{workflow_name}"
        return self._delete(url)

    def delete_workflow_scheme(self, scheme_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete workflow scheme: {scheme_id}")
            return True
        url = f"{self.current_domain}/rest/api/3/workflowscheme/{scheme_id}"
        return self._delete(url)

    def delete_screen(self, screen_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete screen: {screen_id}")
            return True
        url = f"{self.current_domain}/rest/api/3/screens/{screen_id}"
        return self._delete(url)

    def delete_screen_scheme(self, scheme_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete screen scheme: {scheme_id}")
            return True
        url = f"{self.current_domain}/rest/api/3/screenscheme/{scheme_id}"
        return self._delete(url)

    def delete_dashboard(self, dashboard_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete dashboard: {dashboard_id}")
            return True
        url = f"{self.current_domain}/rest/api/3/dashboard/{dashboard_id}"
        return self._delete(url)

    def delete_filter(self, filter_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete filter: {filter_id}")
            return True
        url = f"{self.current_domain}/rest/api/3/filter/{filter_id}"
        return self._delete(url)

    def delete_status(self, status_id_or_name: str) -> bool:
        self._show_message(
            "info",
            "Not Supported by Jira Cloud API",
            "Deleting workflow statuses is not available via REST in Jira Cloud.\n"
            "Use Jira Admin → Issues → Statuses to delete manually.",
        )
        log_to_file("cleanup", f"Status delete requested but not supported: {status_id_or_name}")
        return False

    # ---------- backups helpers (used by Recover UI to pretty-up names) ----------
    def _is_id_like(self, item_type: str, name: str) -> bool:
        if not name:
            return True
        name = str(name).strip().lower()
        if item_type == "Custom Fields":
            return name.startswith("customfield_")
        return False

    def _collect_backup_name_map(self, item_type: str) -> Dict[str, str]:
        name_by_id: Dict[str, str] = {}
        for path in self._iter_backup_json_paths():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
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
                        _id = key
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
        paths: List[str] = []
        for root, _dirs, files in os.walk(BACKUP_DIR):
            for fn in files:
                if fn.lower().endswith(".json"):
                    paths.append(os.path.join(root, fn))
        return paths

    def _extract_normalized_from_backup(self, item_type: str, data: Any) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        if not isinstance(data, dict):
            return out

        def add(id_val: Optional[str], name_val: Optional[str]) -> None:
            if not id_val and not name_val:
                return
            out.append({
                "id": str(id_val or name_val or "unknown"),
                "name": str(name_val or id_val or "unknown")
            })

        # Special case: some backups are single field objects saved at top level
        # (e.g., from delete_custom_field()). Recognize and extract them.
        if item_type == "Custom Fields":
            top_id = data.get("id")
            top_name = data.get("name")
            # Jira field objects typically have these keys; accept if present
            if (top_id or top_name) and any(k in data for k in ("schema", "scope", "key", "custom", "type")):
                add(top_id, top_name)
                return out

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

    # ---------- Auth helpers ----------
    def _load_selected_profile(self) -> None:
        profiles = load_auth_profiles()
        prof = profiles.get(self.profile_var.get(), {})
        domain = prof.get("domain", "")
        if not domain.startswith("http"):
            messagebox.showerror("Error", "Invalid or missing domain in the selected profile.")
            return
        self.domain_var.set(domain)
        self.email_var.set(prof.get("email", ""))
        self.token_var.set(prof.get("token", ""))

    def _save_profile(self) -> None:
        name = self.profile_var.get().strip()
        if not name:
            messagebox.showerror("Error", "Profile name required.")
            return
        profiles = load_auth_profiles()
        profiles[name] = {
            "domain": self.domain_var.get().strip(),
            "email": self.email_var.get().strip(),
            "token": self.token_var.get().strip(),
        }
        save_auth_profiles(profiles)
        self.profile_dropdown["values"] = list(profiles.keys())
        messagebox.showinfo("Saved", f"Profile '{name}' saved.")

    def _apply_profile_fields(self) -> None:
        self.current_domain = self.domain_var.get().strip()
        self.current_email = self.email_var.get().strip()
        self.current_token = self.token_var.get().strip()

    def _test_conn_clicked(self) -> None:
        self._apply_profile_fields
        self._apply_profile_fields()
        if self.test_connection():
            messagebox.showinfo("Success", "Connection successful!")

    # ---------- UI building ----------
    def _build_checkbox_tree(self, parent: tk.Widget) -> ttk.Treeview:
        cols = ("check", "id", "name")
        tree = ttk.Treeview(parent, columns=cols, show="headings", selectmode="extended", height=18)
        tree.heading("check", text="✓")
        tree.heading("id", text="ID")
        tree.heading("name", text="Name / Description")
        tree.column("check", width=40, anchor="center")
        tree.column("id", width=220, anchor="w")
        tree.column("name", width=720, anchor="w")

        def on_click(event):
            col = tree.identify_column(event.x)
            if col != "#1":
                return
            row = tree.identify_row(event.y)
            if not row:
                return
            self._toggle_tree_item(tree, row)
            return "break"

        tree.bind("<Button-1>", on_click)
        tree.bind("<space>", lambda e: ([self._toggle_tree_item(tree, iid) for iid in tree.selection()], "break")[1])
        return tree

    def _build_cleanup_tab(self) -> None:
        notebook = ttk.Notebook(self.cleanup_tab)
        notebook.pack(fill="both", expand=True)

        # Map type → finder function (do NOT call here)
        self.cleanup_types: Dict[str, Callable[[], List[Dict[str, Any]]]] = {
            "Custom Fields": self.get_unused_custom_fields,
            "Workflows": self.get_unused_workflows,
            "Workflow Schemes": self.get_unused_workflow_schemes,
            "Workflow Statuses": self.get_unused_workflow_statuses,
            "Screens": self.get_unused_screens,
            "Screen Schemes": self.get_unused_screen_schemes,
            "Dashboards": self.get_unused_dashboards,
            "Filters": self.get_unused_filters,
        }
        # Type → deleter
        self.cleanup_delete_funcs: Dict[str, Callable[[str], bool]] = {
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

            ttk.Button(actions, text="Find Unused", command=lambda n=name: self._thread(self._load_unused_items, n)).pack(side="left", padx=2)

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
            ttk.Button(actions, text="Check All", command=lambda n=name: self._check_all("recover", n)).pack(side="left", padx=2)
            ttk.Button(actions, text="Uncheck All", command=lambda n=name: self._uncheck_all("recover", n)).pack(side="left", padx=2)
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
                ttk.Label(actions, text="Note: Status recovery via REST is not available in Jira Cloud.").pack(side="right", padx=8)

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

    # ---------- thread-safe status update ----------
    def update_status_and_pump(self, message: str, is_busy: bool) -> None:
        """Update the status label and process pending Tkinter events."""
        if hasattr(self, 'status_label'):
            self.status_label.config(text=message)
    # ---------- Tree helpers ----------
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

    def _resort_tree(self, mode: str, item_type: str) -> None:
        tree, checked = self._tree_sets(mode, item_type)
        rows = []
        for iid in tree.get_children(""):
            id_val = tree.set(iid, "id")
            name_val = tree.set(iid, "name")
            rows.append((iid, id_val, name_val, iid in checked))
        ascending = (self.cleanup_sort_orders if mode == "cleanup" else self.recover_sort_orders)[item_type].get()
        rows.sort(key=lambda r: (r[2] or "").lower(), reverse=not ascending)
        tree.delete(*tree.get_children(""))
        for iid, id_val, name_val, was_checked in rows:
            tree.insert("", "end", iid=iid, values=("☑" if was_checked else "☐", id_val, name_val))

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
                messagebox.showwarning("Nothing to export", "No items match the selection.")
                self._emit_status("Export skipped: nothing to export")
                return
            base = f"{mode}_{item_type.replace(' ', '_').lower()}" + ("_checked" if only_checked else "_all")
            out_path = self._export_rows(rows, base)
            self._emit_status(f"Export complete: {os.path.basename(out_path)}")
            messagebox.showinfo("Export complete", f"Saved: {out_path}")
        except Exception as e:
            self._emit_status("Export failed")
            messagebox.showerror("Export failed", str(e))

    # ---------- Cleanup flows ----------
    def _render_items(self, mode: str, item_type: str) -> None:
        tree, checked = self._tree_sets(mode, item_type)
        search = (self.cleanup_search_vars if mode == "cleanup" else self.recover_search_vars)[item_type].get().strip().lower()
        ascending = (self.cleanup_sort_orders if mode == "cleanup" else self.recover_sort_orders)[item_type].get()
        # Rebuild rows based on current content
        rows = []
        for iid in tree.get_children(""):
            id_val = tree.set(iid, "id")
            name_val = tree.set(iid, "name")
            rows.append((iid, id_val, name_val))
        tree.delete(*tree.get_children(""))
        rows = [r for r in rows if (not search) or (search in (r[2] or "").lower())]
        rows.sort(key=lambda r: (r[2] or "").lower(), reverse=not ascending)
        for iid, id_val, name_val in rows:
            tree.insert("", "end", iid=iid, values=("☑" if iid in checked else "☐", id_val, name_val))

    def _load_unused_items(self, item_type: str) -> None:
        self._emit_status(f"Finding unused {item_type} …")
        tree = self.cleanup_trees[item_type]
        checked = self.cleanup_checked[item_type]
        for iid in tree.get_children(""):
            tree.delete(iid)
        fetcher = self.cleanup_types[item_type]
        items = fetcher() or []
        items_sorted = sorted(items, key=lambda x: (x.get("name") or x.get("description") or "").lower())
        if not self.cleanup_sort_orders[item_type].get():
            items_sorted.reverse()
        for item in items_sorted:
            _id = str(item.get("id") or item.get("name") or item.get("key") or "unknown")
            name = item.get("name") or item.get("description") or _id
            tree.insert("", "end", iid=_id, values=("☑" if _id in checked else "☐", _id, name))
        self._emit_status(f"Loaded {len(items_sorted)} unused {item_type}")

    def _delete_unused_items(self, item_type: str) -> None:
        checked_ids = sorted(self.cleanup_checked[item_type])
        if not checked_ids:
            messagebox.showwarning("No Selection", f"No {item_type} checked for deletion.")
            self._emit_status("Delete skipped: nothing checked")
            return

        self._emit_status(f"Backing up {len(checked_ids)} {item_type} …")
        backup_mgr = BackupManager(self.current_domain, self.jira_auth())
        backup_file = backup_mgr.backup_items(item_type, checked_ids)
        log_to_file("cleanup", f"Pre-delete backup saved: {backup_file}")
        self._emit_status("Backup complete")

        if not self.cleanup_delete_flags[item_type].get():
            log_to_file("cleanup", f"[DRY RUN] Checked {item_type} NOT deleted due to checkbox.")
            messagebox.showinfo("Dry Run", "Deletion skipped due to 'Enable Deletion' being unchecked.")
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
        messagebox.showinfo("Cleanup Complete", f"Deleted {ok}/{len(checked_ids)} {item_type}")

    # ---------- Deleted fetchers (Recover lists) ----------

    def get_deleted_fields(self) -> List[Dict[str, Any]]:
        """Return deleted/trashed custom fields by merging Jira API (when supported) with backups.
        Some tenants do not support `expand=trashed` on `/field/search`, so we avoid that path.
        """
        api_rows: List[Dict[str, Any]] = []

        # 1) Primary: Jira API with status=deleted (supported on many Cloud tenants)
        try:
            url_status = f"{self.current_domain}/rest/api/3/field/search?status=deleted&maxResults=200"
            data = self._get(url_status, "deleted fields")
            if isinstance(data, dict) and isinstance(data.get("values"), list):
                api_rows.extend([r for r in data.get("values", []) if isinstance(r, dict)])
        except Exception:
            # _get already surfaced an error; proceed to backups
            pass

        # 2) Backups (full and per-item)
        backup_rows = self._list_from_backups("Custom Fields")

        # 3) Merge uniquely, preferring entries with human-friendly names
        merged = self._merge_unique(api_rows, backup_rows, id_key="id", item_type="Custom Fields")
        return merged

    def get_deleted_workflows(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/workflow?deleted=true"
        data = self._get(url, "deleted workflows")
        if isinstance(data, dict) and "values" in data:
            return data["values"]
        return load_backup_items("deleted_workflows")

    def get_deleted_workflow_schemes(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/workflowscheme?deleted=true"
        data = self._get(url, "deleted workflow schemes")
        if isinstance(data, dict) and "values" in data:
            return data["values"]
        return load_backup_items("deleted_workflow_schemes")

    def get_deleted_screens(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/screens?deleted=true"
        data = self._get(url, "deleted screens")
        if isinstance(data, dict) and "values" in data:
            return data["values"]
        return load_backup_items("deleted_screens")

    def get_deleted_screen_schemes(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/screenscheme?deleted=true"
        data = self._get(url, "deleted screen schemes")
        if isinstance(data, dict) and "values" in data:
            return data["values"]
        return load_backup_items("deleted_screen_schemes")

    def get_deleted_dashboards(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/dashboard?deleted=true"
        data = self._get(url, "deleted dashboards")
        if isinstance(data, dict) and "dashboards" in data:
            return data["dashboards"]
        return load_backup_items("deleted_dashboards")

    def get_deleted_filters(self) -> List[Dict[str, Any]]:
        backup = self._load_full_backup("Filters")
        items = backup.get("items", {}) if isinstance(backup, dict) else {}
        out: List[Dict[str, Any]] = []
        for old_id, bundle in items.items():
            filt = (bundle or {}).get("filter", {}) or {}
            name = filt.get("name") or f"Filter {old_id}"
            out.append({"id": old_id, "name": name})
        return out

    def get_deleted_statuses(self) -> List[Dict[str, Any]]:
        return self._list_from_backups("Workflow Statuses")

    # ---------- Recover flows ----------
    def _load_deleted_items(self, item_type: str) -> None:
        self._emit_status(f"Loading deleted {item_type} …")
        tree = self.recover_trees[item_type]
        checked = self.recover_checked[item_type]
        for iid in tree.get_children(""):
            tree.delete(iid)
        spec = self.recover_types[item_type]
        items = spec["fetch"]() or []
        id_key = spec["id_key"]
        name_key = spec["name_key"]
        items_sorted = sorted(items, key=lambda x: (x.get(name_key, "") or "").lower())
        if not self.recover_sort_orders[item_type].get():
            items_sorted.reverse()
        for item in items_sorted:
            _id = str(item.get(id_key) or "unknown")
            name = item.get(name_key) or _id
            tree.insert("", "end", iid=_id, values=("☑" if _id in checked else "☐", _id, name))
        self._emit_status(f"Loaded {len(items_sorted)} deleted {item_type}")

    def _recover_selected(self, item_type: str) -> None:
        checked_ids = sorted(self.recover_checked[item_type])
        if not checked_ids:
            messagebox.showwarning("No Selection", f"No {item_type} checked for recovery.")
            self._emit_status("Recovery skipped: nothing checked")
            return
        flag = getattr(self, f"{item_type.lower().replace(' ', '_')}_recover_enabled").get()
        if not flag:
            log_to_file("recover", f"[DRY RUN] Recovery skipped for: {', '.join(checked_ids)}")
            messagebox.showinfo("Dry Run", "Recovery skipped due to 'Enable Recovery' being unchecked.")
            self._emit_status("Recovery skipped (checkbox off)")
            return
        self._emit_status(f"Recovering {len(checked_ids)} {item_type} …")
        recovered = self.recover_types[item_type]["recover"](checked_ids)
        if recovered:
            self._emit_status(f"Recovered {len(recovered)} {item_type}")
            messagebox.showinfo("Recovery Completed", f"Recovered: {', '.join(recovered)}")
        else:
            self._emit_status("No items were recovered")
            messagebox.showinfo("Recovery Completed", "No items were recovered.")

    # ---------- Recover actions ----------
    def recover_deleted_fields(self, field_ids: List[str]) -> List[str]:
        recovered: List[str] = []
        for fid in field_ids:
            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recover custom field {fid}")
                recovered.append(fid)
                continue
            url = f"{self.current_domain}/rest/api/3/field/{fid}"
            if self._post(url):
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
            if self._post(url):
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
            if self._post(url):
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
            if self._post(url):
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
            if self._post(url):
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
            if self._post(url):
                recovered.append(did)
                log_to_file("recover", f"Recovered dashboard {did}")
        return recovered

    def recover_deleted_filters(self, filter_ids: List[str]) -> List[str]:
        """Recreate filters from backup since Jira Cloud has no filter restore endpoint."""
        recreated: List[str] = []
        if not filter_ids:
            return recreated

        backup = self._load_full_backup("Filters")
        items = backup.get("items", {}) if isinstance(backup, dict) else {}

        for old_id in filter_ids:
            bundle = items.get(old_id) or {}
            filt = bundle.get("filter") or {}
            shares = bundle.get("sharePermissions") or []

            name = filt.get("name")
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

            create_url = f"{self.current_domain}/rest/api/3/filter"
            create_body = {"name": name, "jql": jql, "description": description, "favourite": False}
            try:
                r = requests.post(create_url, auth=self.jira_auth(), json=create_body, timeout=TIMEOUT)
                if r.status_code not in (200, 201):
                    log_to_file("recover", f"Create filter '{name}' failed: {r.status_code} {r.text}")
                    continue
                new_filter = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                new_id = (new_filter or {}).get("id")
            except Exception as e:
                log_to_file("recover", f"Create filter '{name}' exception: {e}")
                continue

            for perm in shares if isinstance(shares, list) else []:
                try:
                    perm_url = f"{self.current_domain}/rest/api/2/filter/{new_id}/permission"
                    pr = requests.post(perm_url, auth=self.jira_auth(), json=perm, timeout=TIMEOUT)
                    if pr.status_code not in (200, 201):
                        log_to_file("recover", f"Share add failed for '{name}': {pr.status_code} {pr.text}")
                except Exception as e:
                    log_to_file("recover", f"Share add exception for '{name}': {e}")

            if favourite:
                try:
                    fav_url = f"{self.current_domain}/rest/api/3/filter/{new_id}/favourite"
                    fr = requests.put(fav_url, auth=self.jira_auth(), timeout=TIMEOUT)
                    if fr.status_code not in (200, 204):
                        log_to_file("recover", f"Favourite set failed for '{name}': {fr.status_code} {fr.text}")
                except Exception as e:
                    log_to_file("recover", f"Favourite set exception for '{name}': {e}")

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
            "Use Jira Admin → Issues → Statuses to recreate.",
        )
        return []


if __name__ == "__main__":
    app = JiraToolGUI()
    app.mainloop()