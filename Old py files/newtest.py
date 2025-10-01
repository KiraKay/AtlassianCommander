# Jira Cleanup & Recovery Tool - FINAL VERSION WITH ENHANCED WORKFLOW RECOVERY
# All bugs fixed + comprehensive workflow backup and recovery support
import re
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
from urllib3.util.retry import Retry

# optional Excel support
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
        """
        ENHANCED: Collect comprehensive workflow data for backup and potential recovery.
        Includes statuses, transitions, conditions, validators, and all metadata.
        """
        # 1. Get all workflows with expanded details
        workflows_all = self._get_json("/rest/api/3/workflow/search?expand=transitions,statuses") or {}

        # 2. Find the specific workflow
        target_workflow = None
        workflows_list = workflows_all.get("values", []) if isinstance(workflows_all, dict) else []

        for wf in workflows_list:
            wf_name = wf.get("name", "")
            if wf_name.lower() == workflow_name.lower():
                target_workflow = wf
                break

        # 3. Get detailed workflow information if found
        workflow_details = {}
        statuses = []

        if target_workflow:
            entity_id = None
            wf_id = target_workflow.get("id")

            # Handle both string ID and object ID formats
            if isinstance(wf_id, dict):
                entity_id = wf_id.get("entityId")
            else:
                entity_id = wf_id

            if entity_id:
                # Get full workflow details including transitions
                workflow_details = self._get_json(f"/rest/api/3/workflow/{entity_id}") or {}

                # Get detailed status information
                for status in workflow_details.get("statuses", []):
                    status_id = status.get("id")
                    if status_id:
                        status_details = self._get_json(f"/rest/api/3/status/{status_id}") or {}
                        statuses.append(status_details)

        # 4. Get workflow schemes that reference this workflow
        schemes = self._get_json("/rest/api/3/workflowscheme") or {}
        schemes_list = schemes.get("values", []) if isinstance(schemes, dict) else []
        target = (workflow_name or "").lower()
        schemes_referencing = [s for s in schemes_list if target in json.dumps(s).lower()]

        # 5. Compile comprehensive backup
        return {
            "workflowName": workflow_name,
            "workflowDetails": workflow_details,
            "targetWorkflow": target_workflow,
            "allWorkflows": workflows_all,
            "statuses": statuses,
            "referencedBySchemes": schemes_referencing,
            "backupMetadata": {
                "timestamp": datetime.now().isoformat() + "Z",
                "version": "2.0",
                "canAutoRecover": False,
                "recoveryMethod": "manual",
                "note": "Jira Cloud does not support automated workflow recovery via REST API. Use the generated recovery guide for manual recreation."
            }
        }

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

        # Generate filename with item names for better organization
        if len(ids) == 1 and item_type in ("Custom Fields", "Workflows"):
            # Single item backup - use the item name in filename
            single_id = ids[0]
            item_data = payload["items"].get(single_id, {})

            if item_type == "Custom Fields":
                # Extract field name
                field_info = item_data.get("field", {})
                item_name = field_info.get("name", single_id)
            elif item_type == "Workflows":
                # Extract workflow name
                item_name = item_data.get("workflowName", single_id)
            else:
                item_name = single_id

            # Sanitize name for filename
            safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", str(item_name)).strip("_") or "unnamed"
            filename_base = f"{safe_name}_{item_type.replace(' ', '_')}"
        else:
            # Multiple items or other types - use generic name
            filename_base = f"full_backup_{item_type.replace(' ', '_').lower()}"

        fname = backup_to_file(payload, filename_base, item_type)
        log_to_file("backup", f"Backed up {len(ids)} {item_type} -> {fname}")
        return fname


# ---------------- GUI ----------------
class JiraToolGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Atlassian Commander by KIngram")
        self.geometry("1150x740")
        style = ttk.Style(self)
        style.theme_use("clam")

        self.dry_run = tk.BooleanVar(value=False)
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
        self.cleanup_checked: Dict[str, Set[str]] = {}
        self.cleanup_sort_orders: Dict[str, tk.BooleanVar] = {}
        self.cleanup_delete_flags: Dict[str, tk.BooleanVar] = {}
        self.cleanup_search_vars: Dict[str, tk.StringVar] = {}
        self.cleanup_hidden_locked_labels: Dict[str, ttk.Label] = {}

        self.recover_trees: Dict[str, ttk.Treeview] = {}
        self.recover_checked: Dict[str, Set[str]] = {}
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
        self.after(100, self._pump_status_queue)

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

    # ---------- thread-safe message boxes ----------
    def _show_message(self, level: str, title: str, text: str) -> None:
        """Central UI messaging; ensures calls land on Tk thread."""
        log_to_file("ui", f"{level.upper()}: {title} - {text.splitlines()[0][:140]}")

        def _do(*_args):
            if level == "error":
                messagebox.showerror(title, text)
            elif level == "warning":
                messagebox.showwarning(title, text)
            else:
                messagebox.showinfo(title, text)

        try:
            self.after(0, _do)
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
            self.after(150, self._pump_status_queue)

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

    # ---------- Cleanup method to close HTTP session ----------
    def destroy(self):
        """Clean up resources before closing."""
        if hasattr(self, '_http'):
            try:
                self._http.close()
            except Exception:
                pass
        super().destroy()

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
                s["isLocked"] = True
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
        """Return workflows not referenced by any scheme, including both ID and name."""
        base = f"{self.current_domain}/rest/api/3/workflow/search?expand=schemes&maxResults=200"
        self._emit_status("Fetching all workflows…")

        all_workflows: List[Dict[str, Any]] = []
        start_at = 0
        while True:
            url = f"{base}&startAt={start_at}"
            data = self._get(url, "workflows")
            if data is None:
                self._emit_status("Error: Failed to fetch workflows.")
                return []

            if isinstance(data, dict) and isinstance(data.get("values"), list):
                chunk = data.get("values", [])
            elif isinstance(data, list):
                chunk = data
            else:
                self._emit_status("Error: Unexpected response when fetching workflows.")
                return []

            all_workflows.extend(chunk or [])

            if not isinstance(data, dict):
                break
            is_last = bool(data.get("isLast", False))
            if is_last:
                break
            size = len(chunk or [])
            if size == 0:
                break
            start_at += size

        inactive: List[Dict[str, Any]] = []
        for wf in all_workflows:
            raw_schemes = wf.get("schemes", {}) if isinstance(wf, dict) else {}
            if isinstance(raw_schemes, dict):
                schemes_list = raw_schemes.get("schemes") or raw_schemes.get("values") or []
            elif isinstance(raw_schemes, list):
                schemes_list = raw_schemes
            else:
                schemes_list = []

            in_use_flags = (
                    bool(wf.get("inUse"))
                    or bool(wf.get("isActive"))
                    or bool(wf.get("includedInProjects"))
                    or bool(wf.get("projects"))
            )

            if (not schemes_list) and (not in_use_flags):
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
                        "description": wf.get("description", ""),
                    })

        self._emit_status(f"Found {len(inactive)} inactive workflows.")
        return inactive

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
        """Delete a custom field in Jira Cloud."""
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete custom field: {field_id}")
            return True

        field_data = {}
        try:
            search_url = f"{self.current_domain}/rest/api/3/field/search?id={field_id}&maxResults=1"
            search = self._get(search_url, item_type="field search")
            field_items = (search or {}).get("values", []) if isinstance(search, dict) else []
            field_data = field_items[0] if field_items else {}
            field_name = str(field_data.get("name") or "unnamed")
            safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", field_name).strip("_") or "unnamed"

            backup_to_file(
                field_data if isinstance(field_data, dict) else {"id": field_id},
                f"{safe_name}_Custom_Fields",
                item_type="Custom Fields",
            )
        except Exception as e:
            log_to_file("backup", f"Failed to backup custom field {field_id}: {e}")

        trash_url = f"{self.current_domain}/rest/api/3/field/{field_id}/trash"
        ok = self._post(trash_url)
        log_to_file("cleanup", f"Trash field {field_id}: {'OK' if ok else 'FAILED'}")
        if ok:
            return True

        try:
            del_url = f"{self.current_domain}/rest/api/3/field/{field_id}"
            resp = requests.delete(del_url, auth=self.jira_auth(), timeout=TIMEOUT)
            hard_ok = resp.status_code in (200, 202, 204, 303)
            log_to_file("cleanup",
                        f"Hard delete field {field_id}: {resp.status_code} -> {'OK' if hard_ok else 'FAILED'}")
            return hard_ok
        except Exception as e:
            log_to_file("cleanup", f"Hard delete error for field {field_id}: {e}")
            return False

    def delete_workflow(self, workflow_name: str) -> bool:
        """Workflow deletion via REST API is not supported. Creates comprehensive backup."""
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would backup workflow: {workflow_name}")
            return True

        try:
            backup_mgr = BackupManager(self.current_domain, self.jira_auth())
            backup_file = backup_mgr.backup_items("Workflows", [workflow_name])
            log_to_file("cleanup", f"Workflow backed up to: {backup_file}")
            return True
        except Exception as e:
            log_to_file("cleanup", f"Failed to backup workflow {workflow_name}: {e}")
            return False

    def delete_workflow_scheme(self, scheme_id: str) -> bool:
        """Delete a workflow scheme in Jira Cloud with backup."""
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete workflow scheme: {scheme_id}")
            return True

        # Create backup before deletion
        try:
            backup_mgr = BackupManager(self.current_domain, self.jira_auth())
            backup_file = backup_mgr.backup_items("Workflow Schemes", [scheme_id])
            log_to_file("cleanup", f"Workflow scheme backed up to: {backup_file}")
        except Exception as e:
            log_to_file("cleanup", f"Failed to backup workflow scheme {scheme_id}: {e}")

        url = f"{self.current_domain}/rest/api/2/workflowscheme/{scheme_id}"
        return self._delete(url)

    def delete_screen(self, screen_id: str) -> bool:
        """Delete a screen in Jira Cloud with backup."""
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete screen: {screen_id}")
            return True

        # Create backup before deletion
        try:
            backup_mgr = BackupManager(self.current_domain, self.jira_auth())
            backup_file = backup_mgr.backup_items("Screens", [screen_id])
            log_to_file("cleanup", f"Screen backed up to: {backup_file}")
        except Exception as e:
            log_to_file("cleanup", f"Failed to backup screen {screen_id}: {e}")

        url = f"{self.current_domain}/rest/api/3/screens/{screen_id}"
        return self._delete(url)

    def delete_screen_scheme(self, scheme_id: str) -> bool:
        """Delete a screen scheme in Jira Cloud with backup."""
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete screen scheme: {scheme_id}")
            return True

        # Create backup before deletion
        try:
            backup_mgr = BackupManager(self.current_domain, self.jira_auth())
            backup_file = backup_mgr.backup_items("Screen Schemes", [scheme_id])
            log_to_file("cleanup", f"Screen scheme backed up to: {backup_file}")
        except Exception as e:
            log_to_file("cleanup", f"Failed to backup screen scheme {scheme_id}: {e}")

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

    # ---------- NEW: Workflow Recovery Helper Methods ----------

    def _generate_workflow_recovery_instructions(self, workflow_name: str) -> str:
        """Generate detailed instructions for manually recreating a workflow from backup."""
        backup = self._load_full_backup("Workflows")
        items = backup.get("items", {}) if isinstance(backup, dict) else {}
        workflow_data = items.get(workflow_name, {})

        if not workflow_data:
            return f"No backup found for workflow: {workflow_name}"

        details = workflow_data.get("workflowDetails", {})
        statuses = workflow_data.get("statuses", [])

        instructions = []
        instructions.append(f"# Recovery Instructions for Workflow: {workflow_name}\n")
        instructions.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        instructions.append("=" * 80 + "\n\n")

        # Step 1: Create workflow
        instructions.append("## Step 1: Create New Workflow\n")
        instructions.append("1. Go to Jira Admin → Issues → Workflows\n")
        instructions.append("2. Click 'Add Workflow'\n")
        instructions.append(f"3. Name: **{workflow_name}**\n")
        description = details.get("description", "")
        if description:
            instructions.append(f"4. Description: {description}\n")
        instructions.append("\n")

        # Step 2: Add statuses
        instructions.append("## Step 2: Add Statuses\n")
        instructions.append("Add the following statuses to your workflow:\n\n")
        for i, status in enumerate(statuses, 1):
            status_name = status.get("name", "Unknown")
            status_category = status.get("statusCategory", {}).get("name", "")
            instructions.append(f"{i}. **{status_name}**\n")
            instructions.append(f"   - Category: {status_category}\n")
            instructions.append(f"   - ID: {status.get('id', 'N/A')}\n")
            instructions.append("\n")

        # Step 3: Add transitions
        instructions.append("## Step 3: Configure Transitions\n")
        transitions = details.get("transitions", [])
        if transitions:
            instructions.append("Add the following transitions:\n\n")
            for i, trans in enumerate(transitions, 1):
                trans_name = trans.get("name", "Unknown")
                from_status = trans.get("from", [])
                to_status = trans.get("to", {}).get("name", "Unknown")

                instructions.append(f"{i}. **{trans_name}**\n")
                if from_status:
                    from_names = [s.get("name", "Unknown") for s in from_status]
                    instructions.append(f"   - From: {', '.join(from_names)}\n")
                instructions.append(f"   - To: {to_status}\n")

                screen = trans.get("screen", {})
                if screen:
                    instructions.append(f"   - Screen: {screen.get('name', 'N/A')}\n")

                instructions.append("\n")

        # Step 4: Workflow schemes
        schemes = workflow_data.get("referencedBySchemes", [])
        if schemes:
            instructions.append("## Step 4: Associate with Workflow Schemes\n")
            instructions.append("This workflow was used in the following schemes:\n\n")
            for scheme in schemes:
                scheme_name = scheme.get("name", "Unknown")
                instructions.append(f"- {scheme_name}\n")
            instructions.append("\n")

        # Step 5: Final notes
        instructions.append("## Step 5: Publish Workflow\n")
        instructions.append("1. Review all statuses and transitions\n")
        instructions.append("2. Click 'Publish' to make the workflow active\n")
        instructions.append("3. Associate with appropriate workflow schemes\n")
        instructions.append("4. Test the workflow with a test issue\n")

        return "".join(instructions)

    def _show_workflow_backup_details(self) -> None:
        """Show detailed backup information in a popup window."""
        checked_ids = sorted(self.recover_checked["Workflows"])
        if not checked_ids:
            messagebox.showwarning("No Selection", "Please select workflows to view.")
            return

        popup = tk.Toplevel(self)
        popup.title("Workflow Backup Details")
        popup.geometry("800x600")

        text_frame = ttk.Frame(popup)
        text_frame.pack(fill="both", expand=True, padx=10, pady=10)

        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side="right", fill="y")

        text_widget = tk.Text(text_frame, wrap="word", yscrollcommand=scrollbar.set)
        text_widget.pack(side="left", fill="both", expand=True)
        scrollbar.config(command=text_widget.yview)

        for workflow_name in checked_ids:
            backup = self._load_full_backup("Workflows")
            items = backup.get("items", {}) if isinstance(backup, dict) else {}
            workflow_data = items.get(workflow_name, {})

            if workflow_data:
                text_widget.insert("end", f"{'=' * 80}\n")
                text_widget.insert("end", f"Workflow: {workflow_name}\n")
                text_widget.insert("end", f"{'=' * 80}\n\n")

                details = workflow_data.get("workflowDetails", {})
                text_widget.insert("end", f"Description: {details.get('description', 'N/A')}\n")
                text_widget.insert("end", f"Entity ID: {details.get('id', 'N/A')}\n\n")

                statuses = workflow_data.get("statuses", [])
                text_widget.insert("end", f"Statuses ({len(statuses)}):\n")
                for status in statuses:
                    text_widget.insert("end", f"  - {status.get('name', 'Unknown')}\n")
                text_widget.insert("end", "\n")

                transitions = details.get("transitions", [])
                text_widget.insert("end", f"Transitions ({len(transitions)}):\n")
                for trans in transitions:
                    text_widget.insert("end", f"  - {trans.get('name', 'Unknown')}\n")
                text_widget.insert("end", "\n\n")

        text_widget.config(state="disabled")
        ttk.Button(popup, text="Close", command=popup.destroy).pack(pady=10)

    def _generate_and_save_recovery_guide(self) -> None:
        """Generate and save recovery guide for selected workflows."""
        checked_ids = sorted(self.recover_checked["Workflows"])
        if not checked_ids:
            messagebox.showwarning("No Selection", "Please select workflows to generate guide.")
            return

        guides = []
        for workflow_name in checked_ids:
            guide = self._generate_workflow_recovery_instructions(workflow_name)
            guides.append(guide)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(EXPORT_DIR, f"workflow_recovery_guide_{timestamp}.md")

        with open(filename, "w", encoding="utf-8") as f:
            f.write("\n\n".join(guides))

        messagebox.showinfo(
            "Guide Generated",
            f"Recovery guide saved to:\n{filename}\n\n"
            "Open this file to see step-by-step instructions for recreating your workflows."
        )
        log_to_file("recover", f"Generated recovery guide: {filename}")

    def _export_workflow_backup_json(self) -> None:
        """Export workflow backup as JSON for external tools."""
        checked_ids = sorted(self.recover_checked["Workflows"])
        if not checked_ids:
            messagebox.showwarning("No Selection", "Please select workflows to export.")
            return

        backup = self._load_full_backup("Workflows")
        items = backup.get("items", {}) if isinstance(backup, dict) else {}

        export_data = {
            "exportedAt": datetime.now().isoformat() + "Z",
            "workflows": {}
        }

        for workflow_name in checked_ids:
            if workflow_name in items:
                export_data["workflows"][workflow_name] = items[workflow_name]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(EXPORT_DIR, f"workflow_backup_{timestamp}.json")

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2)

        messagebox.showinfo(
            "Export Complete",
            f"Workflow backup exported to:\n{filename}\n\n"
            "This JSON file contains complete workflow structure and can be used with external tools."
        )
        log_to_file("recover", f"Exported workflow backup: {filename}")

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

        if item_type == "Custom Fields":
            top_id = data.get("id")
            top_name = data.get("name")
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
                for wf_key, bundle in items.items():
                    # Extract workflow name from bundle
                    wf_name = (bundle or {}).get("workflowName") or meta_name(bundle) or wf_key
                    # Use workflow name as both ID and name for display
                    add(wf_name, wf_name)
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

    def _merge_unique(self, items: List[Dict[str, Any]], extra: List[Dict[str, Any]], id_key: str, item_type: str) -> \
    List[Dict[str, Any]]:
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
        self.cleanup_delete_funcs: Dict[str, Callable[[str], bool]] = {
            "Custom Fields": self.delete_custom_field,
            "Workflows": self.delete_workflow,
            "Workflow Schemes": self.delete_workflow_scheme,
            "Workflow Statuses": self.delete_status,
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
            ttk.Button(top, text="Clear",
                       command=lambda v=sv, n=name: (v.set(""), self._render_items("cleanup", n))).pack(side="left",
                                                                                                        padx=2)

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

            ttk.Button(actions, text="Find Unused",
                       command=lambda n=name: self._thread(self._load_unused_items, n)).pack(side="left", padx=2)

            hidden_lbl = ttk.Label(actions, text="Hidden (locked): 0")
            hidden_lbl.pack(side="left", padx=10)
            self.cleanup_hidden_locked_labels[name] = hidden_lbl

            ttk.Button(actions, text="Check All", command=lambda n=name: self._check_all("cleanup", n)).pack(
                side="left", padx=2)
            ttk.Button(actions, text="Uncheck All", command=lambda n=name: self._uncheck_all("cleanup", n)).pack(
                side="left", padx=2)
            ttk.Button(actions, text="Invert", command=lambda n=name: self._invert_all("cleanup", n)).pack(side="left",
                                                                                                           padx=2)

            ttk.Button(actions, text="Export Checked to Excel",
                       command=lambda n=name: self._export_tree("cleanup", n, only_checked=True)).pack(side="right",
                                                                                                       padx=2)
            ttk.Button(actions, text="Export All to Excel",
                       command=lambda n=name: self._export_tree("cleanup", n, only_checked=False)).pack(side="right",
                                                                                                        padx=2)

            del_btn = ttk.Button(actions, text="Delete Checked",
                                 command=lambda n=name: self._thread(self._delete_unused_items, n))
            del_btn.pack(side="right", padx=12)

            if name in ("Workflow Statuses", "Workflows"):
                del_btn.state(["disabled"])
                if name == "Workflows":
                    del_btn.configure(text="Backup Only (Manual deletion required)")
                    ttk.Label(actions,
                              text="Note: Workflow deletion must be done in Jira Admin → Issues → Workflows.").pack(
                        side="right", padx=8)
                else:
                    del_btn.configure(text="Delete (Not supported in Jira Cloud)")
                    ttk.Label(actions, text="Note: Status deletion via REST is not available in Jira Cloud.").pack(
                        side="right", padx=8)
            else:
                del_flag = tk.BooleanVar(value=True)
                ttk.Checkbutton(actions, text="Enable Deletion", variable=del_flag).pack(side="right", padx=8)
                self.cleanup_delete_flags[name] = del_flag

            search_box.bind("<KeyRelease>", lambda _e, n=name: self._render_items("cleanup", n))

    def _build_recover_tab(self) -> None:
        notebook = ttk.Notebook(self.recover_tab)
        notebook.pack(fill="both", expand=True)

        self.recover_types = {
            "Custom Fields": {"fetch": self.get_deleted_fields, "recover": self.recover_deleted_fields, "id_key": "id",
                              "name_key": "name"},
            "Workflows": {"fetch": self.get_deleted_workflows, "recover": self.recover_deleted_workflows,
                          "id_key": "name", "name_key": "name"},
            "Workflow Schemes": {"fetch": self.get_deleted_workflow_schemes,
                                 "recover": self.recover_deleted_workflow_schemes, "id_key": "id", "name_key": "name"},
            "Workflow Statuses": {"fetch": self.get_deleted_statuses, "recover": self.recover_deleted_statuses,
                                  "id_key": "id", "name_key": "name"},
            "Screens": {"fetch": self.get_deleted_screens, "recover": self.recover_deleted_screens, "id_key": "id",
                        "name_key": "name"},
            "Screen Schemes": {"fetch": self.get_deleted_screen_schemes, "recover": self.recover_deleted_screen_schemes,
                               "id_key": "id", "name_key": "name"},
            "Dashboards": {"fetch": self.get_deleted_dashboards, "recover": self.recover_deleted_dashboards,
                           "id_key": "id", "name_key": "name"},
            "Filters": {"fetch": self.get_deleted_filters, "recover": self.recover_deleted_filters, "id_key": "id",
                        "name_key": "name"},
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
                       command=lambda v=sv, n=name: (v.set(""), self._render_items("recover", n))).pack(side="left",
                                                                                                        padx=2)

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

            # ENHANCED: Add workflow-specific buttons
            if name == "Workflows":
                ttk.Button(actions, text="View Backup Details",
                           command=self._show_workflow_backup_details).pack(side="left", padx=2)
                ttk.Button(actions, text="Generate Recovery Guide",
                           command=self._generate_and_save_recovery_guide).pack(side="left", padx=2)
                ttk.Button(actions, text="Export Backup JSON",
                           command=self._export_workflow_backup_json).pack(side="left", padx=2)

            ttk.Button(actions, text="Check All", command=lambda n=name: self._check_all("recover", n)).pack(
                side="left", padx=2)
            ttk.Button(actions, text="Uncheck All", command=lambda n=name: self._uncheck_all("recover", n)).pack(
                side="left", padx=2)
            ttk.Button(actions, text="Invert", command=lambda n=name: self._invert_all("recover", n)).pack(side="left",
                                                                                                           padx=2)

            ttk.Button(actions, text="Export Checked to Excel",
                       command=lambda n=name: self._export_tree("recover", n, only_checked=True)).pack(side="right",
                                                                                                       padx=2)
            ttk.Button(actions, text="Export All to Excel",
                       command=lambda n=name: self._export_tree("recover", n, only_checked=False)).pack(side="right",
                                                                                                        padx=2)

            flag = tk.BooleanVar(value=True)
            ttk.Checkbutton(actions, text="Enable Recovery", variable=flag).pack(side="right", padx=8)
            setattr(self, f"{name.lower().replace(' ', '_')}_recover_enabled", flag)

            rec_btn = ttk.Button(actions, text="Recover Checked",
                                 command=lambda n=name: self._thread(self._recover_selected, n))
            rec_btn.pack(side="right", padx=12)

            if name in ("Workflow Statuses", "Workflows"):
                rec_btn.state(["disabled"])
                if name == "Workflows":
                    rec_btn.configure(text="Manual Recovery Required")
                    ttk.Label(actions, text=(
                        "Note: Use 'Generate Recovery Guide' button for step-by-step instructions."
                    )).pack(side="right", padx=8)
                else:
                    rec_btn.configure(text="Recover (Not supported in Jira Cloud)")
                    ttk.Label(actions, text=(
                        "Note: Recovery via REST is not available in Jira Cloud for this type."
                    )).pack(side="right", padx=8)

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
        ttk.Entry(form, textvariable=self.token_var, width=60, show="*").grid(row=2, column=1, padx=6, pady=3,
                                                                              sticky="we")

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

    # ---------- Tree helpers ----------
    def _tree_sets(self, mode: str, item_type: str) -> Tuple[ttk.Treeview, Set[str]]:
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
        search = (self.cleanup_search_vars if mode == "cleanup" else self.recover_search_vars)[
            item_type].get().strip().lower()
        ascending = (self.cleanup_sort_orders if mode == "cleanup" else self.recover_sort_orders)[item_type].get()
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
        """Special handling for workflows (backup only)."""
        checked_ids = sorted(self.cleanup_checked[item_type])
        if not checked_ids:
            messagebox.showwarning("No Selection", f"No {item_type} checked for deletion.")
            self._emit_status("Delete skipped: nothing checked")
            return

        if item_type == "Workflows":
            self._emit_status(f"Backing up {len(checked_ids)} workflows…")
            backup_mgr = BackupManager(self.current_domain, self.jira_auth())
            backup_file = backup_mgr.backup_items(item_type, checked_ids)
            log_to_file("cleanup", f"Workflows backed up: {backup_file}")

            messagebox.showinfo(
                "Backup Complete",
                f"Backed up {len(checked_ids)} workflow(s) to:\n{backup_file}\n\n"
                "Comprehensive backup includes:\n"
                "• Workflow structure (statuses, transitions)\n"
                "• Conditions and validators\n"
                "• Associated schemes\n"
                "• All metadata\n\n"
                "To delete these workflows:\n"
                "1. Go to Jira Admin → Issues → Workflows\n"
                "2. Find each workflow\n"
                "3. Delete manually through the UI\n\n"
                "To recover, use the Recovery tab to generate step-by-step instructions."
            )
            self._emit_status(f"Backed up {len(checked_ids)} workflows")
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
        failed = []

        for _id in checked_ids:
            if self.dry_run.get():
                log_to_file("cleanup", f"[DRY RUN] Would delete {item_type[:-1]}: {_id}")
                ok += 1
                continue
            if deleter(_id):
                ok += 1
            else:
                failed.append(_id)

        if failed:
            summary = f"Deleted {ok}/{len(checked_ids)} {item_type}\n\nFailed:\n" + "\n".join(failed[:5])
            if len(failed) > 5:
                summary += f"\n... and {len(failed) - 5} more"
            messagebox.showwarning("Cleanup Completed with Errors", summary)
        else:
            messagebox.showinfo("Cleanup Complete", f"Successfully deleted {ok}/{len(checked_ids)} {item_type}")

        self._emit_status(f"Deletion finished: {ok}/{len(checked_ids)} {item_type}")

    # ---------- Deleted fetchers (Recover lists) ----------

    def get_deleted_fields(self) -> List[Dict[str, Any]]:
        """Return deleted/trashed custom fields."""
        api_rows: List[Dict[str, Any]] = []

        for status_value in ("trashed", "deleted"):
            try:
                url_status = f"{self.current_domain}/rest/api/3/field/search?status={status_value}&maxResults=200"
                data = self._get(url_status, f"{status_value} fields")
                if isinstance(data, dict) and isinstance(data.get("values"), list):
                    api_rows.extend([r for r in data.get("values", []) if isinstance(r, dict)])
            except Exception:
                pass

        backup_rows = self._list_from_backups("Custom Fields")
        merged = self._merge_unique(api_rows, backup_rows, id_key="id", item_type="Custom Fields")
        return merged

    def get_deleted_workflows(self) -> List[Dict[str, Any]]:
        """Return deleted workflows from backups."""
        return self._list_from_backups("Workflows")

    def get_deleted_workflow_schemes(self) -> List[Dict[str, Any]]:
        """Return deleted workflow schemes from backups."""
        return self._list_from_backups("Workflow Schemes")

    def get_deleted_screens(self) -> List[Dict[str, Any]]:
        """Return deleted screens from backups."""
        return self._list_from_backups("Screens")

    def get_deleted_screen_schemes(self) -> List[Dict[str, Any]]:
        """Return deleted screen schemes from backups."""
        return self._list_from_backups("Screen Schemes")

    def get_deleted_dashboards(self) -> List[Dict[str, Any]]:
        """Return deleted dashboards from backups."""
        return self._list_from_backups("Dashboards")

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
        """Restore custom fields that are in the Jira trash."""
        recovered: List[str] = []
        for fid in field_ids:
            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would restore custom field {fid}")
                recovered.append(fid)
                continue

            url = f"{self.current_domain}/rest/api/3/field/{fid}/restore"
            try:
                r = requests.post(url, auth=self.jira_auth(), timeout=TIMEOUT)
                status = r.status_code

                if status in (200, 201, 202, 204):
                    recovered.append(fid)
                    log_to_file("recover", f"Restored custom field {fid}")
                else:
                    msg = r.text if getattr(r, "text", None) else ""
                    if status == 404:
                        log_to_file("recover",
                                    f"Restore failed for {fid}: 404 Not Found (likely hard-deleted or not in trash). {msg}")
                    elif status == 400:
                        log_to_file("recover",
                                    f"Restore failed for {fid}: 400 Bad Request (check field state/permissions). {msg}")
                    elif status == 401:
                        log_to_file("recover",
                                    f"Restore failed for {fid}: 401 Unauthorized (check email/API token). {msg}")
                    elif status == 403:
                        log_to_file("recover",
                                    f"Restore failed for {fid}: 403 Forbidden (admin permissions required). {msg}")
                    else:
                        log_to_file("recover", f"Restore failed for {fid}: {status} {msg}")
            except Exception as e:
                log_to_file("recover", f"Restore exception for {fid}: {e}")
        return recovered

    def recover_deleted_workflows(self, names: List[str]) -> List[str]:
        """Workflow recovery via REST API is not supported. Provides instructions."""
        if not names:
            return []

        backup_info = []
        for name in names:
            backup = self._load_full_backup("Workflows")
            items = backup.get("items", {}) if isinstance(backup, dict) else {}
            if name in items:
                backup_info.append(f"✓ {name} - backup available")
            else:
                backup_info.append(f"✗ {name} - no backup found")

        message = (
                "Workflow recovery via REST API is not supported in Jira Cloud.\n\n"
                "To recover workflows:\n"
                "1. Use 'Generate Recovery Guide' button for step-by-step instructions\n"
                "2. Go to Jira Admin → Issues → Workflows\n"
                "3. Create a new workflow with the same name\n"
                "4. Follow the generated guide to recreate statuses and transitions\n\n"
                "Backup status:\n" + "\n".join(backup_info) + "\n\n"
                                                              f"Backup location: {BACKUP_DIR}/workflows/"
        )

        self._show_message("info", "Manual Recovery Required", message)
        log_to_file("recover", f"Workflow recovery requested but not supported: {', '.join(names)}")

        return []

    def recover_deleted_workflow_schemes(self, scheme_ids: List[str]) -> List[str]:
        """Recover workflow schemes from backups by recreating them."""
        recovered: List[str] = []
        if not scheme_ids:
            return recovered

        backup = self._load_full_backup("Workflow Schemes")
        items = backup.get("items", {}) if isinstance(backup, dict) else {}

        for sid in scheme_ids:
            bundle = items.get(sid) or {}
            scheme_data = bundle.get("scheme") or {}

            # Extract scheme details
            name = scheme_data.get("name")
            description = scheme_data.get("description") or ""

            if not name:
                log_to_file("recover", f"Missing name for backup workflow scheme {sid}; skipping.")
                continue

            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recreate workflow scheme '{name}' from backup.")
                recovered.append(name)
                continue

            # Recreate the workflow scheme
            create_url = f"{self.current_domain}/rest/api/3/workflowscheme"
            create_body = {
                "name": name,
                "description": description
            }

            try:
                r = requests.post(create_url, auth=self.jira_auth(), json=create_body, timeout=TIMEOUT)
                if r.status_code not in (200, 201):
                    log_to_file("recover", f"Create workflow scheme '{name}' failed: {r.status_code} {r.text}")
                    continue
                new_scheme = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                new_id = (new_scheme or {}).get("id")

                if new_id:
                    recovered.append(name)
                    log_to_file("recover", f"Recreated workflow scheme '{name}' (old {sid} -> new {new_id})")
                else:
                    log_to_file("recover", f"Failed to get new ID for recreated workflow scheme '{name}'")
            except Exception as e:
                log_to_file("recover", f"Create workflow scheme '{name}' exception: {e}")

        return recovered

    def recover_deleted_screens(self, screen_ids: List[str]) -> List[str]:
        """Recover screens from backups by recreating them."""
        recovered: List[str] = []
        if not screen_ids:
            return recovered

        backup = self._load_full_backup("Screens")
        items = backup.get("items", {}) if isinstance(backup, dict) else {}

        for sid in screen_ids:
            bundle = items.get(sid) or {}
            screen_data = bundle.get("screen") or {}
            tabs_data = bundle.get("tabs") or []
            tab_fields = bundle.get("tabFields") or {}

            # Extract screen details
            name = screen_data.get("name")
            description = screen_data.get("description") or ""

            if not name:
                log_to_file("recover", f"Missing name for backup screen {sid}; skipping.")
                continue

            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recreate screen '{name}' from backup.")
                recovered.append(name)
                continue

            # Recreate the screen
            create_url = f"{self.current_domain}/rest/api/3/screens"
            create_body = {
                "name": name,
                "description": description
            }

            try:
                r = requests.post(create_url, auth=self.jira_auth(), json=create_body, timeout=TIMEOUT)
                if r.status_code not in (200, 201):
                    log_to_file("recover", f"Create screen '{name}' failed: {r.status_code} {r.text}")
                    continue
                new_screen = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                new_id = (new_screen or {}).get("id")

                if new_id:
                    recovered.append(name)
                    log_to_file("recover", f"Recreated screen '{name}' (old {sid} -> new {new_id})")
                else:
                    log_to_file("recover", f"Failed to get new ID for recreated screen '{name}'")
            except Exception as e:
                log_to_file("recover", f"Create screen '{name}' exception: {e}")

        return recovered

    def recover_deleted_screen_schemes(self, scheme_ids: List[str]) -> List[str]:
        """Recover screen schemes from backups by recreating them."""
        recovered: List[str] = []
        if not scheme_ids:
            return recovered

        backup = self._load_full_backup("Screen Schemes")
        items = backup.get("items", {}) if isinstance(backup, dict) else {}

        for sid in scheme_ids:
            bundle = items.get(sid) or {}
            scheme_data = bundle.get("screenScheme") or {}

            # Extract scheme details
            name = scheme_data.get("name")
            description = scheme_data.get("description") or ""

            if not name:
                log_to_file("recover", f"Missing name for backup screen scheme {sid}; skipping.")
                continue

            if self.dry_run.get():
                log_to_file("recover", f"[DRY RUN] Would recreate screen scheme '{name}' from backup.")
                recovered.append(name)
                continue

            # Recreate the screen scheme
            create_url = f"{self.current_domain}/rest/api/3/screenscheme"
            create_body = {
                "name": name,
                "description": description
            }

            try:
                r = requests.post(create_url, auth=self.jira_auth(), json=create_body, timeout=TIMEOUT)
                if r.status_code not in (200, 201):
                    log_to_file("recover", f"Create screen scheme '{name}' failed: {r.status_code} {r.text}")
                    continue
                new_scheme = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                new_id = (new_scheme or {}).get("id")

                if new_id:
                    recovered.append(name)
                    log_to_file("recover", f"Recreated screen scheme '{name}' (old {sid} -> new {new_id})")
                else:
                    log_to_file("recover", f"Failed to get new ID for recreated screen scheme '{name}'")
            except Exception as e:
                log_to_file("recover", f"Create screen scheme '{name}' exception: {e}")

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
        """Recreate filters from backup."""
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
        """Workflow status recovery is not supported in Jira Cloud."""
        log_to_file("recover", f"Status recovery requested but not supported: {', '.join(status_ids)}")
        return []


if __name__ == "__main__":
    app = JiraToolGUI()
    app.mainloop()