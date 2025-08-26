# path: main_updated.py
# Jira Cleanup & Recovery Tool – status bar (thread-safe), checkboxes, Excel export

import glob
import json
import os
import platform
import subprocess
import threading
from datetime import datetime
from typing import Any, Dict, List, Callable, Optional, Tuple
from queue import Queue, Empty  # status pump

import requests
import tkinter as tk
from requests.auth import HTTPBasicAuth
from tkinter import ttk, messagebox

# optional Excel support
try:
    from openpyxl import Workbook  # preferred
except Exception:
    Workbook = None

BACKUP_DIR = "backup/backups"
EXPORT_DIR = "backup/exports"
LOG_DIR = "logs"
PROFILES_FILE = "auth_profiles.json"
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
    with open(PROFILES_FILE, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2)


def log_to_file(job_name: str, message: str) -> None:
    log_file = os.path.join(LOG_DIR, f"{job_name}_{datetime.now().strftime('%Y%m%d')}.log")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")


def backup_to_file(data: Any, name: str) -> str:
    filename = os.path.join(BACKUP_DIR, f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    return filename


def load_backup_items(name_prefix: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    try:
        for filename in os.listdir(BACKUP_DIR):
            if filename.startswith(name_prefix) and filename.endswith(".json"):
                with open(os.path.join(BACKUP_DIR, filename), "r", encoding="utf-8") as f:
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
    """Collects full objects & associations prior to deletion.
    Why: enables precise recovery and auditing beyond just IDs.
    """

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
        assoc = self._get_json(f"/rest/api/2/workflowscheme/project?workflowSchemeId={scheme_id}") or {}
        return {"scheme": scheme, "projectAssociations": assoc}

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

    def _collect_filter(self, filter_id: str) -> Dict[str, Any]:
        filt = self._get_json(f"/rest/api/3/filter/{filter_id}") or {}
        shares = self._get_json(f"/rest/api/2/filter/{filter_id}/permission") or {}
        return {"filter": filt, "sharePermissions": shares}

    def backup_items(self, item_type: str, ids: List[str]) -> str:
        collectors: Dict[str, Callable[[str], Dict[str, Any]]] = {
            "Custom Fields": self._collect_custom_field,
            "Workflows": self._collect_workflow,
            "Workflow Schemes": self._collect_workflow_scheme,
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
        fname = backup_to_file(payload, f"full_backup_{item_type.replace(' ', '_').lower()}")
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

        self.dry_run = tk.BooleanVar(value=True)
        self.current_domain = ""
        self.current_email = ""   # fixed stray indent previously
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

        self.recover_trees: Dict[str, ttk.Treeview] = {}
        self.recover_checked: Dict[str, set[str]] = {}
        self.recover_sort_orders: Dict[str, tk.BooleanVar] = {}

        # --- Status bar (thread-safe) ---
        self.status_queue: Queue[str] = Queue()
        self.status_var = tk.StringVar(value="Ready.")
        status_frame = ttk.Frame(self)
        status_frame.pack(fill="x", side="bottom")
        ttk.Separator(status_frame, orient="horizontal").pack(fill="x")
        self.status_label = ttk.Label(status_frame, textvariable=self.status_var, anchor="w")
        self.status_label.pack(fill="x", padx=8, pady=4)
        self.after(100, self._pump_status_queue)

        self._build_cleanup_tab()
        self._build_recover_tab()
        self._build_logs_tab()
        self._build_auth_tab()

    # ---------- threading helper ----------
    def _thread(self, fn: Callable, *args) -> None:
        """Run a target in a daemon thread; emit start/finish status."""
        def runner():
            try:
                self._emit_status(f"Started: {getattr(fn, '__name__', 'task')}")
                fn(*args)
                self._emit_status(f"Done: {getattr(fn, '__name__', 'task')}")
            except Exception as e:
                self._emit_status(f"Error: {e}")
                log_to_file("runtime", f"Threaded task error: {e}")
        threading.Thread(target=runner, daemon=True).start()

    # ---------- Status helpers ----------
    def _emit_status(self, text: str) -> None:
        """Thread-safe: enqueue status text."""
        ts = datetime.now().strftime("%H:%M:%S")
        self.status_queue.put(f"[{ts}] {text}")

    def _pump_status_queue(self) -> None:
        """Main thread: drain queue and update label."""
        try:
            while True:
                msg = self.status_queue.get_nowait()
                self.status_var.set(msg)
            # loop drains all pending messages
        except Empty:
            pass
        finally:
            self.after(150, self._pump_status_queue)

    # ---------- Backup helpers ----------
    def _latest_full_backup_path(self, item_type: str) -> Optional[str]:
        pat = os.path.join(BACKUP_DIR, f"full_backup_{item_type.replace(' ', '_').lower()}_*.json")
        files = sorted(glob.glob(pat))
        return files[-1] if files else None

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

    # ---------- HTTP ----------
    def jira_auth(self) -> HTTPBasicAuth:
        return HTTPBasicAuth(self.current_email, self.current_token)

    def _get(self, url: str, item_type: str) -> Any:
        try:
            self._emit_status(f"Fetching {item_type} …")
            resp = requests.get(url, auth=self.jira_auth(), timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
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

    def _post(self, url: str) -> bool:
        try:
            resp = requests.post(url, auth=self.jira_auth(), timeout=TIMEOUT)
            return resp.status_code in (200, 201, 202, 204)
        except Exception as e:
            log_to_file("recover", f"Post error at {url}: {e}")
            return False

    def test_connection(self) -> bool:
        try:
            url = f"{self.current_domain}/rest/api/3/myself"
            self._emit_status("Testing connection …")
            resp = requests.get(url, auth=self.jira_auth(), timeout=TIMEOUT)
            resp.raise_for_status()
            self._emit_status("Connection OK")
            return True
        except Exception as e:
            self._emit_status("Connection FAILED")
            messagebox.showerror("Connection Failed", str(e))
            return False

    # ---------- Fetchers (unused lists) ----------
    def get_unused_custom_fields(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/field"
        fields = self._get(url, "custom fields")
        return [f for f in fields if isinstance(f, dict) and f.get("custom") and not f.get("screens")]

    def get_unused_workflows(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/workflow"
        data = self._get(url, "workflows")
        return data.get("values", []) if isinstance(data, dict) else data

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
        data = self._get(url, "dashboards")
        return data.get("dashboards", []) if isinstance(data, dict) else data

    def get_unused_filters(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/filter/favourite"
        data = self._get(url, "filters")
        if isinstance(data, list):
            return data
        return data.get("values", [])

    # ---------- Deleters ----------
    def delete_custom_field(self, field_id: str) -> bool:
        if self.dry_run.get():
            log_to_file("cleanup", f"[DRY RUN] Would delete custom field: {field_id}")
            return True
        url = f"{self.current_domain}/rest/api/3/field/{field_id}"
        return self._delete(url)

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

    # ---------- Deleted fetchers (Recover lists) ----------
    def get_deleted_fields(self) -> List[Dict[str, Any]]:
        url = f"{self.current_domain}/rest/api/3/field/search?status=deleted"
        data = self._get(url, "deleted fields")
        return data.get("values", []) if isinstance(data, dict) else []

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

    # ---------- UI builders ----------
    def _build_checkbox_tree(self, parent: tk.Widget) -> ttk.Treeview:
        cols = ("check", "id", "name")
        tree = ttk.Treeview(parent, columns=cols, show="headings", selectmode="extended", height=18)
        tree.heading("check", text="✓")
        tree.heading("id", text="ID")
        tree.heading("name", text="Name / Description")
        tree.column("check", width=40, anchor="center")
        tree.column("id", width=220, anchor="w")
        tree.column("name", width=720, anchor="w")

        def on_click(event) -> None:
            row = tree.identify_row(event.y)
            if not row:
                return
            self._toggle_tree_item(tree, row)

        tree.bind("<Button-1>", on_click)
        tree.bind("<space>", lambda e: [self._toggle_tree_item(tree, iid) for iid in tree.selection()])
        return tree

    def _build_cleanup_tab(self) -> None:
        notebook = ttk.Notebook(self.cleanup_tab)
        notebook.pack(fill="both", expand=True)

        self.cleanup_types = {
            "Custom Fields": self.get_unused_custom_fields,
            "Workflows": self.get_unused_workflows,
            "Workflow Schemes": self.get_unused_workflow_schemes,
            "Screens": self.get_unused_screens,
            "Screen Schemes": self.get_unused_screen_schemes,
            "Dashboards": self.get_unused_dashboards,
            "Filters": self.get_unused_filters,
        }
        self.cleanup_delete_funcs = {
            "Custom Fields": self.delete_custom_field,
            "Workflows": self.delete_workflow,
            "Workflow Schemes": self.delete_workflow_scheme,
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

            self.cleanup_sort_orders[name] = tk.BooleanVar(value=True)
            ttk.Radiobutton(top, text="Ascending", variable=self.cleanup_sort_orders[name], value=True,
                            command=lambda n=name: self._resort_tree("cleanup", n)).pack(side="right", padx=2)
            ttk.Radiobutton(top, text="Descending", variable=self.cleanup_sort_orders[name], value=False,
                            command=lambda n=name: self._resort_tree("cleanup", n)).pack(side="right", padx=2)
            ttk.Label(top, text="Sort:").pack(side="right")

            tree = self._build_checkbox_tree(frame)
            tree.pack(fill="both", expand=True, padx=4, pady=4)
            self.cleanup_trees[name] = tree
            self.cleanup_checked[name] = set()

            actions = ttk.Frame(frame)
            actions.pack(fill="x", pady=4)
            ttk.Button(actions, text="Find Unused", command=lambda n=name: self._thread(self._load_unused_items, n)).pack(side="left", padx=2)
            ttk.Button(actions, text="Check All", command=lambda n=name: self._check_all("cleanup", n)).pack(side="left", padx=2)
            ttk.Button(actions, text="Uncheck All", command=lambda n=name: self._uncheck_all("cleanup", n)).pack(side="left", padx=2)
            ttk.Button(actions, text="Invert", command=lambda n=name: self._invert_all("cleanup", n)).pack(side="left", padx=2)

            ttk.Button(actions, text="Export Checked to Excel",
                       command=lambda n=name: self._export_tree("cleanup", n, only_checked=True)).pack(side="right", padx=2)
            ttk.Button(actions, text="Export All to Excel",
                       command=lambda n=name: self._export_tree("cleanup", n, only_checked=False)).pack(side="right", padx=2)

            ttk.Button(actions, text="Delete Checked", command=lambda n=name: self._thread(self._delete_unused_items, n)).pack(side="right", padx=12)

            del_flag = tk.BooleanVar(value=True)
            ttk.Checkbutton(actions, text="Enable Deletion", variable=del_flag).pack(side="right", padx=8)
            self.cleanup_delete_flags[name] = del_flag

    def _build_recover_tab(self) -> None:
        notebook = ttk.Notebook(self.recover_tab)
        notebook.pack(fill="both", expand=True)

        self.recover_types = {
            "Custom Fields": {"fetch": self.get_deleted_fields, "recover": self.recover_deleted_fields, "id_key": "id", "name_key": "name"},
            "Workflows": {"fetch": self.get_deleted_workflows, "recover": self.recover_deleted_workflows, "id_key": "name", "name_key": "name"},
            "Workflow Schemes": {"fetch": self.get_deleted_workflow_schemes, "recover": self.recover_deleted_workflow_schemes, "id_key": "id", "name_key": "name"},
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

            self.recover_sort_orders[name] = tk.BooleanVar(value=True)
            ttk.Radiobutton(top, text="Ascending", variable=self.recover_sort_orders[name], value=True,
                            command=lambda n=name: self._resort_tree("recover", n)).pack(side="right", padx=2)
            ttk.Radiobutton(top, text="Descending", variable=self.recover_sort_orders[name], value=False,
                            command=lambda n=name: self._resort_tree("recover", n)).pack(side="right", padx=2)
            ttk.Label(top, text="Sort:").pack(side="right")

            tree = self._build_checkbox_tree(frame)
            tree.pack(fill="both", expand=True, padx=4, pady=4)
            self.recover_trees[name] = tree
            self.recover_checked[name] = set()

            actions = ttk.Frame(frame)
            actions.pack(fill="x", pady=4)
            ttk.Button(actions, text="Refresh List", command=lambda n=name: self._thread(self._load_deleted_items, n)).pack(side="left", padx=2)
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

            ttk.Button(actions, text="Recover Checked", command=lambda n=name: self._thread(self._recover_selected, n)).pack(side="right", padx=12)

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


if __name__ == "__main__":
    app = JiraToolGUI()
    app.mainloop()