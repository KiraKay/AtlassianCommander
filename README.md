# Jira Cleanup & Recovery Tool

## 🚀 Overview
A full-featured GUI app for cleaning up unused Jira items and recovering deleted ones, with backups and logging.

---

## 🔐 Authentication
- Go to **Authentication** tab
- Enter profile name, email, token, and domain
- Save profile or test connection
- Supports multiple saved Jira profiles

---

## 🧹 Cleanup Tab
- Subtabs for: Filters, Custom Fields, Dashboards, Screens
- Click **Find Unused** to fetch items
- Use search bar to filter
- Select multiple and click **Delete Selected**
- Enable **Dry Run** to simulate actions

---

## ♻ Recovery Tab (Coming soon)
- Similar subtabs will show recently deleted items
- Recovery is based on local backup for now

---

## 🪵 Logs
- Logs of cleanup and recovery jobs
- Loaded by clicking **Load Logs**

---

## 🧠 Notes
- Jira API does not support undelete for all object types
- Backups are auto-saved per item type to JSON
- Logs are saved in `/logs` by job and date

---

Built with ❤️ using Python & Tkinter
