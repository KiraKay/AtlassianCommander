
# tests/test_cleanup.py

import os
import json
import pytest
from unittest.mock import patch, mock_open

# Assume these imports are from your codebase
from main_file import delete_filter, recover_deleted_filters, BACKUP_DIR

@pytest.fixture
def dummy_filter_id():
    return "12345"

@pytest.fixture
def backup_path():
    return os.path.join(BACKUP_DIR, "deleted_filters.json")

def test_delete_filter_dry_run(dummy_filter_id):
    with patch("main_file.log_to_file") as mock_log:
        result = delete_filter(dummy_filter_id, dry_run=True)
        assert result is True
        mock_log.assert_called_with("cleanup", f"[DRY RUN] Would delete filter: {dummy_filter_id}")

@patch("builtins.open", new_callable=mock_open, read_data='["12345", "67890"]')
@patch("main_file.os.path.exists", return_value=True)
@patch("main_file.json.dump")
def test_recover_deleted_filters_from_backup(mock_json_dump, mock_exists, mock_file, dummy_filter_id):
    with patch("main_file.log_to_file") as mock_log:
        recovered = recover_deleted_filters([dummy_filter_id])
        assert dummy_filter_id in recovered
        handle = mock_file()
        mock_json_dump.assert_called_once()
        mock_log.assert_called_with("recover_filters", f"Marked filter {dummy_filter_id} as recovered locally.")
