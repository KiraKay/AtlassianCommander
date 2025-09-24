# tests/test_cleanup.py

from unittest.mock import patch, mock_open
from main_updated import delete_filter, recover_deleted_filters

def test_delete_filter_dry_run(dummy_filter_id):
    with patch("main_updated.log_to_file") as mock_log:
        result = delete_filter(filter_id=dummy_filter_id)  # Removed unexpected `dry_run` argument
        assert result is True
        mock_log.assert_called_with("cleanup", f"[DRY RUN] Would delete filter: {dummy_filter_id}")

@patch("builtins.open", new_callable=mock_open, read_data='["12345", "67890"]')
@patch("main_updated.os.path.exists", return_value=True)
@patch("main_updated.json.dump")
def test_recover_deleted_filters_from_backup(mock_json_dump, mock_exists, mock_file, dummy_filter_id):
    with patch("main_updated.log_to_file") as mock_log:
        recovered = recover_deleted_filters(field_ids=[dummy_filter_id])  # Corrected parameter name
        assert dummy_filter_id in recovered
        handle = mock_file()
        mock_json_dump.assert_called_once()
        mock_log.assert_called_with("recover_filters", f"Marked filter {dummy_filter_id} as recovered locally.")
