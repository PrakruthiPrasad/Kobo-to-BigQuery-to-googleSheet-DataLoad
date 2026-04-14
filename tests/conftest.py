"""
conftest.py — Shared fixtures for all test files.
All fixtures use mocks — no live API calls, no live GCP connections.
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


# ── Sample Kobo submissions ───────────────────────────────────────────────────

@pytest.fixture
def sample_submissions():
    """Clean real submissions as returned by Kobo API."""
    return [
        {
            "_id":               1,
            "Full Name":         "Alice Kamau",
            "Email Address":     "alice@example.org",
            "Message":           "Enquiry about the programme",
            "_submission_time":  "2025-01-15T10:00:00",
            "__version__":       "v1",
        },
        {
            "_id":               2,
            "Full Name":         "Bob Odhiambo",
            "Email Address":     "bob@example.org",
            "Message":           "Request for more information",
            "_submission_time":  "2025-01-15T11:30:00",
            "__version__":       "v1",
        },
    ]


@pytest.fixture
def test_submissions():
    """Submissions containing test keywords — should be quarantined."""
    return [
        {
            "_id":              99,
            "Full Name":        "test",
            "Email Address":    "test@example.com",
            "Message":          "testing the form",
            "_submission_time": "2025-01-15T09:00:00",
        },
        {
            "_id":              98,
            "Full Name":        "Demo User",
            "Email Address":    "demo@example.com",
            "Message":          "demo submission",
            "_submission_time": "2025-01-15T09:15:00",
        },
    ]


@pytest.fixture
def clean_df():
    """Pre-transformed clean DataFrame."""
    return pd.DataFrame({
        "_id":              ["1", "2"],
        "full_name":        ["Alice Kamau", "Bob Odhiambo"],
        "email_address":    ["alice@example.org", "bob@example.org"],
        "message":          ["Enquiry about the programme",
                             "Request for more information"],
        "submission_time":  ["2025-01-15T10:00:00", "2025-01-15T11:30:00"],
        "kobo_form_version":["v1", "v1"],
        "pipeline_loaded_at":["2025-01-15T10:00:00", "2025-01-15T10:00:00"],
        "pipeline_run_id":  ["abc123", "abc123"],
        "pipeline_form_uid":["test_form", "test_form"],
    })


@pytest.fixture
def mock_bq_client():
    """Mocked BigQuery client — never hits real GCP."""
    client = MagicMock()
    client.project = "test-project"
    # get_table raises Exception by default (table doesn't exist)
    client.get_table.side_effect = Exception("Table not found")
    return client


@pytest.fixture
def mock_spreadsheet():
    """Mocked gspread spreadsheet."""
    sheet = MagicMock()
    sheet.id    = "test-sheet-id-123"
    sheet.title = "Test Kobo Data"
    ws          = MagicMock()
    ws.title    = "Sheet1"
    ws.get_all_values.return_value = [["col1", "col2"], ["val1", "val2"]]
    sheet.worksheets.return_value  = [ws]
    sheet.worksheet.return_value   = ws
    return sheet


@pytest.fixture
def mock_gc(mock_spreadsheet):
    """Mocked gspread client."""
    gc = MagicMock()
    gc.open_by_key.return_value = mock_spreadsheet
    gc.create.return_value      = mock_spreadsheet
    return gc
