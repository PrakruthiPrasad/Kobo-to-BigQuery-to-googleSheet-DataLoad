"""
config.py — Load all configuration from environment variables.
All secrets come from GCP Secret Manager in production.
In local dev they are read from the .env file.
"""
import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    # Kobo
    kobo_token:          str
    form_uid:            str
    kobo_base_url:       str = "https://kf.kobotoolbox.org"
    kobo_webhook_secret: str = ""

    # BigQuery
    bq_project: str = ""
    bq_dataset:  str = "kobo_data"
    bq_table:    str = ""

    # Google Sheets
    sheet_id:               str = ""
    sheet_name:             str = "Kobo Pipeline Data"
    sheet_tab:              str = "Survey Data"
    shared_drive_folder_id: str = ""
    delegated_email:        str = ""
    team_emails:            List[str] = field(default_factory=list)

    # New entry notification
    new_entry_notify_emails: List[str] = field(default_factory=list)

    # Pipeline
    sync_mode:             str  = "append"
    max_sheet_rows:        int  = 10000
    test_keywords:         List[str] = field(
        default_factory=lambda: ["test", "testing", "demo", "sample", "dummy"]
    )
    alert_emails:          List[str] = field(default_factory=list)

    # Derived
    @property
    def bq_table_staging(self):    return f"{self.bq_table}_staging"

    @property
    def bq_table_quarantine(self): return f"{self.bq_table}_quarantine"


def _split_emails(raw):
    """Split a comma-separated email string into a list."""
    if not raw:
        return []
    return [e.strip() for e in raw.split(",") if e.strip()]


def load_config():
    """Load configuration from environment variables."""
    return Config(
        kobo_token            = os.environ["KOBO_TOKEN"],
        form_uid              = os.environ["FORM_UID"],
        kobo_base_url         = os.environ.get(
            "KOBO_BASE_URL", "https://kf.kobotoolbox.org"
        ),
        kobo_webhook_secret   = os.environ.get("KOBO_WEBHOOK_SECRET", ""),
        bq_project            = os.environ["BQ_PROJECT"],
        bq_dataset            = os.environ.get("BQ_DATASET", "kobo_data"),
        bq_table              = os.environ["BQ_TABLE"],
        sheet_id              = os.environ.get("SHEET_ID", ""),
        sheet_name            = os.environ.get("SHEET_NAME", "Kobo Pipeline Data"),
        sheet_tab             = os.environ.get("SHEET_TAB", "Survey Data"),
        shared_drive_folder_id= os.environ.get("SHARED_DRIVE_FOLDER_ID", ""),
        delegated_email       = os.environ.get("DELEGATED_EMAIL", ""),
        team_emails           = _split_emails(os.environ.get("TEAM_EMAILS", "")),
        new_entry_notify_emails = _split_emails(
            os.environ.get("NEW_ENTRY_NOTIFY_EMAILS", "")
        ),
        sync_mode             = os.environ.get("SYNC_MODE", "append"),
        max_sheet_rows        = int(os.environ.get("MAX_SHEET_ROWS", "10000")),
        test_keywords         = _split_emails(
            os.environ.get(
                "TEST_KEYWORDS",
                "test,testing,demo,sample,dummy"
            )
        ),
        alert_emails          = _split_emails(
            os.environ.get("ALERT_EMAILS", "")
        ),
    )
