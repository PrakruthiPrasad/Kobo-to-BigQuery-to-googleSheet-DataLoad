"""
sheets_writer.py — Google Sheets write operations via gspread.

Responsibilities:
  - Open an existing Google Sheet by ID (sheet must be created manually
    and shared with the service account as Editor — creation is not
    automated, see get_or_create_spreadsheet docstring)
  - Rename default Sheet1 tab (prevents two-tab confusion)
  - Write data with row windowing (Edge Case 8 — 10M cell limit)
  - Append mode: writes by column name (not position) so optional
    fields left blank by the user do not shift other values
  - Overwrite mode: full refresh from BigQuery (used by nightly sync)
  - First-run setup: share sheet with team, send notification
  - New entry notification: email on every run that has new data
    Uses gspread share() — plain text preview of new entries included
"""
import logging
from datetime import datetime, timezone

import gspread
from gspread_dataframe import set_with_dataframe

logger = logging.getLogger(__name__)

# Pipeline metadata columns — excluded from email preview
_META_COLS = {
    "pipeline_loaded_at", "pipeline_run_id",
    "pipeline_form_uid", "kobo_form_version",
}


def get_or_create_spreadsheet(gc, sheet_id, sheet_name, folder_id=None):
    """
    Open an existing Google Sheet by ID.

    Sheet creation is NOT automated — the sheet must be created manually
    in Google Drive and shared with the service account as Editor before
    running the pipeline. This avoids needing Domain-Wide Delegation or
    a Drive storage quota for the service account.

    Returns (spreadsheet, is_new=False).
    Raises ValueError if sheet_id is missing or the sheet is not found /
    not shared with the service account.
    """
    if not sheet_id or sheet_id.lower() in ("none", ""):
        raise ValueError(
            "SHEET_ID is required. Please create a Google Sheet manually, "
            "share it with the service account as Editor, and set the "
            "Sheet ID in GitHub Secrets (e.g. SHEET_ID or "
            "SHEET_ID_<FORM_NAME> for multi-form setups)."
        )
    try:
        sheet = gc.open_by_key(sheet_id)
        logger.info(f"Opened existing sheet: {sheet.title}")
        return sheet, False
    except gspread.exceptions.SpreadsheetNotFound:
        raise ValueError(
            f"Sheet ID '{sheet_id}' not found or not shared with the "
            f"service account. Please check: "
            f"1) The Sheet ID in GitHub Secrets is correct. "
            f"2) The sheet is shared with the service account as Editor."
        )


def write_to_sheet(spreadsheet, tab_name, df, max_rows=10000, mode="append"):
    """
    Write a DataFrame to a Sheet tab with row windowing.

    Row windowing (Edge Case 8):
    Google Sheets has a 10M cell limit. For a 50-column form that is
    ~200K rows. This function only writes the most recent max_rows rows.
    BigQuery always retains the full history.

    Tab naming:
    When Google creates a new spreadsheet it adds a default 'Sheet1' tab.
    This function renames it to tab_name instead of creating a second tab,
    so the user always sees exactly one tab.

    mode="append":
    Writes new rows by column name (reindexed against the sheet's
    existing header) so that optional fields left blank by the user
    become empty cells in the correct column rather than shifting
    subsequent values into the wrong columns.

    mode="overwrite":
    Clears the tab and rewrites everything from scratch (used by the
    nightly sync, which always passes the full BigQuery table).
    """
    if df.empty:
        logger.warning("DataFrame is empty — skipping Sheet write")
        return None

    # Apply row window
    if len(df) > max_rows:
        logger.warning(
            f"Row window applied: showing last {max_rows} "
            f"of {len(df)} rows"
        )
        df = df.tail(max_rows).reset_index(drop=True)

    # Get or create the tab — rename Sheet1 if it exists
    existing_tabs = [ws.title for ws in spreadsheet.worksheets()]

    if tab_name in existing_tabs:
        ws = spreadsheet.worksheet(tab_name)
        if mode == "append":
            logger.info(f"Tab '{tab_name}' exists — appending rows")
        else:
            logger.info(f"Tab '{tab_name}' exists — overwriting")
    elif "Sheet1" in existing_tabs:
        ws = spreadsheet.worksheet("Sheet1")
        ws.update_title(tab_name)
        logger.info(f"Renamed 'Sheet1' to '{tab_name}'")
    else:
        ws = spreadsheet.add_worksheet(
            title=tab_name,
            rows=len(df) + 10,
            cols=len(df.columns) + 2,
        )
        logger.info(f"Created tab '{tab_name}'")

    # Convert any datetime columns to strings for JSON serialization
    # Google Sheets API cannot serialize Timestamp objects
    def stringify_timestamps(dataframe):
        df_copy = dataframe.copy()
        for col in df_copy.columns:
            if "datetime" in str(df_copy[col].dtype) or hasattr(df_copy[col].dtype, "tz"):
                df_copy[col] = df_copy[col].astype(str)
        return df_copy

    if mode == "append":
        existing_values = ws.get_all_values()
        if not existing_values:
            # Sheet is empty — write with headers
            set_with_dataframe(
                ws, stringify_timestamps(df),
                include_index=False, include_column_header=True
            )
            logger.info(
                f"Written {len(df)} rows × {len(df.columns)} cols "
                f"(with headers) to tab '{tab_name}'"
            )
        else:
            # Sheet has data — append rows using set_with_dataframe
            # This writes by column name not position so nulls stay aligned
            # append_rows() is positional and shifts values when nulls
            # are dropped from the DataFrame (e.g. optional fields left blank)
            header   = existing_values[0]
            next_row = len(existing_values) + 1

            # Reindex df to match sheet header order exactly
            # Missing columns become empty strings (not shifted)
            df_aligned = stringify_timestamps(df).reindex(columns=header).fillna("")
            set_with_dataframe(
                ws, df_aligned,
                row=next_row,
                include_index=False,
                include_column_header=False,
                resize=False,
            )
            logger.info(
                f"Appended {len(df)} rows to tab '{tab_name}'"
            )
    else:
        # Overwrite mode — clear and rewrite everything
        ws.clear()
        set_with_dataframe(
            ws, stringify_timestamps(df),
            include_index=False, include_column_header=True
        )
        logger.info(
            f"Written {len(df)} rows × {len(df.columns)} cols "
            f"to tab '{tab_name}'"
        )
    return ws


def move_to_shared_drive(drive_service, file_id, folder_id):
    """
    Move a Google Sheet into a Shared Drive folder.
    Requires Google Workspace and service account added to the Shared Drive.
    Skips gracefully if drive_service is None.

    Not used in the default manual-sheet workflow (sheets are placed in
    their target folder manually when created), but kept available for
    cases where the sheet needs to be relocated programmatically.
    """
    if not drive_service or not folder_id:
        return False
    try:
        drive_service.files().update(
            fileId=file_id,
            addParents=folder_id,
            removeParents="root",
            supportsAllDrives=True,
            fields="id, parents",
        ).execute()
        logger.info(f"Sheet moved to Shared Drive folder: {folder_id}")
        return True
    except Exception as e:
        logger.warning(f"Could not move to Shared Drive: {e}")
        return False


def _build_plain_text_preview(new_rows_df, max_entries=3):
    """
    Build a plain text preview of new entries for the email body.
    gspread share() only supports plain text — not HTML.
    Shows up to max_entries rows with field: value pairs.
    """
    if new_rows_df is None or new_rows_df.empty:
        return ""

    cols  = [c for c in new_rows_df.columns if c not in _META_COLS]
    count = len(new_rows_df)
    text  = f"\n\n── Preview of {min(count, max_entries)} "
    text += f"new submission{'s' if count > 1 else ''} ──\n"

    for i, (_, row) in enumerate(new_rows_df.head(max_entries).iterrows()):
        if count > 1:
            text += f"\nEntry {i + 1}:\n"
        for col in cols:
            val = row.get(col)
            if val and str(val).strip() not in ("None", "nan", ""):
                label = col.replace("_", " ").title()
                text += f"  {label}: {val}\n"

    if count > max_entries:
        text += f"\n  ...and {count - max_entries} more entries in the sheet.\n"

    return text


def share_and_notify_first_run(spreadsheet, team_emails, new_rows_df=None):
    """
    Share the sheet with team emails and send a one-time notification.
    This fires ONCE on first run only — the pipeline_state table
    tracks whether this has been done.

    The email includes:
    - A message that the sheet is ready and already contains data
    - A plain text preview of the latest submissions
    - A direct link to the sheet
    """
    if not team_emails:
        logger.info("No TEAM_EMAILS — skipping first-run share")
        return False

    sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
    preview   = _build_plain_text_preview(new_rows_df)

    for email in team_emails:
        spreadsheet.share(
            email,
            perm_type="user",
            role="writer",
            notify=True,
            email_message=(
                f'Your Kobo data report "{spreadsheet.title}" is ready.\n\n'
                f"The sheet already contains the latest data — "
                f"no need to wait or refresh."
                f"{preview}\n"
                f"Link: {sheet_url}"
            ),
        )
        logger.info(f"First-run share email sent to: {email}")

    return True


def notify_new_entries(spreadsheet, notify_emails, new_rows_df):
    """
    Send a new-entry notification email for EVERY sync run that finds
    new data — not just the first run.

    Uses gspread share() with notify=True and a custom message.
    The email includes a plain text preview of the new entries
    and a link to the sheet.

    Recipients can be any Google account (Gmail or Workspace) since
    gspread delegates sending to Google's own email system. Non-Google
    addresses (Outlook, Yahoo, custom domains on Exchange) may not
    reliably receive these notifications.
    """
    if not notify_emails:
        logger.info("No NEW_ENTRY_NOTIFY_EMAILS — skipping notification")
        return False
    if new_rows_df is None or new_rows_df.empty:
        logger.info("No new rows — skipping notification")
        return False

    count     = len(new_rows_df)
    sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
    preview   = _build_plain_text_preview(new_rows_df)
    timestamp = datetime.now(timezone.utc).strftime("%d %b %Y at %H:%M UTC")

    for email in notify_emails:
        spreadsheet.share(
            email,
            perm_type="user",
            role="reader",      # Read-only for notification recipients
            notify=True,
            email_message=(
                f"[Kobo] {count} new "
                f"{'entry' if count == 1 else 'entries'} received "
                f"— {timestamp}\n"
                f"{preview}\n"
                f"Open the sheet to see all data:\n{sheet_url}"
            ),
        )
        logger.info(
            f"New entry notification sent to: {email} "
            f"({count} new row(s))"
        )

    return True
