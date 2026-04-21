"""
services/webhook/main.py — Real-time Kobo webhook receiver.

FastAPI service that:
  - Receives HTTP POST from Kobo REST Hook on every new submission
  - Validates the X-Kobo-Webhook-Secret header
  - Filters test submissions to quarantine
  - Runs the single-submission pipeline
  - Sends new entry notification email
  - Returns 200 immediately so Kobo does not retry

Deployed as Cloud Run Service with min-instances=1 (always warm).
"""
import logging
import os
import sys
import uuid
import hmac

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from fastapi import FastAPI, Request, HTTPException, status
from google.cloud import bigquery
import gspread
import google.auth
from google.oauth2 import service_account

from config         import load_config
from transformer    import transform_submissions, is_test_submission
from schema_manager import (infer_bq_schema, get_existing_schema,
                             evolve_schema, ensure_schema_changelog)
from loader         import (ensure_all_system_tables, get_processed_ids,
                             record_processed_ids, quarantine_rows,
                             streaming_insert, log_pipeline_run,
                             get_pipeline_state, save_pipeline_state)
from sheets_writer  import (get_or_create_spreadsheet, write_to_sheet,
                             share_and_notify_first_run, notify_new_entries)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Kobo Webhook Receiver")

# Load config once at startup
cfg = load_config()

# BQ and Sheets clients — initialised once at startup
bq_client = bigquery.Client(project=cfg.bq_project)

# Use default credentials from Cloud Run service account
credentials, _ = google.auth.default(
    scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
)
gc = gspread.Client(auth=credentials)


def validate_webhook_secret(request_secret, expected_secret):
    """
    Validate the X-Kobo-Webhook-Secret header.
    Uses hmac.compare_digest to prevent timing attacks.
    Returns True if valid, False otherwise.
    """
    if not request_secret or not expected_secret:
        return not expected_secret   # If no secret configured, allow all
    return hmac.compare_digest(
        str(request_secret).encode(),
        str(expected_secret).encode(),
    )


@app.get("/health")
def health():
    """Health check endpoint for Cloud Run."""
    return {"status": "ok"}


@app.post("/webhook")
async def receive_webhook(request: Request):
    """
    Main webhook endpoint — receives Kobo REST Hook POST.
    Validates secret, processes submission, returns 200 immediately.
    """
    # Validate secret header
    incoming_secret = request.headers.get("X-Kobo-Webhook-Secret", "")
    if not validate_webhook_secret(incoming_secret, cfg.kobo_webhook_secret):
        logger.warning("Webhook rejected — invalid secret")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook secret",
        )

    # Parse payload
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid JSON payload",
        )

    run_id = str(uuid.uuid4())[:8]
    sub_id = str(payload.get("_id", "unknown"))
    logger.info(
        f"Webhook received | submission_id={sub_id} | run_id={run_id}"
    )

    # Process asynchronously — return 200 immediately to Kobo
    # so it does not retry thinking the request failed
    try:
        await _process_submission(payload, run_id)
    except Exception as e:
        logger.exception(f"Webhook processing failed: {e}")
        # Still return 200 — the nightly sync will pick this up
        # Returning 500 would cause Kobo to retry unnecessarily

    return {"status": "received", "run_id": run_id}


async def _process_submission(payload, run_id):
    """Process a single submission from a webhook payload."""
    ensure_all_system_tables(bq_client, cfg.bq_project, cfg.bq_dataset)

    # ── Check for duplicate (EC2) ─────────────────────────────────────────────
    sub_id       = str(payload.get("_id", "unknown"))
    existing_ids = get_processed_ids(
        bq_client, cfg.bq_project, cfg.bq_dataset, cfg.form_uid
    )
    if sub_id in existing_ids:
        logger.info(
            f"Duplicate submission {sub_id} — skipping (already processed)"
        )
        return

    # ── Check for test submission ─────────────────────────────────────────────
    is_test, kw = is_test_submission(payload, cfg.test_keywords)
    if is_test:
        quarantine_rows(
            bq_client, cfg.bq_project, cfg.bq_dataset,
            cfg.bq_table_quarantine,
            [{"row": payload,
              "reason": f"test_submission_detected — keyword: \"{kw}\""}],
            run_id=run_id, source="webhook",
        )
        logger.info(
            f"Test submission {sub_id} quarantined (keyword: {kw})"
        )
        return

    # ── Transform ─────────────────────────────────────────────────────────────
    clean_df, _, _ = transform_submissions(
        [payload],
        form_uid=cfg.form_uid,
        pipeline_run_id=run_id,
        test_keywords=[],   # Already checked above
    )

    if clean_df.empty:
        return

    # ── Schema evolution ──────────────────────────────────────────────────────
    prod_ref = f"{cfg.bq_project}.{cfg.bq_dataset}.{cfg.bq_table}"
    existing_schema = get_existing_schema(bq_client, prod_ref)
    if existing_schema:
        evolve_schema(
            bq_client, prod_ref, clean_df, existing_schema,
            form_uid=cfg.form_uid, run_id=run_id,
            project=cfg.bq_project, dataset=cfg.bq_dataset,
        )

    # ── Streaming insert to production ────────────────────────────────────────
    streaming_insert(bq_client, clean_df, prod_ref)
    record_processed_ids(
        bq_client, cfg.bq_project, cfg.bq_dataset,
        [sub_id], cfg.form_uid, run_id,
    )

    # ── Update Google Sheet ───────────────────────────────────────────────────
    state = get_pipeline_state(
        bq_client, cfg.bq_project, cfg.bq_dataset, cfg.form_uid
    )
    is_first_run = state is None
    spreadsheet, _ = get_or_create_spreadsheet(
        gc,
        cfg.sheet_id or (state.get("sheet_id") if state else ""),
        cfg.sheet_name,
        folder_id=cfg.shared_drive_folder_id,
        delegated_email=cfg.delegated_email,
    )

    # For webhook, append only the new row (efficient for real-time updates)
    write_to_sheet(
        spreadsheet, cfg.sheet_tab,
        clean_df, max_rows=cfg.max_sheet_rows
    )

    # ── Notifications ─────────────────────────────────────────────────────────
    if is_first_run:
        share_and_notify_first_run(
            spreadsheet, cfg.team_emails, new_rows_df=clean_df
        )
        save_pipeline_state(
            bq_client, cfg.bq_project, cfg.bq_dataset,
            cfg.form_uid,
            sheet_id=spreadsheet.id,
            sheet_url=(
                f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
            ),
            emails_sent=bool(cfg.team_emails),
        )
    else:
        # Send new entry notification on every subsequent submission
        notify_new_entries(
            spreadsheet, cfg.new_entry_notify_emails, clean_df
        )

    log_pipeline_run(
        bq_client, cfg.bq_project, cfg.bq_dataset,
        run_id, cfg.form_uid, "ok",
        rows_fetched=1, rows_loaded=1, source="webhook",
    )
    logger.info(f"Webhook processing complete | submission_id={sub_id}")
