"""
services/sync/main.py — Nightly full reconciliation sync service.

This Cloud Run Job:
  1. Loops through ALL registered forms in FORM_REGISTRY
  2. For each form:
     a. Fetches ALL submissions from Kobo API
     b. Filters test submissions to quarantine
     c. Compares against processed_ids for deduplication
     d. Runs staging → validation → production pipeline
     e. Updates the form's Google Sheet
     f. Sends new entry notification for any new rows
     g. Logs to pipeline_runs audit table

Triggered by Cloud Scheduler (nightly).
Acts as the safety net for any submissions missed by the webhook.
Supports multiple KoboToolbox forms via FORM_REGISTRY.
"""
import logging
import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from google.cloud import bigquery
import gspread
import google.auth

from config          import load_config
from form_registry   import get_all_forms
from fetcher         import fetch_submissions, KoboAuthError
from transformer     import transform_submissions
from schema_manager  import (infer_bq_schema, get_existing_schema,
                              evolve_schema, ensure_schema_changelog)
from loader          import (ensure_all_system_tables, get_processed_ids,
                              record_processed_ids, quarantine_rows,
                              batch_load, log_pipeline_run,
                              get_pipeline_state, save_pipeline_state)
from sheets_writer   import (get_or_create_spreadsheet, write_to_sheet,
                              share_and_notify_first_run, notify_new_entries)
from alerting        import alert_on_failure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_sync():
    """Main sync pipeline — loops through all registered forms."""
    cfg    = load_config()
    run_id = str(uuid.uuid4())[:8]

    logger.info(f"=== Sync job started | run_id={run_id} ===")

    # ── Clients ───────────────────────────────────────────────────────────────
    bq_client = bigquery.Client(project=cfg.bq_project)
    credentials, _ = google.auth.default(scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ])
    gc = gspread.Client(auth=credentials)

    # ── System tables ─────────────────────────────────────────────────────────
    ensure_all_system_tables(bq_client, cfg.bq_project, cfg.bq_dataset)
    ensure_schema_changelog(bq_client, cfg.bq_project, cfg.bq_dataset)

    # ── Get all registered forms ──────────────────────────────────────────────
    forms = get_all_forms()
    if not forms:
        logger.warning("No forms registered in FORM_REGISTRY — nothing to sync")
        return

    logger.info(f"Found {len(forms)} registered form(s): "
                f"{[f[1]['name'] for f in forms]}")

    # ── Process each form independently ───────────────────────────────────────
    for form_uid, form_config in forms:
        _sync_form(
            bq_client, gc, cfg, run_id,
            form_uid, form_config
        )

    logger.info(f"=== Sync job complete | run_id={run_id} ===")


def _sync_form(bq_client, gc, cfg, run_id, form_uid, form_config):
    """Sync a single form — fetch, transform, load, update sheet."""
    bq_table   = form_config["bq_table"]
    sheet_id   = form_config["sheet_id"]
    sheet_tab  = form_config["sheet_tab"]
    form_name  = form_config["name"]

    logger.info(
        f"--- Syncing form: {form_name} | "
        f"form_uid={form_uid} | table={bq_table} ---"
    )

    rows_fetched       = 0
    rows_test_filtered = 0
    rows_loaded        = 0
    rows_quarantined   = 0

    try:
        # ── Step 1: Fetch ─────────────────────────────────────────────────────
        logger.info(f"[{form_name}] Step 1/7 — Fetching submissions from Kobo...")
        try:
            submissions = fetch_submissions(
                form_uid, cfg.kobo_token, cfg.kobo_base_url
            )
        except KoboAuthError as e:
            logger.critical(f"[{form_name}] Auth error: {e}")
            log_pipeline_run(
                bq_client, cfg.bq_project, cfg.bq_dataset,
                run_id, form_uid, "failed",
                error=str(e), source="sync"
            )
            return

        rows_fetched = len(submissions)
        logger.info(f"[{form_name}] Fetched {rows_fetched} total submissions")

        if not submissions:
            logger.info(f"[{form_name}] No submissions — skipping")
            log_pipeline_run(
                bq_client, cfg.bq_project, cfg.bq_dataset,
                run_id, form_uid, "ok_empty",
                rows_fetched=0, source="sync"
            )
            return

        # ── Step 2: Transform + filter (dynamic schema, any field set) ────────
        logger.info(f"[{form_name}] Step 2/7 — Transforming and filtering...")
        clean_df, child_tables, test_rows = transform_submissions(
            submissions,
            form_uid=form_uid,
            pipeline_run_id=run_id,
            test_keywords=cfg.test_keywords,
        )
        rows_test_filtered = len(test_rows)

        if test_rows:
            quarantine_rows(
                bq_client, cfg.bq_project, cfg.bq_dataset,
                f"{bq_table}_quarantine", test_rows,
                run_id=run_id, source="sync"
            )
            rows_quarantined += len(test_rows)

        if clean_df.empty:
            logger.info(f"[{form_name}] All submissions were test entries")
            log_pipeline_run(
                bq_client, cfg.bq_project, cfg.bq_dataset,
                run_id, form_uid, "ok_all_filtered",
                rows_fetched=rows_fetched,
                rows_test_filtered=rows_test_filtered,
                rows_quarantined=rows_quarantined,
                source="sync"
            )
            return

        # ── Step 3: Deduplication ─────────────────────────────────────────────
        logger.info(f"[{form_name}] Step 3/7 — Checking for new submissions...")
        existing_ids = get_processed_ids(
            bq_client, cfg.bq_project, cfg.bq_dataset, form_uid
        )
        id_col = next(
            (c for c in clean_df.columns if c in ("_id", "id")), None
        )
        if id_col:
            new_df = clean_df[
                ~clean_df[id_col].astype(str).isin(existing_ids)
            ].reset_index(drop=True)
            dupes = len(clean_df) - len(new_df)
            logger.info(
                f"[{form_name}] New: {len(new_df)} rows | "
                f"Duplicates skipped: {dupes}"
            )
        else:
            new_df = clean_df

        # ── Step 4: Stage (schema inferred dynamically from this form's data) ─
        logger.info(f"[{form_name}] Step 4/7 — Loading to staging table...")
        staging_ref = f"{cfg.bq_project}.{cfg.bq_dataset}.{bq_table}_staging"
        schema      = infer_bq_schema(new_df)
        batch_load(bq_client, new_df, staging_ref, schema, mode="truncate")

        # ── Step 5: Schema evolution + promote to production ──────────────────
        logger.info(f"[{form_name}] Step 5/7 — Promoting to production...")
        prod_ref        = f"{cfg.bq_project}.{cfg.bq_dataset}.{bq_table}"
        existing_schema = get_existing_schema(bq_client, prod_ref)
        if existing_schema:
            evolve_schema(
                bq_client, prod_ref, new_df, existing_schema,
                form_uid=form_uid, run_id=run_id,
                project=cfg.bq_project, dataset=cfg.bq_dataset,
            )
        prod_schema = infer_bq_schema(new_df)
        rows_loaded = batch_load(
            bq_client, new_df, prod_ref, prod_schema,
            mode=cfg.sync_mode
        )

        if id_col and not new_df.empty:
            record_processed_ids(
                bq_client, cfg.bq_project, cfg.bq_dataset,
                new_df[id_col].astype(str).tolist(),
                form_uid, run_id,
            )

        # ── Step 6: Google Sheet (must already exist — created manually) ──────
        logger.info(f"[{form_name}] Step 6/7 — Updating Google Sheet...")
        state        = get_pipeline_state(bq_client, cfg.bq_project, cfg.bq_dataset, form_uid)
        is_first_run = state is None

        spreadsheet, _ = get_or_create_spreadsheet(
            gc,
            sheet_id or (state.get("sheet_id") if state else ""),
            form_config["sheet_name"],
        )

        # Fetch ALL data from BigQuery and overwrite sheet
        logger.info(f"[{form_name}] Fetching all data from BigQuery for sheet refresh...")
        all_df = bq_client.query(
            f"SELECT * FROM `{prod_ref}` ORDER BY pipeline_loaded_at ASC"
        ).to_dataframe(create_bqstorage_client=False)
        logger.info(f"[{form_name}] Writing {len(all_df)} total rows to sheet")
        write_to_sheet(
            spreadsheet, sheet_tab,
            all_df, max_rows=cfg.max_sheet_rows,
            mode="overwrite"
        )

        # ── Step 7: Notifications ─────────────────────────────────────────────
        logger.info(f"[{form_name}] Step 7/7 — Sending notifications...")
        if is_first_run:
            share_and_notify_first_run(
                spreadsheet, cfg.team_emails,
                new_rows_df=new_df if not new_df.empty else None
            )
            save_pipeline_state(
                bq_client, cfg.bq_project, cfg.bq_dataset,
                form_uid,
                sheet_id=spreadsheet.id,
                sheet_url=f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}",
                emails_sent=bool(cfg.team_emails),
            )
        elif not new_df.empty:
            notify_new_entries(spreadsheet, cfg.new_entry_notify_emails, new_df)

        # ── Audit log ─────────────────────────────────────────────────────────
        log_pipeline_run(
            bq_client, cfg.bq_project, cfg.bq_dataset,
            run_id, form_uid, "ok",
            rows_fetched=rows_fetched,
            rows_loaded=rows_loaded,
            rows_quarantined=rows_quarantined,
            rows_test_filtered=rows_test_filtered,
            source="sync",
        )
        logger.info(
            f"[{form_name}] Sync complete | "
            f"loaded={rows_loaded} | quarantined={rows_quarantined} | "
            f"test_filtered={rows_test_filtered}"
        )

    except Exception as e:
        logger.exception(f"[{form_name}] Sync failed: {e}")
        log_pipeline_run(
            bq_client, cfg.bq_project, cfg.bq_dataset,
            run_id, form_uid, "failed",
            rows_fetched=rows_fetched,
            error=str(e), source="sync",
        )


if __name__ == "__main__":
    run_sync()
