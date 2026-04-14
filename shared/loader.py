"""
loader.py — BigQuery load operations.

Responsibilities:
  - Load DataFrames to BQ (staging, production, quarantine)
  - Streaming insert for single webhook-triggered rows (bypasses job limits)
  - Batch load for full sync jobs
  - Idempotency: check processed_ids before inserting (Edge Cases 1 & 2)
  - Quarantine: test submissions and validation failures go here, never dropped
  - Staging pattern: all data lands in staging first, then promoted
"""
import json
import logging
from datetime import datetime, timezone
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# ── Table schemas ─────────────────────────────────────────────────────────────

QUARANTINE_SCHEMA = [
    bigquery.SchemaField("quarantine_reason",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("quarantine_at",       "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("pipeline_run_id",     "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("source",              "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("raw_row_json",        "STRING",    mode="NULLABLE"),
]

PROCESSED_IDS_SCHEMA = [
    bigquery.SchemaField("submission_id",   "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("form_uid",        "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("processed_at",    "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("pipeline_run_id", "STRING",    mode="NULLABLE"),
]

PIPELINE_RUNS_SCHEMA = [
    bigquery.SchemaField("run_id",           "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("form_uid",         "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("started_at",       "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("completed_at",     "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("status",           "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("rows_fetched",     "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("rows_loaded",      "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("rows_quarantined", "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("rows_test_filtered","INTEGER",  mode="NULLABLE"),
    bigquery.SchemaField("error_message",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("source",           "STRING",    mode="NULLABLE"),
]

PIPELINE_STATE_SCHEMA = [
    bigquery.SchemaField("form_uid",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("sheet_id",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("sheet_url",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("emails_sent",    "BOOL",      mode="NULLABLE"),
    bigquery.SchemaField("first_run_at",   "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("last_synced_at", "TIMESTAMP", mode="NULLABLE"),
]


# ── Table creation helpers ────────────────────────────────────────────────────

def ensure_table(client, project, dataset, table_name, schema):
    """Create a BQ table if it does not already exist."""
    ref = f"{project}.{dataset}.{table_name}"
    try:
        client.get_table(ref)
    except Exception:
        client.create_table(bigquery.Table(ref, schema=schema))
        logger.info(f"Created table: {ref}")


def ensure_dataset(client, project, dataset, location="US"):
    """Create a BQ dataset if it does not already exist."""
    ref = f"{project}.{dataset}"
    try:
        client.get_dataset(ref)
    except Exception:
        ds          = bigquery.Dataset(ref)
        ds.location = location
        client.create_dataset(ds, timeout=30)
        logger.info(f"Created dataset: {ref}")


def ensure_all_system_tables(client, project, dataset):
    """Create all system tables needed by the pipeline."""
    ensure_dataset(client, project, dataset)
    ensure_table(client, project, dataset,
                 "processed_ids",    PROCESSED_IDS_SCHEMA)
    ensure_table(client, project, dataset,
                 "pipeline_runs",    PIPELINE_RUNS_SCHEMA)
    ensure_table(client, project, dataset,
                 "pipeline_state",   PIPELINE_STATE_SCHEMA)


# ── Idempotency ───────────────────────────────────────────────────────────────

def get_processed_ids(client, project, dataset, form_uid):
    """Return set of submission IDs already loaded for this form."""
    try:
        result = client.query(
            f"SELECT submission_id "
            f"FROM `{project}.{dataset}.processed_ids` "
            f"WHERE form_uid = '{form_uid}'"
        ).to_dataframe()
        return set(result["submission_id"].astype(str).tolist())
    except Exception:
        return set()


def record_processed_ids(client, project, dataset,
                         ids, form_uid, run_id):
    """Record submission IDs as processed to prevent future duplicates."""
    if not ids:
        return
    ref  = f"{project}.{dataset}.processed_ids"
    rows = [
        {
            "submission_id":   str(sid),
            "form_uid":        form_uid,
            "processed_at":    datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id": run_id,
        }
        for sid in ids
    ]
    client.insert_rows_json(ref, rows)
    logger.info(f"Recorded {len(rows)} processed IDs")


# ── Quarantine ────────────────────────────────────────────────────────────────

def quarantine_rows(client, project, dataset, table_name,
                    rows_info, run_id, source="sync"):
    """
    Send rows to the quarantine table — never silently delete bad data.
    rows_info: list of {'row': dict, 'reason': str}
    """
    if not rows_info:
        return
    ensure_table(client, project, dataset, table_name, QUARANTINE_SCHEMA)
    ref  = f"{project}.{dataset}.{table_name}"
    rows = [
        {
            "quarantine_reason": info["reason"],
            "quarantine_at":     datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id":   run_id,
            "source":            source,
            "raw_row_json":      json.dumps(
                info["row"], default=str
            ),
        }
        for info in rows_info
    ]
    client.insert_rows_json(ref, rows)
    logger.warning(
        f"Quarantined {len(rows)} row(s) in {table_name}"
    )


# ── Core load functions ───────────────────────────────────────────────────────

def batch_load(client, df, table_ref, schema, mode="append"):
    """
    Load a DataFrame to BQ using a batch load job.
    Used by the nightly full sync job.
    mode: 'append' (incremental) or 'truncate' (full refresh)
    """
    if df.empty:
        logger.info("DataFrame is empty — skipping batch load")
        return 0

    disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if mode == "truncate"
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=disposition,
        autodetect=False,
    )
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    job.result()
    row_count = client.get_table(table_ref).num_rows
    logger.info(
        f"Batch load complete: {row_count} rows in {table_ref}"
    )
    return row_count


def streaming_insert(client, df, table_ref):
    """
    Insert rows using BQ streaming insert.
    Used by the webhook receiver for single-submission real-time loads.
    Bypasses the 1500 batch jobs/day/table limit (Edge Case 7).
    """
    if df.empty:
        logger.info("DataFrame is empty — skipping streaming insert")
        return 0

    rows = json.loads(df.to_json(orient="records", default_handler=str))
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(
            f"BQ streaming insert errors: {errors}"
        )
    logger.info(
        f"Streaming insert complete: {len(rows)} row(s) to {table_ref}"
    )
    return len(rows)


# ── Pipeline state ────────────────────────────────────────────────────────────

def get_pipeline_state(client, project, dataset, form_uid):
    """Return pipeline state dict for a form, or None if first run."""
    try:
        result = client.query(
            f"SELECT * FROM `{project}.{dataset}.pipeline_state` "
            f"WHERE form_uid = '{form_uid}' LIMIT 1"
        ).to_dataframe()
        return result.iloc[0].to_dict() if not result.empty else None
    except Exception:
        return None


def save_pipeline_state(client, project, dataset,
                        form_uid, sheet_id, sheet_url,
                        emails_sent=False):
    """Save or update pipeline state after first run setup."""
    ref  = f"{project}.{dataset}.pipeline_state"
    rows = [
        {
            "form_uid":       form_uid,
            "sheet_id":       sheet_id,
            "sheet_url":      sheet_url,
            "emails_sent":    emails_sent,
            "first_run_at":   datetime.now(timezone.utc).isoformat(),
            "last_synced_at": datetime.now(timezone.utc).isoformat(),
        }
    ]
    client.insert_rows_json(ref, rows)
    logger.info(f"Pipeline state saved for form {form_uid}")


def log_pipeline_run(client, project, dataset, run_id, form_uid,
                     status, rows_fetched=0, rows_loaded=0,
                     rows_quarantined=0, rows_test_filtered=0,
                     error=None, source="sync"):
    """Write a pipeline run record to the audit table."""
    ref  = f"{project}.{dataset}.pipeline_runs"
    rows = [
        {
            "run_id":            run_id,
            "form_uid":          form_uid,
            "started_at":        datetime.now(timezone.utc).isoformat(),
            "completed_at":      datetime.now(timezone.utc).isoformat(),
            "status":            status,
            "rows_fetched":      rows_fetched,
            "rows_loaded":       rows_loaded,
            "rows_quarantined":  rows_quarantined,
            "rows_test_filtered":rows_test_filtered,
            "error_message":     str(error) if error else None,
            "source":            source,
        }
    ]
    try:
        client.insert_rows_json(ref, rows)
    except Exception as e:
        logger.warning(f"Could not write to pipeline_runs: {e}")
