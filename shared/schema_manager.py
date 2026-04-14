"""
schema_manager.py — BigQuery schema inference and auto-evolution.

Responsibilities:
  - Infer BQ schema from a DataFrame (all STRING, except timestamps)
  - Detect new columns vs existing BQ table schema
  - Add new columns to BQ table automatically (idempotent — safe for concurrency)
  - Log every schema change to the schema_changelog table
"""
import logging
from datetime import datetime, timezone
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def infer_bq_schema(df):
    """
    Infer a BigQuery schema from a pandas DataFrame.
    All columns are mapped to STRING except pipeline_loaded_at -> TIMESTAMP.
    Using STRING for all raw columns prevents type conflicts when
    Kobo form field types change.
    """
    schema = []
    for col in df.columns:
        bq_type = "TIMESTAMP" if col == "pipeline_loaded_at" else "STRING"
        schema.append(
            bigquery.SchemaField(col, bq_type, mode="NULLABLE")
        )
    return schema


def get_existing_schema(client, table_ref):
    """
    Return the current schema of a BQ table as a list of SchemaFields.
    Returns an empty list if the table does not exist yet.
    """
    try:
        return list(client.get_table(table_ref).schema)
    except Exception:
        return []


def evolve_schema(client, table_ref, new_df, existing_schema,
                  form_uid=None, run_id=None,
                  project=None, dataset=None):
    """
    Compare new_df columns against existing BQ schema.
    Adds any new columns as NULLABLE STRING fields.

    This function is idempotent — if two processes simultaneously try to
    add the same column, the second attempt catches the 'already exists'
    error and treats it as success (Edge Case 6).

    Returns list of newly added column names.
    """
    existing_cols = {f.name for f in existing_schema}
    new_cols      = set(new_df.columns) - existing_cols

    if not new_cols:
        logger.debug("Schema unchanged — no new columns")
        return []

    try:
        table          = client.get_table(table_ref)
        updated_schema = list(table.schema)
        added          = []

        for col in sorted(new_cols):
            updated_schema.append(
                bigquery.SchemaField(col, "STRING", mode="NULLABLE")
            )
            logger.warning(
                f"New field detected: '{col}' — adding to BQ schema"
            )
            added.append(col)

        table.schema = updated_schema
        client.update_table(table, ["schema"])
        logger.info(f"Schema evolved: {len(added)} new field(s) added")

        # Log to changelog if BQ details provided
        if project and dataset and added:
            _log_schema_changes(
                client, project, dataset, form_uid,
                added, run_id
            )

        return added

    except Exception as e:
        if "already exists" in str(e).lower():
            logger.warning(
                "Column already added by concurrent process — continuing"
            )
            return []
        raise


def ensure_schema_changelog(client, project, dataset):
    """Create schema_changelog table if it does not exist."""
    ref    = f"{project}.{dataset}.schema_changelog"
    schema = [
        bigquery.SchemaField("form_uid",        "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("changed_at",      "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("change_type",     "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("field_name",      "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("bq_type",         "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("pipeline_run_id", "STRING",    mode="NULLABLE"),
    ]
    try:
        client.get_table(ref)
    except Exception:
        client.create_table(bigquery.Table(ref, schema=schema))
        logger.info(f"Created schema_changelog table")


def _log_schema_changes(client, project, dataset,
                        form_uid, added_cols, run_id):
    """Write schema change records to the changelog table."""
    ref  = f"{project}.{dataset}.schema_changelog"
    rows = [
        {
            "form_uid":        form_uid or "unknown",
            "changed_at":      datetime.now(timezone.utc).isoformat(),
            "change_type":     "field_added",
            "field_name":      col,
            "bq_type":         "STRING",
            "pipeline_run_id": run_id or "unknown",
        }
        for col in added_cols
    ]
    try:
        client.insert_rows_json(ref, rows)
    except Exception as e:
        logger.warning(f"Could not write to schema_changelog: {e}")
