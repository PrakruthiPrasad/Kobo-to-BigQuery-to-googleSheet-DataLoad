"""
transformer.py — Clean and transform raw Kobo submissions.

Responsibilities:
  - Normalise column names to snake_case (BQ-safe)
  - Drop internal Kobo system columns
  - Preserve form version for cross-version analysis
  - Add pipeline metadata columns
  - Cast all values to string (prevents BQ type conflicts)
  - Detect and flatten repeat groups into child DataFrames
  - Filter out test submissions (quarantine, never load to BQ)
"""
import re
import logging
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Default keywords that mark a submission as a test entry.
# Whole-word matching: "protest" does NOT match "test".
DEFAULT_TEST_KEYWORDS = ["test", "testing", "demo", "sample", "dummy"]

# Internal Kobo columns that are not useful in BigQuery
_DROP_COLS = [
    "formhub_uuid", "meta_instanceid", "xform_id_string",
    "status", "submitted_by",
]

# Pipeline metadata columns — excluded from test-keyword scanning
_META_COLS = {
    "pipeline_loaded_at", "pipeline_run_id",
    "pipeline_form_uid", "kobo_form_version",
}


def clean_column_name(col):
    """
    Normalise a column name to a BigQuery-safe snake_case identifier.
    Examples:
        'Full Name'        -> 'full_name'
        'group/question_1' -> 'group_question_1'
        '_submission_time' -> 'submission_time'
        '  Spaces  '       -> 'spaces'
        'A--B'             -> 'a_b'
    """
    col = str(col).strip().lower()
    col = re.sub(r"[^a-z0-9_]", "_", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_")


def is_test_submission(row_dict, keywords=None):
    """
    Check whether any field in a submission contains a test keyword.
    Uses whole-word matching to avoid false positives.
    ('protest' does NOT match 'test', 'latest' does NOT match 'test')

    Returns:
        (True, matched_keyword)  if a test keyword is found
        (False, None)            if the row is clean
    """
    if keywords is None:
        keywords = DEFAULT_TEST_KEYWORDS

    for col, val in row_dict.items():
        if col in _META_COLS:
            continue
        if val is None or not isinstance(val, str):
            continue
        val_lower = val.lower().strip()
        for kw in keywords:
            kw_lower = kw.lower()
            # Whole-word match: exact, starts-with, ends-with, or surrounded
            if (val_lower == kw_lower
                    or val_lower.startswith(kw_lower + " ")
                    or val_lower.endswith(" " + kw_lower)
                    or f" {kw_lower} " in val_lower):
                return True, kw
    return False, None


def transform_submissions(submissions, form_uid="unknown",
                          pipeline_run_id="unknown",
                          test_keywords=None):
    """
    Transform raw Kobo submissions into a clean DataFrame.

    Steps:
      1. Build DataFrame from raw dicts
      2. Normalise all column names to snake_case
      3. Drop internal Kobo system columns
      4. Preserve form version
      5. Add pipeline metadata columns
      6. Cast all non-metadata values to str (prevents BQ type conflicts)
      7. Detect and extract repeat groups into separate DataFrames

    Returns:
        (main_df, child_tables, test_rows)
        - main_df:     clean DataFrame ready for BQ staging
        - child_tables: dict of {group_name: DataFrame} for repeat groups
        - test_rows:   list of dicts for submissions that were test entries
    """
    if not submissions:
        return pd.DataFrame(), {}, []

    df = pd.DataFrame(submissions)

    # 1. Normalise column names
    df.columns = [clean_column_name(c) for c in df.columns]

    # 2. Drop internal Kobo columns
    df = df.drop(
        columns=[c for c in _DROP_COLS if c in df.columns],
        errors="ignore"
    )

    # 3. Preserve form version
    if "version" in df.columns:
        df = df.rename(columns={"version": "kobo_form_version"})
    elif "kobo_form_version" not in df.columns:
        df["kobo_form_version"] = "unknown"

    # 4. Add pipeline metadata
    df["pipeline_loaded_at"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")
    df["pipeline_run_id"]    = pipeline_run_id
    df["pipeline_form_uid"]  = form_uid

    # 5. Detect repeat groups BEFORE casting to string
    df, child_tables = _flatten_repeat_groups(df)

    # 6. Filter test submissions
    test_rows   = []
    clean_rows  = []
    for _, row in df.iterrows():
        detected, kw = is_test_submission(
            row.to_dict(), keywords=test_keywords
        )
        if detected:
            test_rows.append({
                "row":    row.to_dict(),
                "reason": f"test_submission_detected — keyword: \"{kw}\"",
            })
            logger.warning(
                f"Test submission detected (_id={row.get('_id', '?')}) "
                f"keyword='{kw}' — quarantining"
            )
        else:
            clean_rows.append(row)

    clean_df = (
        pd.DataFrame(clean_rows).reset_index(drop=True)
        if clean_rows else pd.DataFrame(columns=df.columns)
    )

    # 7. Cast all non-metadata columns to string safely
    for col in clean_df.columns:
        if col not in _META_COLS:
            clean_df[col] = (
                clean_df[col]
                .astype(str)
                .replace({"nan": None, "None": None, "<NA>": None})
            )

    if test_rows:
        logger.info(
            f"Transform complete: {len(clean_df)} clean rows, "
            f"{len(test_rows)} test rows quarantined"
        )

    return clean_df, child_tables, test_rows


def _flatten_repeat_groups(df):
    """
    Detect columns containing lists (Kobo repeat groups).
    Flattens each one into a separate child DataFrame.
    The child DataFrame includes parent_submission_id as a foreign key.
    Returns (main_df_without_list_cols, {group_name: child_df}).
    """
    child_tables = {}
    cols_to_drop = []

    for col in df.columns:
        sample = df[col].dropna()
        if sample.empty:
            continue
        first_val = sample.iloc[0]
        if not isinstance(first_val, list):
            continue

        logger.info(
            f"Repeat group detected: '{col}' — "
            f"flattening to child table"
        )
        rows = []
        for _, main_row in df.iterrows():
            items = main_row[col] if isinstance(main_row[col], list) else []
            parent_id = main_row.get("_id", main_row.get("id", "unknown"))
            for item in items:
                if item is None:
                    continue
                flat = {"parent_submission_id": str(parent_id)}
                flat.update({
                    clean_column_name(k): v for k, v in item.items()
                })
                rows.append(flat)

        if rows:
            child_df = pd.DataFrame(rows)
            for c in child_df.columns:
                if c != "parent_submission_id":
                    child_df[c] = (
                        child_df[c]
                        .astype(str)
                        .replace({"nan": None, "None": None})
                    )
            child_tables[col] = child_df

        cols_to_drop.append(col)

    df = df.drop(columns=cols_to_drop, errors="ignore")
    return df, child_tables
