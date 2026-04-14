"""
test_transformer.py — Unit tests for shared/transformer.py
Tests column cleaning, test detection, version tagging, repeat groups.
"""
import pytest
import pandas as pd

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

from transformer import (
    clean_column_name, is_test_submission,
    transform_submissions, _flatten_repeat_groups,
    DEFAULT_TEST_KEYWORDS,
)


# ── Column name cleaning ──────────────────────────────────────────────────────

class TestCleanColumnName:
    def test_spaces_become_underscores(self):
        assert clean_column_name("Full Name") == "full_name"

    def test_slashes_become_underscores(self):
        assert clean_column_name("group/question_1") == "group_question_1"

    def test_leading_trailing_spaces_removed(self):
        assert clean_column_name("  Spaces  ") == "spaces"

    def test_multiple_underscores_collapsed(self):
        assert clean_column_name("A--B") == "a_b"

    def test_all_lowercase(self):
        assert clean_column_name("CamelCase") == "camelcase"

    def test_leading_underscores_stripped(self):
        assert clean_column_name("_submission_time") == "submission_time"

    def test_date_of_birth(self):
        assert clean_column_name("Date of Birth") == "date_of_birth"

    def test_nested_group(self):
        assert clean_column_name("a/b/c") == "a_b_c"


# ── Test submission detection ─────────────────────────────────────────────────

class TestIsTestSubmission:
    def test_detects_test_in_name(self):
        row = {"_id": "1", "name": "test", "message": "real message"}
        detected, kw = is_test_submission(row)
        assert detected is True
        assert kw == "test"

    def test_detects_testing_in_message(self):
        row = {"_id": "1", "name": "Alice", "message": "testing the form"}
        detected, kw = is_test_submission(row)
        assert detected is True
        assert kw == "testing"

    def test_detects_demo_keyword(self):
        row = {"_id": "1", "name": "Demo User", "message": "demo submission"}
        detected, kw = is_test_submission(row)
        assert detected is True

    def test_detects_sample_keyword(self):
        row = {"_id": "1", "name": "Alice", "message": "sample data only"}
        detected, kw = is_test_submission(row)
        assert detected is True

    def test_clean_submission_not_flagged(self):
        row = {"_id": "1", "name": "Alice", "message": "Hello from Nairobi"}
        detected, _ = is_test_submission(row)
        assert detected is False

    def test_protest_not_matched_as_test(self):
        """Whole-word match: 'protest' must NOT match 'test'."""
        row = {"_id": "1", "name": "Bob", "message": "protest march info"}
        detected, _ = is_test_submission(row)
        assert detected is False

    def test_latest_not_matched_as_test(self):
        """Whole-word match: 'latest' must NOT match 'test'."""
        row = {"_id": "1", "name": "Eve", "message": "Latest news update"}
        detected, _ = is_test_submission(row)
        assert detected is False

    def test_attestation_not_matched(self):
        row = {"_id": "1", "name": "Frank", "message": "attestation form"}
        detected, _ = is_test_submission(row)
        assert detected is False

    def test_none_values_not_flagged(self):
        row = {"_id": "1", "name": None, "message": None}
        detected, _ = is_test_submission(row)
        assert detected is False

    def test_custom_keywords(self):
        row = {"_id": "1", "name": "Alice", "message": "pilot study entry"}
        detected, kw = is_test_submission(row, keywords=["pilot"])
        assert detected is True
        assert kw == "pilot"

    def test_meta_columns_excluded_from_check(self):
        """Pipeline metadata columns should never trigger test detection."""
        row = {
            "_id":               "1",
            "name":              "Alice",
            "pipeline_run_id":   "test-run-id-123",  # contains 'test'
            "pipeline_form_uid": "test_form",          # contains 'test'
        }
        detected, _ = is_test_submission(row)
        assert detected is False


# ── transform_submissions ─────────────────────────────────────────────────────

class TestTransformSubmissions:
    def test_returns_empty_df_for_empty_input(self):
        df, children, test_rows = transform_submissions([])
        assert df.empty
        assert children == {}
        assert test_rows == []

    def test_column_names_normalised(self, sample_submissions):
        df, _, _ = transform_submissions(sample_submissions)
        assert "full_name" in df.columns
        assert "Full Name" not in df.columns

    def test_pipeline_metadata_added(self, sample_submissions):
        df, _, _ = transform_submissions(sample_submissions,
                                         pipeline_run_id="run123")
        assert "pipeline_loaded_at"  in df.columns
        assert "pipeline_run_id"     in df.columns
        assert "pipeline_form_uid"   in df.columns
        assert df["pipeline_run_id"].iloc[0] == "run123"

    def test_form_version_preserved(self, sample_submissions):
        df, _, _ = transform_submissions(sample_submissions)
        assert "kobo_form_version" in df.columns
        assert df["kobo_form_version"].iloc[0] == "v1"

    def test_form_version_defaults_to_unknown(self):
        subs = [{"_id": 1, "name": "Alice"}]
        df, _, _ = transform_submissions(subs)
        assert df["kobo_form_version"].iloc[0] == "unknown"

    def test_none_values_become_python_none(self):
        subs = [{"_id": 1, "name": None}]
        df, _, _ = transform_submissions(subs)
        assert df["name"].iloc[0] is None

    def test_nan_becomes_none(self):
        subs = [{"_id": 1, "score": float("nan")}]
        df, _, _ = transform_submissions(subs)
        assert df["score"].iloc[0] is None

    def test_multiple_rows_all_preserved(self, sample_submissions):
        df, _, _ = transform_submissions(sample_submissions)
        assert len(df) == 2

    def test_test_submissions_separated(self, sample_submissions,
                                        test_submissions):
        all_subs = sample_submissions + test_submissions
        df, _, test_rows = transform_submissions(all_subs)
        # Only clean submissions in main df
        assert len(df) == 2
        # Test submissions captured separately
        assert len(test_rows) == 2

    def test_test_rows_include_reason(self, test_submissions):
        _, _, test_rows = transform_submissions(test_submissions)
        assert all("reason" in r for r in test_rows)
        assert all("test_submission_detected" in r["reason"]
                   for r in test_rows)

    def test_internal_kobo_columns_dropped(self):
        subs = [{
            "_id": 1, "name": "Alice",
            "formhub/uuid": "abc", "meta/instanceID": "xyz",
            "submitted_by": "user",
        }]
        df, _, _ = transform_submissions(subs)
        assert "formhub_uuid" not in df.columns
        assert "meta_instanceid" not in df.columns


# ── Repeat group flattening ───────────────────────────────────────────────────

class TestRepeatGroups:
    def test_repeat_group_creates_child_table(self):
        df = pd.DataFrame([{
            "_id": 1, "name": "Alice",
            "household_members": [
                {"name": "Bob", "age": "12"},
                {"name": "Carol", "age": "8"},
            ],
        }])
        main_df, children = _flatten_repeat_groups(df)
        assert "household_members" in children
        assert len(children["household_members"]) == 2

    def test_child_table_has_parent_id(self):
        df = pd.DataFrame([{
            "_id": 42, "name": "Alice",
            "children": [{"name": "Bob"}],
        }])
        _, children = _flatten_repeat_groups(df)
        assert "parent_submission_id" in children["children"].columns
        assert children["children"]["parent_submission_id"].iloc[0] == "42"

    def test_repeat_col_removed_from_main(self):
        df = pd.DataFrame([{
            "_id": 1, "name": "Alice",
            "items": [{"item": "a"}],
        }])
        main_df, _ = _flatten_repeat_groups(df)
        assert "items" not in main_df.columns

    def test_no_repeat_groups_unchanged(self):
        df = pd.DataFrame([{"_id": 1, "name": "Alice", "age": "30"}])
        main_df, children = _flatten_repeat_groups(df)
        assert children == {}
        assert len(main_df) == 1
        assert "name" in main_df.columns
