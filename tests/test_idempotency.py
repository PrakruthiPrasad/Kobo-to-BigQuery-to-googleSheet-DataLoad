"""
test_idempotency.py — Tests for duplicate submission handling.
Verifies EC1 (missed webhook recovery) and EC2 (duplicate delivery).
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

from loader import get_processed_ids, record_processed_ids


class TestIdempotency:
    def test_new_submission_not_in_processed_ids(self, mock_bq_client):
        mock_result = MagicMock()
        mock_result.to_dataframe.return_value = pd.DataFrame(
            {"submission_id": ["10", "20", "30"]}
        )
        mock_bq_client.query.return_value = mock_result

        existing = get_processed_ids(
            mock_bq_client, "p", "d", "form1"
        )
        assert "99" not in existing

    def test_existing_submission_in_processed_ids(self, mock_bq_client):
        mock_result = MagicMock()
        mock_result.to_dataframe.return_value = pd.DataFrame(
            {"submission_id": ["10", "20", "30"]}
        )
        mock_bq_client.query.return_value = mock_result

        existing = get_processed_ids(
            mock_bq_client, "p", "d", "form1"
        )
        assert "10" in existing

    def test_dedup_filter_removes_existing_ids(self, mock_bq_client):
        """Simulate what the sync job does: filter out already-processed IDs."""
        mock_result = MagicMock()
        mock_result.to_dataframe.return_value = pd.DataFrame(
            {"submission_id": ["1", "2"]}
        )
        mock_bq_client.query.return_value = mock_result

        all_rows = pd.DataFrame({
            "_id":  ["1", "2", "3", "4"],
            "name": ["Alice", "Bob", "Carol", "David"],
        })
        existing_ids = get_processed_ids(
            mock_bq_client, "p", "d", "form1"
        )
        new_rows = all_rows[
            ~all_rows["_id"].astype(str).isin(existing_ids)
        ]

        assert len(new_rows) == 2
        assert set(new_rows["_id"].tolist()) == {"3", "4"}

    def test_record_then_dedup_prevents_reinsert(self, mock_bq_client):
        """After recording IDs, those IDs must be filtered on next run."""
        # First: record IDs 1 and 2
        record_processed_ids(
            mock_bq_client, "p", "d", ["1", "2"], "form1", "run1"
        )
        mock_bq_client.insert_rows_json.assert_called_once()

        # Second: simulate next run — IDs 1 and 2 are now in the table
        mock_result = MagicMock()
        mock_result.to_dataframe.return_value = pd.DataFrame(
            {"submission_id": ["1", "2"]}
        )
        mock_bq_client.query.return_value = mock_result

        existing = get_processed_ids(
            mock_bq_client, "p", "d", "form1"
        )
        assert "1" in existing
        assert "2" in existing

    def test_full_dedup_cycle(self, mock_bq_client):
        """
        End-to-end dedup simulation:
        Run 1: process submissions 1, 2, 3
        Run 2: Kobo returns 1, 2, 3, 4, 5 — only 4, 5 should be new
        """
        # Run 1
        mock_result_run1 = MagicMock()
        mock_result_run1.to_dataframe.return_value = pd.DataFrame(
            {"submission_id": []}
        )
        mock_bq_client.query.return_value = mock_result_run1
        existing_run1 = get_processed_ids(
            mock_bq_client, "p", "d", "form1"
        )
        all_run1 = pd.DataFrame({"_id": ["1", "2", "3"]})
        new_run1 = all_run1[
            ~all_run1["_id"].isin(existing_run1)
        ]
        assert len(new_run1) == 3
        record_processed_ids(
            mock_bq_client, "p", "d",
            new_run1["_id"].tolist(), "form1", "run1"
        )

        # Run 2 — IDs 1, 2, 3 now in processed_ids
        mock_result_run2 = MagicMock()
        mock_result_run2.to_dataframe.return_value = pd.DataFrame(
            {"submission_id": ["1", "2", "3"]}
        )
        mock_bq_client.query.return_value = mock_result_run2
        existing_run2 = get_processed_ids(
            mock_bq_client, "p", "d", "form1"
        )
        all_run2 = pd.DataFrame({"_id": ["1", "2", "3", "4", "5"]})
        new_run2 = all_run2[
            ~all_run2["_id"].isin(existing_run2)
        ]
        assert len(new_run2) == 2
        assert set(new_run2["_id"].tolist()) == {"4", "5"}
