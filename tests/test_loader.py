"""
test_loader.py — Unit tests for shared/loader.py
Tests BQ load operations, idempotency, quarantine.
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

from loader import (
    get_processed_ids, record_processed_ids,
    quarantine_rows, batch_load, streaming_insert,
    get_pipeline_state, save_pipeline_state, log_pipeline_run,
)
from google.cloud import bigquery


class TestGetProcessedIds:
    def test_returns_set_of_ids(self, mock_bq_client):
        mock_result = MagicMock()
        mock_result.to_dataframe.return_value = pd.DataFrame(
            {"submission_id": ["1", "2", "3"]}
        )
        mock_bq_client.query.return_value = mock_result
        result = get_processed_ids(mock_bq_client, "p", "d", "form1")
        assert result == {"1", "2", "3"}

    def test_returns_empty_set_on_error(self, mock_bq_client):
        mock_bq_client.query.side_effect = Exception("Table not found")
        result = get_processed_ids(mock_bq_client, "p", "d", "form1")
        assert result == set()


class TestRecordProcessedIds:
    def test_records_ids_to_bq(self, mock_bq_client):
        record_processed_ids(
            mock_bq_client, "p", "d", ["1", "2", "3"], "form1", "run1"
        )
        mock_bq_client.insert_rows_json.assert_called_once()
        rows = mock_bq_client.insert_rows_json.call_args[0][1]
        assert len(rows) == 3
        assert rows[0]["submission_id"] == "1"
        assert rows[0]["form_uid"] == "form1"

    def test_skips_empty_id_list(self, mock_bq_client):
        record_processed_ids(mock_bq_client, "p", "d", [], "form1", "run1")
        mock_bq_client.insert_rows_json.assert_not_called()


class TestQuarantineRows:
    def test_quarantines_rows_to_bq(self, mock_bq_client):
        mock_bq_client.get_table.side_effect = None
        rows_info = [
            {"row": {"_id": 99, "name": "test"},
             "reason": "test_submission_detected — keyword: \"test\""},
        ]
        quarantine_rows(
            mock_bq_client, "p", "d", "quarantine_table",
            rows_info, run_id="run1", source="sync"
        )
        mock_bq_client.insert_rows_json.assert_called_once()
        q_rows = mock_bq_client.insert_rows_json.call_args[0][1]
        assert len(q_rows) == 1
        assert "test_submission_detected" in q_rows[0]["quarantine_reason"]
        assert q_rows[0]["source"] == "sync"

    def test_skips_empty_rows_info(self, mock_bq_client):
        quarantine_rows(
            mock_bq_client, "p", "d", "q_table",
            [], run_id="run1"
        )
        mock_bq_client.insert_rows_json.assert_not_called()

    def test_quarantine_includes_raw_json(self, mock_bq_client):
        mock_bq_client.get_table.side_effect = None
        rows_info = [{"row": {"_id": 1, "name": "test"},
                      "reason": "test"}]
        quarantine_rows(
            mock_bq_client, "p", "d", "q", rows_info, "run1"
        )
        q_rows = mock_bq_client.insert_rows_json.call_args[0][1]
        assert "raw_row_json" in q_rows[0]
        import json
        raw = json.loads(q_rows[0]["raw_row_json"])
        assert raw["_id"] == 1


class TestBatchLoad:
    def test_loads_dataframe_to_bq(self, mock_bq_client, clean_df):
        from google.cloud import bigquery as bq
        mock_table = MagicMock()
        mock_table.num_rows = len(clean_df)
        mock_bq_client.get_table.side_effect = None
        mock_bq_client.get_table.return_value = mock_table
        mock_job = MagicMock()
        mock_bq_client.load_table_from_dataframe.return_value = mock_job

        schema = [bq.SchemaField("name", "STRING", mode="NULLABLE")]
        result = batch_load(mock_bq_client, clean_df, "p.d.t", schema)

        mock_bq_client.load_table_from_dataframe.assert_called_once()
        assert result == len(clean_df)

    def test_skips_empty_dataframe(self, mock_bq_client):
        from google.cloud import bigquery as bq
        result = batch_load(
            mock_bq_client, pd.DataFrame(), "p.d.t",
            [bq.SchemaField("x", "STRING")]
        )
        mock_bq_client.load_table_from_dataframe.assert_not_called()
        assert result == 0

    def test_uses_truncate_disposition(self, mock_bq_client, clean_df):
        from google.cloud import bigquery as bq
        mock_table = MagicMock()
        mock_table.num_rows = 2
        mock_bq_client.get_table.side_effect = None
        mock_bq_client.get_table.return_value = mock_table
        mock_bq_client.load_table_from_dataframe.return_value = MagicMock()

        batch_load(mock_bq_client, clean_df, "p.d.t",
                   [bq.SchemaField("x", "STRING")], mode="truncate")

        job_config = mock_bq_client.load_table_from_dataframe.call_args[1]["job_config"]
        assert job_config.write_disposition == bq.WriteDisposition.WRITE_TRUNCATE

    def test_uses_append_disposition(self, mock_bq_client, clean_df):
        from google.cloud import bigquery as bq
        mock_table = MagicMock()
        mock_table.num_rows = 2
        mock_bq_client.get_table.side_effect = None
        mock_bq_client.get_table.return_value = mock_table
        mock_bq_client.load_table_from_dataframe.return_value = MagicMock()

        batch_load(mock_bq_client, clean_df, "p.d.t",
                   [bq.SchemaField("x", "STRING")], mode="append")

        job_config = mock_bq_client.load_table_from_dataframe.call_args[1]["job_config"]
        assert job_config.write_disposition == bq.WriteDisposition.WRITE_APPEND


class TestStreamingInsert:
    def test_inserts_rows_to_bq(self, mock_bq_client, clean_df):
        mock_bq_client.insert_rows_json.return_value = []
        result = streaming_insert(mock_bq_client, clean_df, "p.d.t")
        mock_bq_client.insert_rows_json.assert_called_once()
        assert result == len(clean_df)

    def test_raises_on_insert_errors(self, mock_bq_client, clean_df):
        mock_bq_client.insert_rows_json.return_value = [
            {"errors": [{"reason": "invalid"}]}
        ]
        with pytest.raises(RuntimeError, match="BQ streaming insert errors"):
            streaming_insert(mock_bq_client, clean_df, "p.d.t")

    def test_skips_empty_dataframe(self, mock_bq_client):
        result = streaming_insert(mock_bq_client, pd.DataFrame(), "p.d.t")
        mock_bq_client.insert_rows_json.assert_not_called()
        assert result == 0


class TestPipelineState:
    def test_get_state_returns_none_when_not_found(self, mock_bq_client):
        mock_result = MagicMock()
        mock_result.to_dataframe.return_value = pd.DataFrame()
        mock_bq_client.query.return_value = mock_result
        result = get_pipeline_state(mock_bq_client, "p", "d", "form1")
        assert result is None

    def test_get_state_returns_dict_when_found(self, mock_bq_client):
        mock_result = MagicMock()
        mock_result.to_dataframe.return_value = pd.DataFrame([{
            "form_uid": "form1", "sheet_id": "abc",
            "emails_sent": True,
        }])
        mock_bq_client.query.return_value = mock_result
        result = get_pipeline_state(mock_bq_client, "p", "d", "form1")
        assert result is not None
        assert result["sheet_id"] == "abc"
        assert result["emails_sent"] is True

    def test_save_pipeline_state_writes_to_bq(self, mock_bq_client):
        save_pipeline_state(
            mock_bq_client, "p", "d", "form1",
            "sheet123", "https://sheets.google.com/...", emails_sent=True
        )
        mock_bq_client.insert_rows_json.assert_called_once()
        rows = mock_bq_client.insert_rows_json.call_args[0][1]
        assert rows[0]["form_uid"]    == "form1"
        assert rows[0]["sheet_id"]    == "sheet123"
        assert rows[0]["emails_sent"] is True
