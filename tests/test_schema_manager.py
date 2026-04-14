"""
test_schema_manager.py — Unit tests for shared/schema_manager.py
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock, call

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

from schema_manager import (
    infer_bq_schema, get_existing_schema, evolve_schema
)
from google.cloud import bigquery


class TestInferBqSchema:
    def test_all_columns_are_string(self):
        df     = pd.DataFrame({"name": ["Alice"], "age": ["30"]})
        schema = infer_bq_schema(df)
        types  = {f.name: f.field_type for f in schema}
        assert types["name"] == "STRING"
        assert types["age"]  == "STRING"

    def test_pipeline_loaded_at_is_timestamp(self):
        df     = pd.DataFrame({"pipeline_loaded_at": ["2025-01-01"],
                                "name": ["Alice"]})
        schema = infer_bq_schema(df)
        types  = {f.name: f.field_type for f in schema}
        assert types["pipeline_loaded_at"] == "TIMESTAMP"
        assert types["name"]               == "STRING"

    def test_all_fields_are_nullable(self):
        df     = pd.DataFrame({"x": ["1"], "y": ["2"]})
        schema = infer_bq_schema(df)
        assert all(f.mode == "NULLABLE" for f in schema)

    def test_schema_is_deterministic(self):
        df = pd.DataFrame({"a": ["1"], "b": ["2"]})
        s1 = {f.name: f.field_type for f in infer_bq_schema(df)}
        s2 = {f.name: f.field_type for f in infer_bq_schema(df)}
        assert s1 == s2


class TestGetExistingSchema:
    def test_returns_empty_list_when_table_not_found(self, mock_bq_client):
        mock_bq_client.get_table.side_effect = Exception("Not found")
        result = get_existing_schema(mock_bq_client, "p.d.t")
        assert result == []

    def test_returns_schema_when_table_exists(self, mock_bq_client):
        mock_table        = MagicMock()
        mock_table.schema = [
            bigquery.SchemaField("name", "STRING", mode="NULLABLE")
        ]
        mock_bq_client.get_table.side_effect = None
        mock_bq_client.get_table.return_value = mock_table
        result = get_existing_schema(mock_bq_client, "p.d.t")
        assert len(result) == 1
        assert result[0].name == "name"


class TestEvolveSchema:
    def test_adds_new_columns(self, mock_bq_client):
        existing = [bigquery.SchemaField("name", "STRING", mode="NULLABLE")]
        new_df   = pd.DataFrame({"name": ["Alice"], "age": ["30"],
                                  "city": ["Nairobi"]})
        mock_table        = MagicMock()
        mock_table.schema = existing
        mock_bq_client.get_table.side_effect = None
        mock_bq_client.get_table.return_value = mock_table

        added = evolve_schema(mock_bq_client, "p.d.t", new_df, existing)

        assert "age"  in added
        assert "city" in added
        mock_bq_client.update_table.assert_called_once()

    def test_no_update_when_schema_unchanged(self, mock_bq_client):
        existing = [bigquery.SchemaField("name", "STRING", mode="NULLABLE")]
        new_df   = pd.DataFrame({"name": ["Alice"]})
        mock_bq_client.get_table.side_effect = None

        added = evolve_schema(mock_bq_client, "p.d.t", new_df, existing)

        assert added == []
        mock_bq_client.update_table.assert_not_called()

    def test_concurrent_add_treated_as_success(self, mock_bq_client):
        """If column already exists (concurrent update), treat as success."""
        existing = [bigquery.SchemaField("name", "STRING", mode="NULLABLE")]
        new_df   = pd.DataFrame({"name": ["Alice"], "age": ["30"]})
        mock_table        = MagicMock()
        mock_table.schema = existing
        mock_bq_client.get_table.side_effect = None
        mock_bq_client.get_table.return_value = mock_table
        mock_bq_client.update_table.side_effect = Exception(
            "column already exists"
        )

        # Should not raise — treats 'already exists' as success
        added = evolve_schema(mock_bq_client, "p.d.t", new_df, existing)
        assert added == []
