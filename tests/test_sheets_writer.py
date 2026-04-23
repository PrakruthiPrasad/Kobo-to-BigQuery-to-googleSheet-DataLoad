"""
test_sheets_writer.py — Unit tests for shared/sheets_writer.py
Tests Sheet creation, tab naming, windowing, notifications.
"""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

from sheets_writer import (
    get_or_create_spreadsheet, write_to_sheet,
    share_and_notify_first_run, notify_new_entries,
    _build_plain_text_preview, move_to_shared_drive,
)


class TestGetOrCreateSpreadsheet:
    def test_opens_existing_sheet_when_id_provided(
            self, mock_gc, mock_spreadsheet):
        sheet, is_new = get_or_create_spreadsheet(
            mock_gc, "existing-id", "Test Sheet"
        )
        mock_gc.open_by_key.assert_called_once_with("existing-id")
        assert is_new is False
    
    # def test_creates_new_sheet_when_id_blank(self, mock_gc, mock_spreadsheet):
    #     sheet, is_new = get_or_create_spreadsheet(
    #         mock_gc, "", "New Sheet"
    #     )
    #     mock_gc.create.assert_called_once_with("New Sheet")
    #     assert is_new is True

    # def test_creates_new_sheet_when_id_not_found(
    #         self, mock_gc, mock_spreadsheet):
    #     import gspread
    #     mock_gc.open_by_key.side_effect = (
    #         gspread.exceptions.SpreadsheetNotFound
    #     )
    #     sheet, is_new = get_or_create_spreadsheet(
    #         mock_gc, "bad-id", "Fallback Sheet"
    #     )
    #     mock_gc.create.assert_called_once_with("Fallback Sheet")
    #     assert is_new is True


class TestWriteToSheet:
    @patch("sheets_writer.set_with_dataframe")
    def test_renames_sheet1_to_correct_tab_name(
            self, mock_swdf, mock_spreadsheet, clean_df):
        ws      = MagicMock()
        ws.title = "Sheet1"
        mock_spreadsheet.worksheets.return_value = [ws]
        mock_spreadsheet.worksheet.return_value  = ws

        write_to_sheet(mock_spreadsheet, "Survey Data", clean_df, mode="overwrite")

        ws.update_title.assert_called_once_with("Survey Data")

    @patch("sheets_writer.set_with_dataframe")
    def test_overwrites_existing_tab(self, mock_swdf, mock_spreadsheet,
                                      clean_df):
        ws      = MagicMock()
        ws.title = "Survey Data"
        mock_spreadsheet.worksheets.return_value = [ws]
        mock_spreadsheet.worksheet.return_value  = ws

        write_to_sheet(mock_spreadsheet, "Survey Data", clean_df, mode="overwrite")

        ws.clear.assert_called_once()

    @patch("sheets_writer.set_with_dataframe")
    def test_creates_new_tab_when_neither_exists(
            self, mock_swdf, mock_spreadsheet, clean_df):
        mock_spreadsheet.worksheets.return_value = []
        new_ws = MagicMock()
        mock_spreadsheet.add_worksheet.return_value = new_ws

        write_to_sheet(mock_spreadsheet, "Survey Data", clean_df, mode="overwrite")

        mock_spreadsheet.add_worksheet.assert_called_once()

    @patch("sheets_writer.set_with_dataframe")
    def test_applies_row_windowing(self, mock_swdf, mock_spreadsheet):
        """Sheet should only contain last max_rows rows."""
        large_df = pd.DataFrame(
            {"_id": [str(i) for i in range(200)],
             "name": [f"User{i}" for i in range(200)]}
        )
        ws = MagicMock()
        ws.title = "Sheet1"
        mock_spreadsheet.worksheets.return_value = [ws]
        mock_spreadsheet.worksheet.return_value  = ws

        write_to_sheet(mock_spreadsheet, "Survey Data",
                        large_df, max_rows=50, mode="overwrite")

        # set_with_dataframe was called with windowed data
        assert mock_swdf.called
        called_df = mock_swdf.call_args[0][1]
        assert len(called_df) == 50

    def test_skips_empty_dataframe(self, mock_spreadsheet):
        result = write_to_sheet(
            mock_spreadsheet, "Survey Data", pd.DataFrame()
        )
        assert result is None
        mock_spreadsheet.worksheets.assert_not_called()


class TestBuildPlainTextPreview:
    def test_includes_field_values(self, clean_df):
        preview = _build_plain_text_preview(clean_df.head(1))
        assert "Alice Kamau" in preview

    def test_excludes_pipeline_metadata_columns(self, clean_df):
        preview = _build_plain_text_preview(clean_df.head(1))
        assert "pipeline_run_id" not in preview
        assert "pipeline_loaded_at" not in preview

    def test_returns_empty_string_for_empty_df(self):
        preview = _build_plain_text_preview(pd.DataFrame())
        assert preview == ""

    def test_returns_empty_for_none(self):
        preview = _build_plain_text_preview(None)
        assert preview == ""

    def test_shows_at_most_3_entries(self, clean_df):
        # Build df with 5 rows
        big_df = pd.concat([clean_df] * 3, ignore_index=True)
        preview = _build_plain_text_preview(big_df, max_entries=3)
        assert "more entries" in preview


class TestShareAndNotifyFirstRun:
    def test_shares_with_all_team_emails(self, mock_spreadsheet):
        emails = ["alice@example.org", "bob@example.org"]
        result = share_and_notify_first_run(mock_spreadsheet, emails)
        assert mock_spreadsheet.share.call_count == 2
        assert result is True

    def test_skips_when_no_emails(self, mock_spreadsheet):
        result = share_and_notify_first_run(mock_spreadsheet, [])
        mock_spreadsheet.share.assert_not_called()
        assert result is False

    def test_email_contains_sheet_url(self, mock_spreadsheet, clean_df):
        share_and_notify_first_run(
            mock_spreadsheet, ["alice@example.org"],
            new_rows_df=clean_df
        )
        call_kwargs = mock_spreadsheet.share.call_args[1]
        assert "test-sheet-id-123" in call_kwargs["email_message"]

    def test_email_includes_data_preview(self, mock_spreadsheet, clean_df):
        share_and_notify_first_run(
            mock_spreadsheet, ["alice@example.org"],
            new_rows_df=clean_df
        )
        call_kwargs = mock_spreadsheet.share.call_args[1]
        assert "Alice Kamau" in call_kwargs["email_message"]


class TestNotifyNewEntries:
    def test_sends_notification_to_all_recipients(
            self, mock_spreadsheet, clean_df):
        emails = ["lead@org.org", "manager@yahoo.com"]
        result = notify_new_entries(mock_spreadsheet, emails, clean_df)
        assert mock_spreadsheet.share.call_count == 2
        assert result is True

    def test_skips_when_no_emails(self, mock_spreadsheet, clean_df):
        result = notify_new_entries(mock_spreadsheet, [], clean_df)
        mock_spreadsheet.share.assert_not_called()
        assert result is False

    def test_skips_when_no_new_rows(self, mock_spreadsheet):
        result = notify_new_entries(
            mock_spreadsheet, ["alice@org.org"], pd.DataFrame()
        )
        mock_spreadsheet.share.assert_not_called()
        assert result is False

    def test_notification_includes_entry_count(
            self, mock_spreadsheet, clean_df):
        notify_new_entries(
            mock_spreadsheet, ["alice@org.org"], clean_df
        )
        call_kwargs = mock_spreadsheet.share.call_args[1]
        # 2 rows in clean_df fixture
        assert "2 new entries" in call_kwargs["email_message"]

    def test_notification_includes_data_preview(
            self, mock_spreadsheet, clean_df):
        notify_new_entries(
            mock_spreadsheet, ["alice@org.org"], clean_df
        )
        call_kwargs = mock_spreadsheet.share.call_args[1]
        assert "Alice Kamau" in call_kwargs["email_message"]

    def test_notification_includes_sheet_link(
            self, mock_spreadsheet, clean_df):
        notify_new_entries(
            mock_spreadsheet, ["alice@org.org"], clean_df
        )
        call_kwargs = mock_spreadsheet.share.call_args[1]
        assert "test-sheet-id-123" in call_kwargs["email_message"]

    def test_recipients_can_be_any_email_provider(
            self, mock_spreadsheet, clean_df):
        """Gmail, Yahoo, company emails all work with gspread share()."""
        emails = [
            "alice@gmail.com",
            "bob@yahoo.com",
            "carol@companydomain.org",
        ]
        notify_new_entries(mock_spreadsheet, emails, clean_df)
        assert mock_spreadsheet.share.call_count == 3


class TestMoveToSharedDrive:
    def test_moves_sheet_to_shared_drive(self, mock_spreadsheet):
        drive_service = MagicMock()
        files_mock    = MagicMock()
        drive_service.files.return_value  = files_mock
        files_mock.update.return_value    = MagicMock()
        files_mock.update.return_value.execute.return_value = {}

        result = move_to_shared_drive(
            drive_service, "file-id", "folder-id"
        )
        assert result is True
        files_mock.update.assert_called_once()

    def test_skips_when_no_folder_id(self):
        result = move_to_shared_drive(MagicMock(), "file-id", "")
        assert result is False

    def test_skips_when_no_drive_service(self):
        result = move_to_shared_drive(None, "file-id", "folder-id")
        assert result is False

    def test_returns_false_on_error(self):
        drive_service = MagicMock()
        drive_service.files.side_effect = Exception("Access denied")
        result = move_to_shared_drive(
            drive_service, "file-id", "folder-id"
        )
        assert result is False
