"""
test_form_registry.py — Unit tests for shared/form_registry.py
"""
import pytest
import os
from unittest.mock import patch

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))


class TestFormRegistry:
    def test_get_form_config_returns_correct_config(self):
        """get_form_config returns config for a known form_uid."""
        with patch.dict(os.environ, {
            "FORM_UID_CONTACT_US":    "uid_contact_123",
            "BQ_TABLE_CONTACT_US":    "kobo_contact_us",
            "SHEET_ID_CONTACT_US":    "sheet_contact_123",
            "SHEET_TAB_CONTACT_US":   "Contact Us",
            "SHEET_NAME_CONTACT_US":  "Contact Us Responses",
            "FORM_UID_FOOD_DISTRIBUTION": "",
            "FORM_UID_MEMBERSHIP":    "",
        }):
            import importlib
            import form_registry
            importlib.reload(form_registry)
            from form_registry import get_form_config

            config = get_form_config("uid_contact_123")
            assert config is not None
            assert config["name"]      == "ContactUs"
            assert config["bq_table"]  == "kobo_contact_us"
            assert config["sheet_id"]  == "sheet_contact_123"
            assert config["sheet_tab"] == "Contact Us"

    def test_get_form_config_returns_none_for_unknown_uid(self):
        """get_form_config returns None for unregistered form_uid."""
        with patch.dict(os.environ, {
            "FORM_UID_CONTACT_US":        "uid_contact_123",
            "FORM_UID_FOOD_DISTRIBUTION": "",
            "FORM_UID_MEMBERSHIP":        "",
        }):
            import importlib
            import form_registry
            importlib.reload(form_registry)
            from form_registry import get_form_config

            config = get_form_config("unknown_uid_xyz")
            assert config is None

    def test_get_all_forms_returns_only_configured_forms(self):
        """get_all_forms skips forms with empty FORM_UID."""
        with patch.dict(os.environ, {
            "FORM_UID_CONTACT_US":        "uid_contact_123",
            "FORM_UID_FOOD_DISTRIBUTION": "",   # not configured
            "FORM_UID_MEMBERSHIP":        "",   # not configured
        }):
            import importlib
            import form_registry
            importlib.reload(form_registry)
            from form_registry import get_all_forms

            forms = get_all_forms()
            assert len(forms) == 1
            assert forms[0][0] == "uid_contact_123"

    def test_get_all_forms_returns_multiple_configured_forms(self):
        """get_all_forms returns all forms when multiple are configured."""
        with patch.dict(os.environ, {
            "FORM_UID_CONTACT_US":        "uid_contact_123",
            "FORM_UID_FOOD_DISTRIBUTION": "uid_food_456",
            "FORM_UID_MEMBERSHIP":        "uid_member_789",
        }):
            import importlib
            import form_registry
            importlib.reload(form_registry)
            from form_registry import get_all_forms

            forms = get_all_forms()
            assert len(forms) == 3
            uids = [f[0] for f in forms]
            assert "uid_contact_123" in uids
            assert "uid_food_456"    in uids
            assert "uid_member_789"  in uids

    def test_get_all_uids_returns_list(self):
        """get_all_form_uids returns a list of UIDs."""
        with patch.dict(os.environ, {
            "FORM_UID_CONTACT_US":        "uid_contact_123",
            "FORM_UID_FOOD_DISTRIBUTION": "uid_food_456",
            "FORM_UID_MEMBERSHIP":        "",
        }):
            import importlib
            import form_registry
            importlib.reload(form_registry)
            from form_registry import get_all_form_uids

            uids = get_all_form_uids()
            assert "uid_contact_123" in uids
            assert "uid_food_456"    in uids
            assert len(uids) == 2

    def test_empty_registry_when_no_env_vars_set(self):
        """Registry is empty when no FORM_UID env vars are set."""
        with patch.dict(os.environ, {
            "FORM_UID_CONTACT_US":        "",
            "FORM_UID_FOOD_DISTRIBUTION": "",
            "FORM_UID_MEMBERSHIP":        "",
        }):
            import importlib
            import form_registry
            importlib.reload(form_registry)
            from form_registry import get_all_forms

            forms = get_all_forms()
            assert len(forms) == 0
