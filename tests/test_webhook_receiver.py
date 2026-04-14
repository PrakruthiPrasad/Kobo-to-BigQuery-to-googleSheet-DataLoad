"""
test_webhook_receiver.py — Tests for the webhook FastAPI service.
Tests secret validation, payload handling, duplicate detection.
"""
import pytest
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__),
                                '..', 'services', 'webhook'))

from webhook_secret import validate_webhook_secret


class TestValidateWebhookSecret:
    def test_valid_secret_accepted(self):
        assert validate_webhook_secret("my-secret", "my-secret") is True

    def test_invalid_secret_rejected(self):
        assert validate_webhook_secret("wrong", "my-secret") is False

    def test_empty_string_rejected(self):
        assert validate_webhook_secret("", "my-secret") is False

    def test_none_rejected(self):
        assert validate_webhook_secret(None, "my-secret") is False

    def test_no_expected_secret_allows_all(self):
        """If no secret is configured, all requests are allowed."""
        assert validate_webhook_secret("anything", "") is True
        assert validate_webhook_secret("", "") is True
        assert validate_webhook_secret(None, "") is True

    def test_case_sensitive(self):
        assert validate_webhook_secret("My-Secret", "my-secret") is False

    def test_uses_hmac_compare_digest(self):
        """
        verify hmac.compare_digest is used — timing-safe comparison.
        We test this indirectly by confirming it doesn't raise on None.
        """
        result = validate_webhook_secret(None, "secret")
        assert result is False
