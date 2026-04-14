"""
webhook_secret.py — Webhook secret validation helper.
Extracted as a standalone module so it can be unit-tested
without importing the full FastAPI app and all its dependencies.
"""
import hmac


def validate_webhook_secret(request_secret, expected_secret):
    """
    Validate the X-Kobo-Webhook-Secret header value.
    Uses hmac.compare_digest for timing-safe comparison.

    Returns True if:
    - Both secrets match
    - No expected_secret is configured (open endpoint)

    Returns False if:
    - request_secret is None or empty
    - Secrets do not match
    """
    if not expected_secret:
        return True
    if not request_secret:
        return False
    return hmac.compare_digest(
        str(request_secret).encode(),
        str(expected_secret).encode(),
    )
