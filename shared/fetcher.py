"""
fetcher.py — Kobo API fetch with pagination, retry, auth error handling.
Raises KoboAuthError on 401 (never retried).
Retries on connection/timeout errors up to 3 times.
"""
import logging
import requests
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, before_sleep_log,
)

logger = logging.getLogger(__name__)


class KoboAuthError(Exception):
    """401 from Kobo — bad/expired token. Never retry."""
    pass


class KoboFetchError(Exception):
    """Non-auth fetch failure after all retries exhausted."""
    pass


@retry(
    retry=retry_if_exception_type(
        (requests.exceptions.ConnectionError,
         requests.exceptions.Timeout, KoboFetchError)
    ),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def _fetch_page(url, headers, params=None):
    """Fetch one page from Kobo API — retried on transient errors."""
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code == 401:
        raise KoboAuthError(
            "Kobo returned 401. Check KOBO_TOKEN in Secret Manager. "
            "This error is CRITICAL and will not be retried."
        )
    if resp.status_code != 200:
        raise KoboFetchError(
            f"Kobo returned HTTP {resp.status_code}: {resp.text[:200]}"
        )
    return resp.json()


def fetch_submissions(form_uid, token, base_url, limit=30000):
    """
    Fetch all submissions for a form with automatic pagination.
    Returns list of raw submission dicts as returned by Kobo.
    """
    headers     = {"Authorization": f"Token {token}"}
    url         = f"{base_url}/api/v2/assets/{form_uid}/data/"
    params      = {"format": "json", "limit": min(limit, 30000)}
    all_results = []
    page        = 1

    logger.info(f"Fetching submissions for form {form_uid}")

    while url:
        logger.info(f"  Fetching page {page}...")
        data    = _fetch_page(url, headers, params if page == 1 else None)
        results = data.get("results", [])
        all_results.extend(results)
        logger.info(f"  Page {page}: {len(results)} rows "
                    f"(total: {len(all_results)})")
        if len(all_results) >= limit:
            logger.warning(f"Reached fetch limit {limit} — stopping")
            break
        url    = data.get("next")
        params = None
        page  += 1

    logger.info(f"Fetch complete: {len(all_results)} submissions")
    return all_results


def validate_token(form_uid, token, base_url):
    """Validate token and form UID. Returns form metadata on success."""
    headers = {"Authorization": f"Token {token}"}
    resp    = requests.get(
        f"{base_url}/api/v2/assets/{form_uid}/", headers=headers, timeout=15
    )
    if resp.status_code == 401:
        raise KoboAuthError("Invalid KOBO_TOKEN")
    if resp.status_code == 404:
        raise KoboFetchError(f"Form UID '{form_uid}' not found")
    if resp.status_code != 200:
        raise KoboFetchError(f"Unexpected HTTP {resp.status_code}")
    return resp.json()
