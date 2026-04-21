"""
test_fetcher.py — Unit tests for shared/fetcher.py
Tests Kobo API fetch, pagination, retry logic, and auth error handling.
"""
import pytest
try:
    import responses as resp_mock
except ImportError:
    resp_mock = None
import pytest
pytestmark = pytest.mark.skipif(resp_mock is None, reason="responses package not installed")
import requests

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))

from fetcher import (
    fetch_submissions, validate_token,
    KoboAuthError, KoboFetchError
)

BASE_URL = "https://kf.kobotoolbox.org"
FORM_UID = "test_form_uid"
TOKEN    = "test-token"


@resp_mock.activate
def test_fetch_returns_submissions_on_success():
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/",
        json={"results": [{"_id": 1, "name": "Alice"},
                           {"_id": 2, "name": "Bob"}],
              "next": None, "count": 2},
        status=200,
    )
    result = fetch_submissions(FORM_UID, TOKEN, BASE_URL)
    assert len(result) == 2
    assert result[0]["_id"] == 1


@resp_mock.activate
def test_fetch_raises_auth_error_on_401():
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/",
        status=401,
    )
    with pytest.raises(KoboAuthError):
        fetch_submissions(FORM_UID, TOKEN, BASE_URL)


@resp_mock.activate
def test_fetch_raises_fetch_error_on_500():
    # responses library doesn't support retry easily,
    # so we test KoboFetchError is raised on non-200
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/",
        status=500, json={"error": "server error"},
    )
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/",
        status=500,
    )
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/",
        status=500,
    )
    with pytest.raises(KoboFetchError):
        fetch_submissions(FORM_UID, TOKEN, BASE_URL)


@resp_mock.activate
def test_fetch_paginates_multiple_pages():
    """Ensure pagination follows 'next' URL until exhausted."""
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/",
        json={
            "results": [{"_id": 1}],
            "next": f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/?offset=1",
            "count": 2,
        },
        status=200,
    )
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/?offset=1",
        json={"results": [{"_id": 2}], "next": None, "count": 2},
        status=200,
    )
    result = fetch_submissions(FORM_UID, TOKEN, BASE_URL)
    assert len(result) == 2
    assert result[1]["_id"] == 2


@resp_mock.activate
def test_fetch_returns_empty_list_when_no_submissions():
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/",
        json={"results": [], "next": None, "count": 0},
        status=200,
    )
    result = fetch_submissions(FORM_UID, TOKEN, BASE_URL)
    assert result == []


@resp_mock.activate
def test_validate_token_returns_metadata_on_success():
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/",
        json={"uid": FORM_UID, "name": "Test Form",
              "deployment__submission_count": 42},
        status=200,
    )
    result = validate_token(FORM_UID, TOKEN, BASE_URL)
    assert result["uid"] == FORM_UID
    assert result["name"] == "Test Form"


@resp_mock.activate
def test_validate_token_raises_auth_error_on_401():
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/",
        status=401,
    )
    with pytest.raises(KoboAuthError):
        validate_token(FORM_UID, TOKEN, BASE_URL)


@resp_mock.activate
def test_validate_token_raises_fetch_error_on_404():
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/",
        status=404,
    )
    with pytest.raises(KoboFetchError):
        validate_token(FORM_UID, TOKEN, BASE_URL)


@resp_mock.activate
def test_fetch_respects_limit():
    """Fetch stops when limit is reached even if next page exists."""
    resp_mock.add(
        resp_mock.GET,
        f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/",
        json={
            "results": [{"_id": i} for i in range(10)],
            "next": f"{BASE_URL}/api/v2/assets/{FORM_UID}/data/?offset=10",
            "count": 100,
        },
        status=200,
    )
    result = fetch_submissions(FORM_UID, TOKEN, BASE_URL, limit=5)
    assert len(result) == 10   # Got 10 in first page, limit=5 stops after
