"""
form_registry.py — Registry of all KoboToolbox forms handled by this pipeline.

Each entry maps a KoboToolbox Asset UID (form_uid) to its pipeline config:
  - name:        Human-readable form name (used in logs)
  - bq_table:    BigQuery table name for this form's data
  - sheet_id:    Google Sheet ID (created manually, shared with service account)
  - sheet_tab:   Tab name inside the Google Sheet
  - sheet_name:  Google Sheet filename (for logging/reference only)

HOW TO ADD A NEW FORM:
  1. Create a Google Sheet manually in Google Drive
  2. Share it with the service account as Editor
  3. Copy the Sheet ID from the URL
  4. Add a new entry to FORM_REGISTRY below with the form's Asset UID
  5. Add the new Sheet ID and other values to GitHub Secrets
  6. Push to main — GitHub Actions deploys automatically

HOW TO GET A FORM'S ASSET UID:
  Go to KoboToolbox → open the form → look at the browser URL:
  https://kf.kobotoolbox.org/#/forms/ASSET_UID_HERE/summary
                                              ^^^^^^^^^^^
"""
import os

# ── Form Registry ─────────────────────────────────────────────────────────────
# Each key is the KoboToolbox Asset UID (form_uid)
# Each value is a dict with pipeline config for that form

FORM_REGISTRY = {
    # ── ContactUs Form ────────────────────────────────────────────────────────
    os.environ.get("FORM_UID_CONTACT_US", ""): {
        "name":       "ContactUs",
        "bq_table":   os.environ.get("BQ_TABLE_CONTACT_US", "kobo_contact_us"),
        "sheet_id":   os.environ.get("SHEET_ID_CONTACT_US", ""),
        "sheet_tab":  os.environ.get("SHEET_TAB_CONTACT_US", "Contact Us"),
        "sheet_name": os.environ.get("SHEET_NAME_CONTACT_US", "Contact Us Responses"),
    },

    # ── Food Distribution Form ────────────────────────────────────────────────
    os.environ.get("FORM_UID_FOOD_DISTRIBUTION", ""): {
        "name":       "FoodDistribution",
        "bq_table":   os.environ.get("BQ_TABLE_FOOD_DISTRIBUTION", "kobo_food_distribution"),
        "sheet_id":   os.environ.get("SHEET_ID_FOOD_DISTRIBUTION", ""),
        "sheet_tab":  os.environ.get("SHEET_TAB_FOOD_DISTRIBUTION", "Food Distribution"),
        "sheet_name": os.environ.get("SHEET_NAME_FOOD_DISTRIBUTION", "Food Distribution Data"),
    },

    # ── Membership Form ───────────────────────────────────────────────────────
    os.environ.get("FORM_UID_MEMBERSHIP", ""): {
        "name":       "Membership",
        "bq_table":   os.environ.get("BQ_TABLE_MEMBERSHIP", "kobo_membership"),
        "sheet_id":   os.environ.get("SHEET_ID_MEMBERSHIP", ""),
        "sheet_tab":  os.environ.get("SHEET_TAB_MEMBERSHIP", "Membership"),
        "sheet_name": os.environ.get("SHEET_NAME_MEMBERSHIP", "Membership Data"),
    },
}

# Remove entries with empty form_uid (not configured)
FORM_REGISTRY = {k: v for k, v in FORM_REGISTRY.items() if k}


def get_form_config(form_uid):
    """
    Look up pipeline config for a given form_uid.
    Returns the config dict or None if form is not registered.
    """
    return FORM_REGISTRY.get(form_uid)


def get_all_form_uids():
    """Return list of all registered form UIDs."""
    return list(FORM_REGISTRY.keys())


def get_all_forms():
    """Return list of (form_uid, config) tuples for all registered forms."""
    return list(FORM_REGISTRY.items())
