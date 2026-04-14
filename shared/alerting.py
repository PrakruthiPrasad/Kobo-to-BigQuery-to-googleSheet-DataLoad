"""
alerting.py — Pipeline failure detection and notification.
Logs every run to pipeline_runs. Alerts on failure via gspread email.
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def alert_on_failure(spreadsheet, alert_emails, error_msg,
                     form_uid, run_id):
    """
    Send a failure alert email when the pipeline encounters an error.
    Uses the same gspread share mechanism as notifications — no extra setup.
    """
    if not alert_emails or not spreadsheet:
        logger.warning(f"Pipeline failure (no alert recipient): {error_msg}")
        return

    for email in alert_emails:
        try:
            spreadsheet.share(
                email,
                perm_type="user",
                role="reader",
                notify=True,
                email_message=(
                    f"[ALERT] Kobo pipeline failure\n\n"
                    f"Form: {form_uid}\n"
                    f"Run ID: {run_id}\n"
                    f"Time: {datetime.now(timezone.utc).strftime('%d %b %Y %H:%M UTC')}\n\n"
                    f"Error: {error_msg}\n\n"
                    f"Check the pipeline_runs table in BigQuery for details."
                ),
            )
            logger.info(f"Failure alert sent to: {email}")
        except Exception as e:
            logger.warning(f"Could not send failure alert: {e}")
