"""
email.py — Transactional email via Resend
"""
import os
import logging
import httpx

logger = logging.getLogger(__name__)

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
FROM_ADDRESS = "Helix <notifications@millyweb.com>"
RESEND_URL = "https://api.resend.com/emails"


def send_email(to: str, subject: str, body: str, html: bool = False) -> bool:
    """Send transactional email via Resend. Returns True on success."""
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY not set — skipping email")
        return False
    payload = {
        "from": FROM_ADDRESS,
        "to": [to],
        "subject": subject,
    }
    if html:
        payload["html"] = body
    else:
        payload["text"] = body
    try:
        r = httpx.post(
            RESEND_URL,
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json=payload,
            timeout=10,
        )
        if r.status_code in (200, 201):
            logger.info(f"email sent to={to} subject={subject!r}")
            return True
        logger.warning(f"resend {r.status_code}: {r.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"email send failed: {e}")
        return False


def send_invite(to: str, name: str, slug: str, setup_url: str) -> bool:
    subject = f"Your Helix workspace is ready ⧠"
    body = f"""Hi {name},

Your Helix workspace is set up and ready.

Open your dashboard (no password needed — link logs you straight in):
{setup_url}

From the dashboard you can track usage and connect Claude Desktop.
To connect your first machine, the dashboard will walk you through it.

Reach out if you have any questions.

— Ryan"""
    return send_email(to, subject, body)
