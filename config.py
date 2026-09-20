"""
Configuration loaded from environment variables.

Locally: python-dotenv loads .env (never committed -- see .gitignore).
On GitHub Actions: the workflow injects these as env vars from
repository Secrets, no .env file involved at all.
"""

from __future__ import annotations

import os
import socket
import uuid

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    value = os.environ.get(name)
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


# ------------------------------------------------------------------
# Firebase
# ------------------------------------------------------------------

# Path to the service-account JSON. Locally this points at a file on
# disk (kept out of git via .gitignore). On GitHub Actions the workflow
# writes the secret to a temp file at this same path before running.
FIREBASE_CREDENTIALS_PATH = os.environ.get(
    "FIREBASE_CREDENTIALS_PATH", "firebase-service-account.json"
)

FIRESTORE_PROJECT_ID = os.environ.get("FIRESTORE_PROJECT_ID", "")

# ------------------------------------------------------------------
# Worker identity
# ------------------------------------------------------------------

def _default_worker_id() -> str:
    run_id = os.environ.get("GITHUB_RUN_ID")
    if run_id:
        return f"github-actions-{run_id}"
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"


WORKER_ID = os.environ.get("WORKER_ID") or _default_worker_id()

# ------------------------------------------------------------------
# Job queue tuning
# ------------------------------------------------------------------

# A PROCESSING job whose updatedAt is older than this is considered
# abandoned by a crashed worker and becomes claimable again.
STALE_JOB_TIMEOUT_SECONDS = _int("STALE_JOB_TIMEOUT_SECONDS", 15 * 60)

MAX_JOB_ATTEMPTS = _int("MAX_JOB_ATTEMPTS", 5)

# Retry backoff for transient SCCE/Firestore errors within one job run.
RETRY_BASE_DELAY_SECONDS = _int("RETRY_BASE_DELAY_SECONDS", 3)
RETRY_MAX_ATTEMPTS = _int("RETRY_MAX_ATTEMPTS", 3)

# ------------------------------------------------------------------
# Scheduling (informational -- actual cron lives in the GitHub workflow;
# these are read by the workers to decide "is it my turn yet" if ever
# run in a loop, and to log the intended local time for clarity)
# ------------------------------------------------------------------

DAILY_SYNC_HOUR_IST = _int("DAILY_SYNC_HOUR_IST", 18)
DAILY_SYNC_MINUTE_IST = _int("DAILY_SYNC_MINUTE_IST", 30)

INIT_SYNC_HOUR_IST = _int("INIT_SYNC_HOUR_IST", 0)
INIT_SYNC_MINUTE_IST = _int("INIT_SYNC_MINUTE_IST", 0)

# ------------------------------------------------------------------
# Misc
# ------------------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")