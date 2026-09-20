"""
Shared scraping helpers used by historical_sync.py and daily_sync.py.

Wraps the untouched scraper.py with:
  - session-expiry detection + one fresh-session retry for the CURRENT date
  - bounded retry with exponential backoff for transient failures
  - post-scrape validation before anything is treated as trustworthy

None of this changes scraper.py's own behavior (its internal one-retry
HTTP handling for 5xx-with-usable-body is untouched and still runs
underneath every call here).
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import scraper
import config
from exceptions import LoginError, ScraperError, ValidationError

logger = logging.getLogger("scce.sync_common")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | sync | %(message)s")
    )
    logger.addHandler(_handler)
logger.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))


# ------------------------------------------------------------------
# Authentication
# ------------------------------------------------------------------

def authenticate(hall_ticket: str):
    try:
        return scraper.create_authenticated_session(hall_ticket)
    except scraper.ScraperError as exc:
        raise LoginError(str(exc)) from exc


# ------------------------------------------------------------------
# Session-expiry detection
# ------------------------------------------------------------------

_EXPIRY_HINTS = (
    "date selector",
    "could not be opened",
    "login",
    "not offered by the scce date selector",
)


def _looks_like_expired_session(message: str) -> bool:
    lowered = message.lower()
    return any(hint in lowered for hint in _EXPIRY_HINTS)


def _scrape_date_once(session, hall_ticket: str, scrape_date: str):
    """
    One attempt at one date. If the failure looks like an expired
    session, transparently authenticates a NEW session and retries the
    same date once on that new session -- the date is never marked
    complete or skipped because of this, and the caller gets the new
    session back to keep using for subsequent dates.

    Returns (result_dict, session_to_use_going_forward).
    """
    try:
        result = scraper.scrape_daily_attendance_with_session(session, scrape_date)
        return result, session
    except scraper.ScraperError as exc:
        if not _looks_like_expired_session(str(exc)):
            raise ScraperError(str(exc)) from exc

        logger.warning(
            "Session appears expired mid-job, re-authenticating | hall=%s date=%s",
            hall_ticket, scrape_date,
        )
        try:
            session.close()
        except Exception:
            pass

        new_session = authenticate(hall_ticket)
        try:
            result = scraper.scrape_daily_attendance_with_session(new_session, scrape_date)
            return result, new_session
        except scraper.ScraperError as exc2:
            raise ScraperError(str(exc2)) from exc2


# ------------------------------------------------------------------
# Validation
# ------------------------------------------------------------------

def validate_scraped_result(result: dict, expected_date: str) -> dict:
    """
    A second, independent check on top of scraper.py's own internal
    checks -- belt and suspenders. Raising ValidationError here means
    the response was structurally parseable but not trustworthy;
    callers must not overwrite existing good data with it.
    """
    if result.get("date") != expected_date:
        raise ValidationError(
            f"Date mismatch: requested {expected_date}, scraper returned {result.get('date')}"
        )

    records = result.get("records")
    if not records:
        raise ValidationError(f"No attendance records for {expected_date}")

    attended = result.get("attended")
    conducted = result.get("conducted")
    if attended is None or conducted is None:
        raise ValidationError(f"Missing attended/conducted for {expected_date}")
    if attended > conducted:
        raise ValidationError(
            f"attended ({attended}) exceeds conducted ({conducted}) for {expected_date}"
        )
    if conducted <= 0:
        raise ValidationError(f"conducted is zero for {expected_date}")

    percentage = result.get("percentage")
    if percentage is None or not (0 <= percentage <= 100.01):
        raise ValidationError(f"percentage out of bounds for {expected_date}: {percentage}")

    return result


# ------------------------------------------------------------------
# Retry with exponential backoff
# ------------------------------------------------------------------

def scrape_date_with_retry(session, hall_ticket: str, scrape_date: str, max_attempts: Optional[int] = None):
    """
    Scrape one date with session-expiry recovery AND bounded retry with
    exponential backoff for transient failures (timeouts, connection
    resets, HTTP 5xx that scraper.py's own single retry didn't clear).

    Re-scraping the same date is always safe (Firestore save is a full
    overwrite on a deterministic doc ID), so retrying here never risks
    duplicating data -- it only risks wasted requests, which is why
    attempts are bounded and back off exponentially rather than
    hammering SCCE.

    Returns (result_dict, session_to_use_going_forward).
    Raises ScraperError / ValidationError / LoginError if every attempt fails.
    """
    max_attempts = max_attempts or config.RETRY_MAX_ATTEMPTS
    last_exc: Exception = ScraperError(f"No attempts made for {scrape_date}")

    for attempt in range(1, max_attempts + 1):
        try:
            result, session = _scrape_date_once(session, hall_ticket, scrape_date)
            validate_scraped_result(result, scrape_date)
            return result, session
        except (ScraperError, ValidationError, LoginError) as exc:
            last_exc = exc
            if attempt < max_attempts:
                delay = config.RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "RETRY hall=%s date=%s attempt=%d/%d delay=%ss error=%s",
                    hall_ticket, scrape_date, attempt, max_attempts, delay, exc,
                )
                time.sleep(delay)

    raise last_exc