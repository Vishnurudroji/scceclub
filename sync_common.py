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
        logging.Formatter(
            "%(asctime)s | %(levelname)s | sync | %(message)s"
        )
    )
    logger.addHandler(_handler)

logger.setLevel(
    getattr(config, "LOG_LEVEL", logging.INFO)
)


# ------------------------------------------------------------------
# Authentication
# ------------------------------------------------------------------

def authenticate(hall_ticket: str):
    """
    Create a fresh authenticated SCCE session.

    scraper.py remains responsible for the actual login flow.
    Any scraper-level authentication failure is translated into the
    orchestration-layer LoginError.
    """
    try:
        return scraper.create_authenticated_session(hall_ticket)

    except scraper.ScraperError as exc:
        raise LoginError(str(exc)) from exc


# ------------------------------------------------------------------
# Session-expiry detection
# ------------------------------------------------------------------

# These messages indicate that an already-authenticated session may
# have become invalid while processing a job.
#
# IMPORTANT:
# "is not offered by the scce date selector" (the SPECIFIC business
# message scraper.py raises from scrape_daily_attendance_with_session
# when a date genuinely isn't in this student's selector) is
# intentionally NOT included here -- see _is_date_not_available()
# below, which is always checked first and takes priority over these
# hints. A date missing from the selector is a legitimate per-
# student/date condition, not evidence of session expiry.
#
# The hints below, in contrast, correspond to scraper.py messages that
# ARE genuine "the page didn't load the way we expected, the session
# is probably dead" signals, even though they don't contain the word
# "login" verbatim:
#   - "Dailywise report page could not be opened"        (_open_dailywise)
#   - "Could not find the date selector on the ..."        (_extract_date_options)
#   - "Date selector found but contains no recognisable dates" (_extract_date_options)
#   - "Date selector disappeared from the Dailywise page"  (_build_daily_payload)
# Because _is_date_not_available() is always checked first, matching
# "date selector" here can never misfire on the specific
# "is not offered by the SCCE date selector" message.
_EXPIRY_HINTS = (
    "login",
    "could not be opened",
    "date selector",
)


def _looks_like_expired_session(message: str) -> bool:
    """
    Return True only when the scraper error looks like an
    authentication/session-expiry problem.
    """
    lowered = message.lower()

    return any(
        hint in lowered
        for hint in _EXPIRY_HINTS
    )


def _is_date_not_available(message: str) -> bool:
    """
    Detect SCCE's explicit 'date not offered' condition.

    This means the requested date does not exist in this student's
    Dailywise date selector.

    It is NOT:
      - session expiry
      - authentication failure
      - transient network failure

    Therefore it must not trigger re-authentication or retry.
    """
    lowered = message.lower()

    return (
        "is not offered by the scce date selector"
        in lowered
    )


# ------------------------------------------------------------------
# One scrape attempt
# ------------------------------------------------------------------

def _scrape_date_once(
    session,
    hall_ticket: str,
    scrape_date: str,
):
    """
    Attempt to scrape one date.

    If the currently authenticated session genuinely appears to have
    expired, authenticate a NEW session and retry the SAME date once.

    If SCCE says the requested date is not offered for this student,
    the error is returned as a normal ScraperError without retrying.

    Returns:
        (result_dict, session_to_use_going_forward)

    Raises:
        ScraperError
        LoginError
    """
    try:
        result = scraper.scrape_daily_attendance_with_session(
            session,
            scrape_date,
        )

        return result, session

    except scraper.ScraperError as exc:

        message = str(exc)

        # ----------------------------------------------------------
        # Date is genuinely unavailable for this student.
        #
        # DO NOT:
        #   - close the session
        #   - authenticate again
        #   - retry the same date
        #
        # This check MUST run before the expiry-hint check below,
        # since "date selector" is also one of those hints.
        # ----------------------------------------------------------
        if _is_date_not_available(message):
            raise ScraperError(message) from exc

        # ----------------------------------------------------------
        # Not a session-expiry error.
        #
        # Let the outer retry mechanism decide whether this
        # transient scraper failure should be retried.
        # ----------------------------------------------------------
        if not _looks_like_expired_session(message):
            raise ScraperError(message) from exc

        # ----------------------------------------------------------
        # Session appears to have expired.
        #
        # Close the old session and authenticate a fresh one.
        # ----------------------------------------------------------
        logger.warning(
            "Session appears expired mid-job, re-authenticating | "
            "hall=%s date=%s",
            hall_ticket,
            scrape_date,
        )

        try:
            session.close()
        except Exception:
            pass

        new_session = authenticate(hall_ticket)

        try:
            result = scraper.scrape_daily_attendance_with_session(
                new_session,
                scrape_date,
            )

            return result, new_session

        except scraper.ScraperError as exc2:
            raise ScraperError(str(exc2)) from exc2


# ------------------------------------------------------------------
# Validation
# ------------------------------------------------------------------

def validate_scraped_result(
    result: dict,
    expected_date: str,
) -> dict:
    """
    Perform a second independent validation on top of scraper.py's
    own internal validation.

    A ValidationError means the response was structurally parseable
    but is not trustworthy enough to save.

    Existing stored attendance must never be overwritten because
    of a failed validation.
    """

    # --------------------------------------------------------------
    # Date must match exactly.
    # --------------------------------------------------------------
    if result.get("date") != expected_date:
        raise ValidationError(
            f"Date mismatch: requested {expected_date}, "
            f"scraper returned {result.get('date')}"
        )

    # --------------------------------------------------------------
    # Attendance records must exist.
    # --------------------------------------------------------------
    records = result.get("records")

    if not records:
        raise ValidationError(
            f"No attendance records for {expected_date}"
        )

    # --------------------------------------------------------------
    # Attended/conducted values must exist.
    # --------------------------------------------------------------
    attended = result.get("attended")
    conducted = result.get("conducted")

    if attended is None or conducted is None:
        raise ValidationError(
            f"Missing attended/conducted for {expected_date}"
        )

    # --------------------------------------------------------------
    # Attended cannot exceed conducted.
    # --------------------------------------------------------------
    if attended > conducted:
        raise ValidationError(
            f"attended ({attended}) exceeds conducted ({conducted}) "
            f"for {expected_date}"
        )

    # --------------------------------------------------------------
    # There must be at least one conducted class.
    # --------------------------------------------------------------
    if conducted <= 0:
        raise ValidationError(
            f"conducted is zero for {expected_date}"
        )

    # --------------------------------------------------------------
    # Percentage must be sane.
    # --------------------------------------------------------------
    percentage = result.get("percentage")

    if percentage is None or not (0 <= percentage <= 100.01):
        raise ValidationError(
            f"percentage out of bounds for "
            f"{expected_date}: {percentage}"
        )

    return result


# ------------------------------------------------------------------
# Retry with exponential backoff
# ------------------------------------------------------------------

def scrape_date_with_retry(
    session,
    hall_ticket: str,
    scrape_date: str,
    max_attempts: Optional[int] = None,
):
    """
    Scrape one date with:

      1. session-expiry recovery
      2. bounded retry
      3. exponential backoff
      4. post-scrape validation

    Important behavior:

      Date not offered by SCCE
          -> NO re-authentication
          -> NO retry

      Session actually expired
          -> fresh authentication
          -> retry current date

      Transient scraper failure
          -> bounded retry
          -> exponential backoff

    Re-scraping the same date is safe because attendance is stored
    using a deterministic Firestore document ID.

    Returns:
        (result_dict, session_to_use_going_forward)

    Raises:
        ScraperError
        ValidationError
        LoginError
    """

    max_attempts = (
        max_attempts
        or config.RETRY_MAX_ATTEMPTS
    )

    last_exc: Exception = ScraperError(
        f"No attempts made for {scrape_date}"
    )

    for attempt in range(
        1,
        max_attempts + 1,
    ):
        try:

            result, session = _scrape_date_once(
                session,
                hall_ticket,
                scrape_date,
            )

            validate_scraped_result(
                result,
                scrape_date,
            )

            return result, session

        except (
            ScraperError,
            ValidationError,
            LoginError,
        ) as exc:

            last_exc = exc

            # ------------------------------------------------------
            # SCCE explicitly says this date is not offered.
            #
            # This is deterministic. Retrying cannot make the date
            # appear, and re-authentication cannot help.
            # ------------------------------------------------------
            if (
                isinstance(exc, ScraperError)
                and _is_date_not_available(str(exc))
            ):
                logger.info(
                    "DATE_NOT_AVAILABLE "
                    "hall=%s date=%s -- no retry",
                    hall_ticket,
                    scrape_date,
                )

                raise

            # ------------------------------------------------------
            # Retry only if attempts remain.
            # ------------------------------------------------------
            if attempt < max_attempts:

                delay = (
                    config.RETRY_BASE_DELAY_SECONDS
                    * (2 ** (attempt - 1))
                )

                logger.warning(
                    "RETRY hall=%s date=%s "
                    "attempt=%d/%d delay=%ss error=%s",
                    hall_ticket,
                    scrape_date,
                    attempt,
                    max_attempts,
                    delay,
                    exc,
                )

                time.sleep(delay)

    # --------------------------------------------------------------
    # All attempts exhausted.
    # --------------------------------------------------------------
    raise last_exc