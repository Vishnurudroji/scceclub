#!/usr/bin/env python3
"""
SCCE Attendance Recovery / Update Worker

Purpose
-------
This is a SEPARATE worker for students whose attendance history may have
stopped at some point.

It does NOT replace or modify:
    - worker.py
    - historical_sync.py
    - daily_sync.py
    - firestore_repo.py
    - sync_common.py
    - scraper.py

It reuses the existing modules and Firestore schema.

Behavior
--------
For each student:

1. Read the attendance dates already stored in Firestore.
2. Start checking from RECOVERY_START_DATE.
3. Check every calendar date through today.
4. Already stored dates are skipped -- no SCCE request.
5. Missing dates are attempted with the existing session/retry logic.
6. A date that SCCE genuinely does not offer is skipped.
7. Temporary scrape failures are remembered.
8. After the first pass, failed dates are retried once more.
9. If a date is still missing, it is reported as failed for this run.
10. No Firestore schema or existing job is changed.

Important:
Today's "no college" situation does not stop recovery. The worker does
not use today's availability as the gate for historical recovery. It tests
the recovery dates individually.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import config
import firestore_repo
import scraper
import sync_common

from exceptions import (
    FirebaseError,
    LoginError,
    ScraperError,
    ValidationError,
)


IST = ZoneInfo("Asia/Kolkata")

# Recovery starts here for the current backlog.
# Change this one value later if the recovery window needs to move.
RECOVERY_START_DATE = "2026-09-25"


logger = logging.getLogger("scce.recovery_worker")

if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | recovery | %(message)s"
        )
    )
    logger.addHandler(_handler)

logger.setLevel(
    getattr(config, "LOG_LEVEL", logging.INFO)
)


# ============================================================
# DATE HELPERS
# ============================================================

def get_ist_today() -> str:
    """Return today's date in IST as YYYY-MM-DD."""
    return datetime.now(IST).date().isoformat()


def _parse_iso(value: str) -> date:
    return date.fromisoformat(value)


def _date_range(start_date: str, end_date: str) -> list[str]:
    """
    Return every calendar date from start_date through end_date inclusive.
    """
    start = _parse_iso(start_date)
    end = _parse_iso(end_date)

    if start > end:
        return []

    result = []
    current = start

    while current <= end:
        result.append(current.isoformat())
        current += timedelta(days=1)

    return result


def _normalize(hall_ticket: str) -> str:
    return (hall_ticket or "").strip().upper()


# ============================================================
# STUDENT SELECTION
# ============================================================

def get_active_students() -> list[dict]:
    """
    Reuse the existing Firestore student list.

    firestore_repo.list_students() already returns ACTIVE students.
    """
    return firestore_repo.list_students()


# ============================================================
# ONE DATE
# ============================================================

def _scrape_and_save_date(
    hall_ticket: str,
    scrape_date: str,
    session,
):
    """
    Scrape one date using the existing sync_common retry/session logic.

    Returns:
        updated session

    Returns the same session unless sync_common replaces it after a
    session-expiry recovery.
    """

    logger.info(
        "SCRAPE_START hall=%s date=%s",
        hall_ticket,
        scrape_date,
    )

    result, session = sync_common.scrape_date_with_retry(
        session,
        hall_ticket,
        scrape_date,
    )

    # Existing repository function. No schema changes.
    firestore_repo.save_attendance_date(
        hall_ticket,
        result,
    )

    logger.info(
        "SCRAPE_SAVED hall=%s date=%s attended=%s conducted=%s",
        hall_ticket,
        scrape_date,
        result.get("attended"),
        result.get("conducted"),
    )

    return session


def _date_is_offered(
    session,
    scrape_date: str,
) -> bool:
    """
    Check the existing SCCE date selector.

    This is deliberately per-date and per-student.

    A missing date is a normal condition here (holiday/no class), not a
    system failure.
    """
    available_dates = scraper.get_available_dates_from_session(session)
    return scrape_date in available_dates


# ============================================================
# ONE STUDENT
# ============================================================

def recover_student(
    hall_ticket: str,
    start_date: str = RECOVERY_START_DATE,
    end_date: str | None = None,
) -> dict:
    """
    Recover missing attendance for one student.

    The existing attendance collection is the source of truth.
    No job document is created or modified.
    """

    hall_ticket = _normalize(hall_ticket)
    end_date = end_date or get_ist_today()

    if not hall_ticket:
        return {
            "hallTicket": hall_ticket,
            "status": "FAILED",
            "error": "Student hall ticket is required",
        }

    if _parse_iso(start_date) > _parse_iso(end_date):
        return {
            "hallTicket": hall_ticket,
            "status": "FAILED",
            "error": f"Invalid recovery range: {start_date} > {end_date}",
        }

    logger.info(
        "RECOVERY_START hall=%s from=%s to=%s",
        hall_ticket,
        start_date,
        end_date,
    )

    session = None
    stored_dates = set()
    attempted_dates = []
    skipped_existing = []
    skipped_not_offered = []
    failed_dates = []
    retry_failed_dates = []
    newly_scraped = 0

    try:
        # --------------------------------------------------------
        # Firestore is checked FIRST.
        # --------------------------------------------------------
        stored_dates = set(
            firestore_repo.list_attendance_dates(hall_ticket)
        )

        recovery_dates = _date_range(start_date, end_date)

        logger.info(
            "RECOVERY_STATE hall=%s stored=%d range=%d",
            hall_ticket,
            len(stored_dates),
            len(recovery_dates),
        )

        if not recovery_dates:
            return {
                "hallTicket": hall_ticket,
                "status": "UP_TO_DATE",
                "processed": 0,
                "newly_scraped": 0,
                "failed_dates": [],
            }

        # --------------------------------------------------------
        # Authenticate ONCE for the student.
        # Existing sync_common handles session expiry and can return
        # a refreshed session.
        # --------------------------------------------------------
        session = sync_common.authenticate(hall_ticket)

        # --------------------------------------------------------
        # FIRST PASS
        # --------------------------------------------------------
        for scrape_date in recovery_dates:

            # Never scrape something already saved.
            if scrape_date in stored_dates:
                skipped_existing.append(scrape_date)

                logger.info(
                    "SKIP_EXISTING hall=%s date=%s",
                    hall_ticket,
                    scrape_date,
                )
                continue

            attempted_dates.append(scrape_date)

            try:
                # ------------------------------------------------
                # Today's holiday/no-class does NOT stop the worker.
                # Check this individual date only.
                # ------------------------------------------------
                try:
                    offered = _date_is_offered(
                        session,
                        scrape_date,
                    )
                except (LoginError, ScraperError) as exc:
                    # If the selector itself fails, let the normal
                    # scrape/retry machinery have a chance. We do not
                    # classify the whole recovery run as "no dates".
                    logger.warning(
                        "DATE_SELECTOR_CHECK_FAILED "
                        "hall=%s date=%s error=%s "
                        "-- attempting scrape",
                        hall_ticket,
                        scrape_date,
                        exc,
                    )
                    offered = True

                if not offered:
                    skipped_not_offered.append(scrape_date)

                    logger.info(
                        "DATE_NOT_OFFERED hall=%s date=%s "
                        "-- skipping",
                        hall_ticket,
                        scrape_date,
                    )
                    continue

                session = _scrape_and_save_date(
                    hall_ticket,
                    scrape_date,
                    session,
                )

                stored_dates.add(scrape_date)
                newly_scraped += 1

            except FirebaseError:
                # Firestore failure is not a date-level scrape failure.
                # Do not continue while persistence is unavailable.
                raise

            except (LoginError, ScraperError, ValidationError) as exc:
                failed_dates.append(scrape_date)

                logger.warning(
                    "DATE_FAILED hall=%s date=%s error=%s",
                    hall_ticket,
                    scrape_date,
                    exc,
                )

                # Continue to the next date.
                continue

        # --------------------------------------------------------
        # SECOND PASS
        # Retry only dates that failed in the first pass.
        # --------------------------------------------------------
        for scrape_date in failed_dates:

            # A previous operation may have saved it despite an error
            # being reported afterward. Never overwrite unnecessarily.
            if scrape_date in stored_dates:
                continue

            logger.info(
                "RETRY_START hall=%s date=%s",
                hall_ticket,
                scrape_date,
            )

            try:
                # Re-check the selector for the retry.
                try:
                    offered = _date_is_offered(
                        session,
                        scrape_date,
                    )
                except (LoginError, ScraperError) as exc:
                    logger.warning(
                        "RETRY_SELECTOR_CHECK_FAILED "
                        "hall=%s date=%s error=%s "
                        "-- attempting scrape",
                        hall_ticket,
                        scrape_date,
                        exc,
                    )
                    offered = True

                if not offered:
                    skipped_not_offered.append(scrape_date)

                    logger.info(
                        "RETRY_DATE_NOT_OFFERED "
                        "hall=%s date=%s -- skipping",
                        hall_ticket,
                        scrape_date,
                    )
                    continue

                session = _scrape_and_save_date(
                    hall_ticket,
                    scrape_date,
                    session,
                )

                stored_dates.add(scrape_date)
                newly_scraped += 1

                logger.info(
                    "RETRY_SUCCESS hall=%s date=%s",
                    hall_ticket,
                    scrape_date,
                )

            except FirebaseError:
                raise

            except (LoginError, ScraperError, ValidationError) as exc:
                retry_failed_dates.append(scrape_date)

                logger.warning(
                    "RETRY_FAILED hall=%s date=%s error=%s",
                    hall_ticket,
                    scrape_date,
                    exc,
                )

        # --------------------------------------------------------
        # FINAL STATUS
        # --------------------------------------------------------
        if retry_failed_dates:
            status = "PARTIAL"
        else:
            status = "UP_TO_DATE"

        logger.info(
            "RECOVERY_COMPLETE hall=%s status=%s "
            "new=%d missing=%d not_offered=%d",
            hall_ticket,
            status,
            newly_scraped,
            len(retry_failed_dates),
            len(skipped_not_offered),
        )

        return {
            "hallTicket": hall_ticket,
            "status": status,
            "start_date": start_date,
            "end_date": end_date,
            "processed": len(attempted_dates),
            "newly_scraped": newly_scraped,
            "skipped_existing": len(skipped_existing),
            "skipped_not_offered": skipped_not_offered,
            "failed_dates": retry_failed_dates,
        }

    except FirebaseError as exc:
        logger.error(
            "RECOVERY_FIRESTORE_FAILED hall=%s error=%s",
            hall_ticket,
            exc,
        )

        return {
            "hallTicket": hall_ticket,
            "status": "FAILED",
            "error": str(exc),
            "newly_scraped": newly_scraped,
            "failed_dates": retry_failed_dates or failed_dates,
        }

    except Exception as exc:
        logger.exception(
            "RECOVERY_UNEXPECTED_FAILED hall=%s",
            hall_ticket,
        )

        return {
            "hallTicket": hall_ticket,
            "status": "FAILED",
            "error": str(exc),
            "newly_scraped": newly_scraped,
            "failed_dates": retry_failed_dates or failed_dates,
        }

    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "SCCE attendance recovery worker. "
            "Does not modify existing jobs or schema."
        )
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--student",
        help="Recover one student by hall ticket",
    )

    group.add_argument(
        "--all",
        action="store_true",
        help="Recover all ACTIVE students",
    )

    parser.add_argument(
        "--from-date",
        default=RECOVERY_START_DATE,
        help=(
            "Recovery start date YYYY-MM-DD "
            f"(default: {RECOVERY_START_DATE})"
        ),
    )

    parser.add_argument(
        "--to-date",
        default=None,
        help="Recovery end date YYYY-MM-DD (default: today in IST)",
    )

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    start_date = args.from_date
    end_date = args.to_date or get_ist_today()

    # Validate before touching Firestore/SCCE.
    try:
        _parse_iso(start_date)
        _parse_iso(end_date)
    except ValueError as exc:
        parser.error(f"Invalid date: {exc}")

    if _parse_iso(start_date) > _parse_iso(end_date):
        parser.error(
            f"from-date {start_date} cannot be after to-date {end_date}"
        )

    logger.info(
        "Recovery worker starting | from=%s to=%s",
        start_date,
        end_date,
    )

    if args.student:
        result = recover_student(
            args.student,
            start_date=start_date,
            end_date=end_date,
        )

        print(result)

        return 0 if result.get("status") in {
            "UP_TO_DATE",
            "PARTIAL",
        } else 1

    # ------------------------------------------------------------
    # ALL ACTIVE STUDENTS
    # ------------------------------------------------------------
    students = get_active_students()

    logger.info(
        "ACTIVE_STUDENTS count=%d",
        len(students),
    )

    if not students:
        print({
            "status": "NO_ACTIVE_STUDENTS",
            "processed": 0,
        })
        return 0

    results = []

    for student in students:
        hall_ticket = _normalize(
            student.get("hallTicket")
        )

        if not hall_ticket:
            logger.warning(
                "SKIP_STUDENT missing hallTicket"
            )
            continue

        result = recover_student(
            hall_ticket,
            start_date=start_date,
            end_date=end_date,
        )

        results.append(result)

    summary = {
        "status": "COMPLETE",
        "from_date": start_date,
        "to_date": end_date,
        "students": len(results),
        "up_to_date": sum(
            1 for r in results
            if r.get("status") == "UP_TO_DATE"
        ),
        "partial": sum(
            1 for r in results
            if r.get("status") == "PARTIAL"
        ),
        "failed": sum(
            1 for r in results
            if r.get("status") == "FAILED"
        ),
        "newly_scraped": sum(
            r.get("newly_scraped", 0)
            for r in results
        ),
        "results": results,
    }

    print(summary)

    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
