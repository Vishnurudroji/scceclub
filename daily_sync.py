"""
Daily sync: scrape attendance for an existing student for one date.

Flow:
    1. Create deterministic DAILY_SYNC job.
    2. Claim the job.
    3. Check Firestore attendance FIRST.
    4. If already scraped -> skip immediately.
    5. If not scraped -> contact SCCE and scrape.
    6. Save attendance.
    7. Mark job SUCCESS.

Important:
    Firestore is the source of truth.

    If attendance/{date} already exists for a student,
    SCCE is NOT contacted again for that student/date.
"""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import config
import firestore_repo
import scraper
import sync_common

from exceptions import (
    JobClaimError,
    LoginError,
    ScraperError,
    ValidationError,
    FirebaseError,
)


IST = ZoneInfo("Asia/Kolkata")


logger = logging.getLogger("scce.daily_sync")

if not logger.handlers:
    _handler = logging.StreamHandler()

    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | daily | %(message)s"
        )
    )

    logger.addHandler(_handler)

logger.setLevel(
    getattr(config, "LOG_LEVEL", "INFO")
)


# ============================================================
# DATE
# ============================================================

def get_ist_today() -> str:
    """
    Return today's date in India Standard Time.
    """

    return datetime.now(IST).date().isoformat()


# ============================================================
# DATE AVAILABILITY PROBE
# ============================================================

def check_date_available(
    hall_ticket: str,
    target_date: str,
) -> bool:
    """
    Check whether SCCE offers target_date.

    This is only a lightweight date-availability probe.

    Returns:
        True  -> date exists in SCCE selector
        False -> date is not offered
    """

    hall_ticket = hall_ticket.strip().upper()

    session = None

    try:

        logger.info(
            "DATE_PROBE_START hall=%s date=%s",
            hall_ticket,
            target_date,
        )

        session = sync_common.authenticate(
            hall_ticket
        )

        available_dates = (
            scraper.get_available_dates_from_session(
                session
            )
        )

        available = target_date in available_dates

        if available:

            logger.info(
                "DATE_AVAILABLE hall=%s date=%s",
                hall_ticket,
                target_date,
            )

        else:

            logger.info(
                "DATE_NOT_AVAILABLE hall=%s date=%s",
                hall_ticket,
                target_date,
            )

        return available

    finally:

        if session is not None:

            try:
                session.close()

            except Exception:
                pass


# ============================================================
# DAILY SYNC
# ============================================================

def run_daily_sync(
    hall_ticket: str,
    target_date: str = None,
    worker_id: str = None,
) -> dict:
    """
    Scrape one student's attendance for one date.

    Important idempotency behavior:

        Firestore attendance exists
                ↓
        DO NOT CONTACT SCCE
                ↓
        Finish job SUCCESS
                ↓
        Move to next queue job

    This prevents already-scraped students from being scraped again.
    """

    worker_id = worker_id or config.WORKER_ID

    hall_ticket = hall_ticket.strip().upper()

    target_date = target_date or get_ist_today()

    # --------------------------------------------------------
    # Deterministic job ID
    # --------------------------------------------------------

    job_id = firestore_repo.daily_sync_job_id(
        hall_ticket,
        target_date,
    )

    logger.info(
        "DAILY_START hall=%s date=%s job=%s",
        hall_ticket,
        target_date,
        job_id,
    )

    # --------------------------------------------------------
    # Create job if it does not already exist.
    #
    # create_job() should be idempotent.
    # --------------------------------------------------------

    firestore_repo.create_job(
        job_id,
        "DAILY_SYNC",
        hall_ticket,
        total_count=1,
        extra_fields={
            "date": target_date,
        },
    )

    # --------------------------------------------------------
    # Claim job
    # --------------------------------------------------------

    try:

        firestore_repo.claim_job(
            job_id,
            worker_id,
        )

    except JobClaimError as exc:

        logger.info(
            "CLAIM_SKIPPED job=%s reason=%s",
            job_id,
            exc,
        )

        return {
            "claimed": False,
            "job_id": job_id,
            "hallTicket": hall_ticket,
            "date": target_date,
            "reason": str(exc),
        }

    logger.info(
        "JOB_CLAIMED job=%s hall=%s date=%s worker=%s",
        job_id,
        hall_ticket,
        target_date,
        worker_id,
    )

    # ========================================================
    # CRITICAL CHECK
    #
    # Check Firestore BEFORE SCCE.
    #
    # If attendance already exists:
    #
    #     NO LOGIN
    #     NO HTTP REQUEST
    #     NO SCRAPER
    #     NO RETRY
    #
    # Just finish this job and continue to next student.
    # ========================================================

    try:

        existing_attendance = (
            firestore_repo.get_attendance_date(
                hall_ticket,
                target_date,
            )
        )

    except FirebaseError as exc:

        logger.warning(
            "FIRESTORE_CHECK_FAILED "
            "job=%s hall=%s date=%s error=%s",
            job_id,
            hall_ticket,
            target_date,
            exc,
        )

        firestore_repo.finish_job(
            job_id,
            "FAILED",
            error=str(exc),
        )

        return {
            "claimed": True,
            "job_id": job_id,
            "hallTicket": hall_ticket,
            "date": target_date,
            "status": "FAILED",
            "error": str(exc),
        }

    # ========================================================
    # ALREADY SCRAPED
    # ========================================================

    if existing_attendance is not None:

        logger.info(
            "DAILY_SKIP_ALREADY_SCRAPED "
            "hall=%s date=%s job=%s",
            hall_ticket,
            target_date,
            job_id,
        )

        # Update checkpoint so Firestore clearly shows
        # this job has completed this date.

        try:

            firestore_repo.update_job_checkpoint(
                job_id,
                last_completed_date=target_date,
                current_date=target_date,
                completed_count=1,
            )

            firestore_repo.finish_job(
                job_id,
                "SUCCESS",
            )

        except FirebaseError as exc:

            logger.warning(
                "SKIP_FINALIZE_FAILED "
                "job=%s error=%s",
                job_id,
                exc,
            )

            return {
                "claimed": True,
                "job_id": job_id,
                "hallTicket": hall_ticket,
                "date": target_date,
                "status": "FAILED",
                "skipped": True,
                "error": str(exc),
            }

        return {
            "claimed": True,
            "job_id": job_id,
            "hallTicket": hall_ticket,
            "date": target_date,
            "status": "SUCCESS",
            "skipped": True,
            "reason": "ALREADY_SCRAPED",
        }

    # ========================================================
    # ATTENDANCE DOES NOT EXIST
    #
    # Now we are allowed to contact SCCE.
    # ========================================================

    logger.info(
        "DAILY_ATTENDANCE_MISSING "
        "hall=%s date=%s -> scraping SCCE",
        hall_ticket,
        target_date,
    )

    session = None

    try:

        # ----------------------------------------------------
        # Authenticate
        # ----------------------------------------------------

        session = sync_common.authenticate(
            hall_ticket
        )

        logger.info(
            "SCCE_AUTHENTICATED hall=%s date=%s",
            hall_ticket,
            target_date,
        )

        # ----------------------------------------------------
        # Scrape requested date
        #
        # Handles:
        # - retryable errors
        # - session expiration
        # - re-authentication
        # ----------------------------------------------------

        result, session = (
            sync_common.scrape_date_with_retry(
                session,
                hall_ticket,
                target_date,
            )
        )

        logger.info(
            "SCRAPE_SUCCESS "
            "hall=%s date=%s records=%d",
            hall_ticket,
            target_date,
            len(result["records"]),
        )

        # ----------------------------------------------------
        # Save attendance immediately
        # ----------------------------------------------------

        firestore_repo.save_attendance_date(
            hall_ticket,
            result,
        )

        logger.info(
            "ATTENDANCE_SAVED "
            "hall=%s date=%s",
            hall_ticket,
            target_date,
        )

        # ----------------------------------------------------
        # Update student checkpoint
        # ----------------------------------------------------

        firestore_repo.update_student_checkpoint(
            hall_ticket,
            last_scraped_date=target_date,
        )

        # ----------------------------------------------------
        # Update job checkpoint
        # ----------------------------------------------------

        firestore_repo.update_job_checkpoint(
            job_id,
            last_completed_date=target_date,
            current_date=target_date,
            completed_count=1,
        )

        # ----------------------------------------------------
        # Finish SUCCESS
        # ----------------------------------------------------

        firestore_repo.finish_job(
            job_id,
            "SUCCESS",
        )

        logger.info(
            "OPERATION_SUCCESS "
            "job=%s hall=%s date=%s",
            job_id,
            hall_ticket,
            target_date,
        )

        return {
            "claimed": True,
            "job_id": job_id,
            "hallTicket": hall_ticket,
            "status": "SUCCESS",
            "date": target_date,
            "skipped": False,
        }

    # ========================================================
    # EXPECTED FAILURES
    # ========================================================

    

    finally:

        if session is not None:

            try:
                session.close()

            except Exception:
                pass


# ============================================================
# ENQUEUE DAILY JOBS
# ============================================================

def enqueue_daily_jobs_for_all_students(
    target_date: str = None,
) -> dict:
    """
    Create DAILY_SYNC jobs for all ACTIVE students.

    Important:
        This function ONLY creates jobs.

        It does NOT scrape SCCE.

    The worker queue later processes those jobs.
    """

    target_date = target_date or get_ist_today()

    students = firestore_repo.list_students()

    active_students = [
        student
        for student in students
        if student.get("status", "ACTIVE") == "ACTIVE"
    ]

    created = 0
    existing = 0

    for student in active_students:

        hall_ticket = (
            student["hallTicket"]
            .strip()
            .upper()
        )

        job_id = firestore_repo.daily_sync_job_id(
            hall_ticket,
            target_date,
        )

        was_created = firestore_repo.create_job(
            job_id,
            "DAILY_SYNC",
            hall_ticket,
            total_count=1,
            extra_fields={
                "date": target_date,
            },
        )

        if was_created:

            created += 1

            logger.info(
                "DAILY_JOB_CREATED "
                "hall=%s date=%s job=%s",
                hall_ticket,
                target_date,
                job_id,
            )

        else:

            existing += 1

            logger.info(
                "DAILY_JOB_EXISTS "
                "hall=%s date=%s job=%s",
                hall_ticket,
                target_date,
                job_id,
            )

    logger.info(
        "ENQUEUE_DAILY "
        "date=%s students=%d "
        "jobs_created=%d existing=%d",
        target_date,
        len(active_students),
        created,
        existing,
    )

    return {
        "date": target_date,
        "students": len(active_students),
        "jobs_created": created,
        "jobs_existing": existing,
    }