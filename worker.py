#!/usr/bin/env python3

"""
SCCE Attendance Worker CLI.

Commands:

    python worker.py register --student 23N01A0596

        Create the student and INITIAL_SYNC job.
        Does not contact SCCE.


    python worker.py historical --student 23N01A0596

        Process the student's historical attendance.


    python worker.py daily --student 23N01A0596 --date 2026-09-20

        Process one student's attendance for one date.


    python worker.py enqueue-daily --date 2026-09-20

        Find all ACTIVE registered students.

        1. If no students -> STOP.
        2. Check date availability using ONE student.
        3. If unavailable -> store NOT_AVAILABLE and STOP.
        4. If available -> create DAILY_SYNC jobs for all students.


    python worker.py queue --type DAILY_SYNC --limit 100

        Process eligible jobs sequentially.

        DAILY_SYNC:
            - NOT_AVAILABLE date -> skip
            - Already scraped -> skip through daily_sync
            - Not scraped -> scrape

        INITIAL_SYNC:
            - historical attendance sync
"""


from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import config
import firestore_repo
import daily_sync
import historical_sync


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger("scce.worker")

if not logger.handlers:

    _handler = logging.StreamHandler()

    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | worker | %(message)s"
        )
    )

    logger.addHandler(_handler)


logger.setLevel(
    getattr(
        logging,
        config.LOG_LEVEL,
        logging.INFO,
    )
)


# ============================================================
# TIMEZONE
# ============================================================

IST = ZoneInfo("Asia/Kolkata")


def get_ist_today() -> str:
    """
    Return today's date in IST.
    """

    return datetime.now(IST).date().isoformat()


# ============================================================
# HELPERS
# ============================================================

def _normalize(hall_ticket: str) -> str:
    """
    Normalize hall ticket.
    """

    return (
        hall_ticket or ""
    ).strip().upper()


def get_active_students() -> list[dict]:
    """
    Return all ACTIVE registered students.
    """

    students = firestore_repo.list_students()

    return [
        student
        for student in students
        if student.get("status", "ACTIVE") == "ACTIVE"
    ]


# ============================================================
# REGISTER
# ============================================================

def cmd_register(args) -> dict:
    """
    Register a student.

    Registration itself does NOT contact SCCE.

    It creates:

        students/{hallTicket}

    and:

        initial_{hallTicket}
    """

    hall_ticket = _normalize(args.student)

    if not hall_ticket:

        return {
            "status": "FAILED",
            "error": "Student hall ticket is required",
        }

    logger.info(
        "REGISTER student=%s",
        hall_ticket,
    )

    # --------------------------------------------------------
    # Create/update student
    # --------------------------------------------------------

    firestore_repo.create_student(
        hall_ticket
    )

    # --------------------------------------------------------
    # Deterministic INITIAL_SYNC job
    # --------------------------------------------------------

    job_id = firestore_repo.initial_sync_job_id(
        hall_ticket
    )

    created = firestore_repo.create_job(
        job_id,
        "INITIAL_SYNC",
        hall_ticket,
    )

    if created:

        logger.info(
            "INITIAL_JOB_CREATED student=%s job=%s",
            hall_ticket,
            job_id,
        )

    else:

        logger.info(
            "INITIAL_JOB_ALREADY_EXISTS student=%s job=%s",
            hall_ticket,
            job_id,
        )

    return {
        "status": "REGISTERED",
        "hall_ticket": hall_ticket,
        "job_id": job_id,
        "job_created": created,
    }


# ============================================================
# HISTORICAL
# ============================================================

def cmd_historical(args) -> dict:
    """
    Process one student's INITIAL_SYNC.
    """

    hall_ticket = _normalize(args.student)

    job_id = firestore_repo.initial_sync_job_id(
        hall_ticket
    )

    # Create only if missing.
    firestore_repo.create_job(
        job_id,
        "INITIAL_SYNC",
        hall_ticket,
    )

    logger.info(
        "HISTORICAL_START student=%s",
        hall_ticket,
    )

    return historical_sync.run_historical_sync(
        hall_ticket,
        worker_id=config.WORKER_ID,
    )


# ============================================================
# SINGLE STUDENT DAILY
# ============================================================

def cmd_daily(args) -> dict:
    """
    Process one student's attendance for one date.
    """

    hall_ticket = _normalize(args.student)

    target_date = (
        args.date
        or get_ist_today()
    )

    logger.info(
        "DAILY_START student=%s date=%s",
        hall_ticket,
        target_date,
    )

    return daily_sync.run_daily_sync(
        hall_ticket,
        target_date=target_date,
        worker_id=config.WORKER_ID,
    )


# ============================================================
# ENQUEUE DAILY
# ============================================================

def cmd_enqueue_daily(args) -> dict:
    """
    Prepare DAILY_SYNC jobs for all ACTIVE students.

    Flow:

        ACTIVE students?
              |
              +-- NO --> STOP
              |
             YES
              |
              v
        Probe ONE student
              |
              +-- unavailable --> store NOT_AVAILABLE
              |                   STOP
              |
              +-- available ----> create jobs
                                  for ALL students
    """

    target_date = (
        args.date
        or get_ist_today()
    )

    logger.info(
        "ENQUEUE_DAILY_START date=%s",
        target_date,
    )

    # --------------------------------------------------------
    # 1. Find registered ACTIVE students
    # --------------------------------------------------------

    students = get_active_students()

    if not students:

        logger.info(
            "NO_ACTIVE_STUDENTS date=%s",
            target_date,
        )

        return {
            "status": "NO_ACTIVE_STUDENTS",
            "date": target_date,
            "message": (
                "No registered students. "
                "Nothing to scrape."
            ),
        }

    logger.info(
        "ACTIVE_STUDENTS count=%d",
        len(students),
    )

    # --------------------------------------------------------
    # 2. Check whether date already has global status
    # --------------------------------------------------------

    existing_status = (
        firestore_repo.get_daily_status(
            target_date
        )
    )

    if (
        existing_status
        and existing_status.get("status") == "NOT_AVAILABLE"
    ):

        logger.info(
            "DATE_ALREADY_NOT_AVAILABLE date=%s",
            target_date,
        )

        return {
            "status": "SKIPPED_NOT_AVAILABLE",
            "date": target_date,
            "message": (
                "Date was already confirmed unavailable "
                "by the SCCE date selector."
            ),
        }

    # --------------------------------------------------------
    # 3. Pick one student for date probe
    # --------------------------------------------------------

    probe_student = students[0]

    probe_hall_ticket = _normalize(
        probe_student["hallTicket"]
    )

    logger.info(
        "DATE_PROBE hall=%s date=%s",
        probe_hall_ticket,
        target_date,
    )

    try:

        available = daily_sync.check_date_available(
            probe_hall_ticket,
            target_date,
        )

    except Exception as exc:

        logger.exception(
            "DATE_CHECK_FAILED date=%s",
            target_date,
        )

        return {
            "status": "DATE_CHECK_FAILED",
            "date": target_date,
            "probe_student": probe_hall_ticket,
            "error": str(exc),
        }

    # --------------------------------------------------------
    # 4. Date unavailable
    # --------------------------------------------------------

    if not available:

        logger.info(
            "DATE_NOT_AVAILABLE date=%s",
            target_date,
        )

        firestore_repo.set_daily_status(
            target_date,
            "NOT_AVAILABLE",
            checkedBy=probe_hall_ticket,
        )

        return {
            "status": "SKIPPED_NOT_AVAILABLE",
            "date": target_date,
            "message": (
                "Date is not offered by SCCE. "
                "No students will be scraped."
            ),
        }

    # --------------------------------------------------------
    # 5. Date available
    # --------------------------------------------------------

    logger.info(
        "DATE_AVAILABLE date=%s",
        target_date,
    )

    firestore_repo.set_daily_status(
        target_date,
        "AVAILABLE",
        checkedBy=probe_hall_ticket,
    )

    # --------------------------------------------------------
    # 6. Create jobs for ALL active students
    # --------------------------------------------------------

    created = 0
    existing = 0

    for student in students:

        hall_ticket = _normalize(
            student["hallTicket"]
        )

        job_id = firestore_repo.daily_sync_job_id(
            hall_ticket,
            target_date,
        )

        existing_job = firestore_repo.get_job(
            job_id
        )

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # Never create another job for the same
        # student + date.
        # ----------------------------------------------------

        if existing_job:

            existing += 1

            logger.info(
                "DAILY_JOB_EXISTS "
                "hall=%s date=%s job=%s",
                hall_ticket,
                target_date,
                job_id,
            )

            continue

        firestore_repo.create_job(
            job_id,
            "DAILY_SYNC",
            hall_ticket,
            total_count=1,
            extra_fields={
                "date": target_date,
            },
        )

        created += 1

        logger.info(
            "DAILY_JOB_CREATED "
            "hall=%s date=%s job=%s",
            hall_ticket,
            target_date,
            job_id,
        )

    logger.info(
        "ENQUEUE_DAILY_COMPLETE "
        "date=%s students=%d created=%d existing=%d",
        target_date,
        len(students),
        created,
        existing,
    )

    return {
        "status": "ENQUEUED",
        "date": target_date,
        "students": len(students),
        "created": created,
        "existing": existing,
    }


# ============================================================
# QUEUE
# ============================================================

def cmd_queue(args) -> dict:
    """
    Process eligible jobs sequentially.

    DAILY_SYNC:

        1. Check global daily_status.
        2. If NOT_AVAILABLE -> skip.
        3. Otherwise call run_daily_sync().
        4. run_daily_sync() checks Firestore attendance BEFORE
           contacting SCCE.
        5. Already scraped -> SUCCESS + skip.
        6. Missing -> scrape.

    This means one already-scraped student cannot cause
    another student to be scraped repeatedly.
    """

    jobs = firestore_repo.query_pending_jobs(
        job_type=args.type,
        limit=args.limit,
    )

    logger.info(
        "QUEUE_SCAN found=%d type=%s",
        len(jobs),
        args.type or "any",
    )

    results = []

    skipped_not_available = 0
    skipped_invalid = 0

    # --------------------------------------------------------
    # Process jobs sequentially
    # --------------------------------------------------------

    for job in jobs:

        job_id = job.get("id")
        job_type = job.get("type")
        hall_ticket = _normalize(
            job.get("hallTicket")
        )

        logger.info(
            "QUEUE_JOB job=%s type=%s hall=%s",
            job_id,
            job_type,
            hall_ticket,
        )

        # ====================================================
        # INITIAL SYNC
        # ====================================================

        if job_type == "INITIAL_SYNC":

            result = historical_sync.run_historical_sync(
                hall_ticket,
                worker_id=config.WORKER_ID,
            )

            results.append(result)

            continue

        # ====================================================
        # DAILY SYNC
        # ====================================================

        if job_type == "DAILY_SYNC":

            target_date = job.get("date")

            if not target_date:

                logger.warning(
                    "DAILY_JOB_INVALID "
                    "job=%s missing date",
                    job_id,
                )

                skipped_invalid += 1

                continue

            # ------------------------------------------------
            # Global date check
            # ------------------------------------------------

            daily_status = (
                firestore_repo.get_daily_status(
                    target_date
                )
            )

            # ------------------------------------------------
            # Date confirmed unavailable
            #
            # Do not call run_daily_sync().
            # ------------------------------------------------

            if (
                daily_status
                and daily_status.get("status")
                == "NOT_AVAILABLE"
            ):

                logger.info(
                    "DAILY_SKIP "
                    "job=%s hall=%s date=%s "
                    "reason=DATE_NOT_AVAILABLE",
                    job_id,
                    hall_ticket,
                    target_date,
                )

                skipped_not_available += 1

                continue

            # ------------------------------------------------
            # Date available / unknown
            #
            # run_daily_sync() is the authority for:
            #
            #   Firestore attendance exists?
            #
            # If yes:
            #
            #   NO SCCE REQUEST
            #
            # If no:
            #
            #   SCRAPE
            # ------------------------------------------------

            result = daily_sync.run_daily_sync(
                hall_ticket,
                target_date=target_date,
                worker_id=config.WORKER_ID,
            )

            results.append(result)

            continue

        # ====================================================
        # UNKNOWN JOB TYPE
        # ====================================================

        logger.warning(
            "UNKNOWN_JOB_TYPE job=%s type=%r",
            job_id,
            job_type,
        )

        skipped_invalid += 1

    # ========================================================
    # RESULT COUNTS
    # ========================================================

    succeeded = sum(
        1
        for result in results
        if result.get("status") == "SUCCESS"
    )

    failed = sum(
        1
        for result in results
        if result.get("status") == "FAILED"
    )

    skipped_claim = sum(
        1
        for result in results
        if result.get("claimed") is False
    )

    already_scraped = sum(
        1
        for result in results
        if (
            result.get("status") == "SUCCESS"
            and result.get("skipped") is True
            and result.get("reason") == "ALREADY_SCRAPED"
        )
    )

    scraped = sum(
        1
        for result in results
        if (
            result.get("status") == "SUCCESS"
            and result.get("skipped") is False
        )
    )

    return {
        "scanned": len(jobs),
        "processed": len(results),
        "succeeded": succeeded,
        "failed": failed,
        "scraped": scraped,
        "already_scraped": already_scraped,
        "skipped_claim": skipped_claim,
        "skipped_not_available": skipped_not_available,
        "skipped_invalid": skipped_invalid,
        "results": results,
    }


# ============================================================
# CLI
# ============================================================

def build_parser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        description="SCCE attendance worker"
    )

    sub = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # --------------------------------------------------------
    # REGISTER
    # --------------------------------------------------------

    p_register = sub.add_parser(
        "register",
        help="Register student and create INITIAL_SYNC job",
    )

    p_register.add_argument(
        "--student",
        required=True,
        help="Hall ticket number",
    )

    p_register.set_defaults(
        func=cmd_register
    )

    # --------------------------------------------------------
    # HISTORICAL
    # --------------------------------------------------------

    p_hist = sub.add_parser(
        "historical",
        help="Process student's historical attendance",
    )

    p_hist.add_argument(
        "--student",
        required=True,
        help="Hall ticket number",
    )

    p_hist.set_defaults(
        func=cmd_historical
    )

    # --------------------------------------------------------
    # DAILY
    # --------------------------------------------------------

    p_daily = sub.add_parser(
        "daily",
        help="Process one student's daily attendance",
    )

    p_daily.add_argument(
        "--student",
        required=True,
        help="Hall ticket number",
    )

    p_daily.add_argument(
        "--date",
        help="YYYY-MM-DD, default today in IST",
    )

    p_daily.set_defaults(
        func=cmd_daily
    )

    # --------------------------------------------------------
    # ENQUEUE DAILY
    # --------------------------------------------------------

    p_enqueue = sub.add_parser(
        "enqueue-daily",
        help=(
            "Check date availability and create "
            "DAILY_SYNC jobs"
        ),
    )

    p_enqueue.add_argument(
        "--date",
        help="YYYY-MM-DD, default today in IST",
    )

    p_enqueue.set_defaults(
        func=cmd_enqueue_daily
    )

    # --------------------------------------------------------
    # QUEUE
    # --------------------------------------------------------

    p_queue = sub.add_parser(
        "queue",
        help="Process eligible jobs",
    )

    p_queue.add_argument(
        "--type",
        choices=[
            "INITIAL_SYNC",
            "DAILY_SYNC",
        ],
        default=None,
        help="Only process this job type",
    )

    p_queue.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum jobs to scan",
    )

    p_queue.set_defaults(
        func=cmd_queue
    )

    return parser


# ============================================================
# MAIN
# ============================================================

def main(argv=None) -> int:

    parser = build_parser()

    args = parser.parse_args(argv)

    logger.info(
        "Worker starting | id=%s command=%s",
        config.WORKER_ID,
        args.command,
    )

    try:

        result = args.func(args)

    except Exception as exc:

        logger.exception(
            "WORKER_CRASHED"
        )

        print(
            json.dumps(
                {
                    "status": "CRASHED",
                    "error": str(exc),
                },
                indent=2,
                default=str,
            )
        )

        return 1

    print(
        json.dumps(
            result,
            indent=2,
            default=str,
        )
    )

    if result.get("status") == "FAILED":
        return 1

    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    sys.exit(main())