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
        2. Probe date availability using ONE student (informational
           only -- see cmd_enqueue_daily docstring for Problem 2).
        3. Create DAILY_SYNC jobs for ALL active students regardless
           of the probe result.


    python worker.py queue --type DAILY_SYNC --limit 100

        Process eligible jobs, each fully isolated from the others,
        with bounded concurrency.

        DAILY_SYNC:
            - NOT_AVAILABLE date -> skip
            - Already scraped -> skip through daily_sync
            - Not scraped -> scrape

        INITIAL_SYNC:
            - historical attendance sync

RELIABILITY FIX (surgical) -- summary of what changed in this file:

    Problem 1 (one student's failure crashed the whole queue):
        cmd_queue() used to call historical_sync.run_historical_sync()
        / daily_sync.run_daily_sync() directly inside its loop with no
        exception handling of its own. Combined with the missing
        except clause that used to be in daily_sync.run_daily_sync()
        (fixed separately in daily_sync.py), an exception for one
        student propagated all the way to main() and crashed the
        process, so students queued after the failing one were never
        processed.

        Fix: job dispatch was extracted into _run_single_job(), which
        wraps EVERYTHING in try/except Exception and always returns a
        result dict -- it never raises. cmd_queue() now runs jobs
        through a small bounded thread pool so a genuine exception in
        one job's thread cannot affect any other job's thread.

    Problem 2 (one probe student's "no class" blocked every other
    student for that date):
        cmd_enqueue_daily() used to check availability using only
        students[0], and on a negative result would write a global
        daily_status/{date} = NOT_AVAILABLE document and return
        immediately -- never creating jobs for anyone else. It also
        short-circuited on any pre-existing NOT_AVAILABLE status.

        Fix: the probe is now informational only. A positive probe is
        still recorded (harmless, since it never blocks anything). A
        negative probe (or a probe that itself errors) is logged but
        no longer written as NOT_AVAILABLE and no longer prevents job
        creation -- DAILY_SYNC jobs are always created for every
        active student. Each student's own job independently
        discovers, via the existing Firestore-first check and scrape
        in daily_sync.run_daily_sync(), whether SCCE has attendance
        for THAT student on that date.

    Problems 3/4 (scaling + retry):
        Retry-with-backoff already existed in sync_common.py and is
        untouched. cmd_queue() now bounds how many jobs run at once
        via a small ThreadPoolExecutor, sized from
        config.MAX_CONCURRENT_SCRAPES when that setting exists,
        otherwise a conservative default of 3 -- config.py itself is
        NOT modified.

    No Firestore schema, collection, field, or job-ID changes were
    made anywhere in this file.
"""


from __future__ import annotations

import argparse
import concurrent.futures
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

    Problem 2 fix -- read this before touching this function again:

        SCCE's date selector can differ per student/section (e.g. two
        students in different sections can legitimately see different
        available dates). The old implementation probed exactly
        students[0] and, if that ONE student came back unavailable,
        wrote a GLOBAL daily_status/{date} = NOT_AVAILABLE document
        and returned without creating a single job -- so a student
        with an ordinary no-class day (e.g. hall ticket 501) silently
        prevented every other student (e.g. 565, who DID have classes)
        from ever being scraped for that date.

        The probe is now purely informational:
          - A positive result is still written to daily_status (this
            is safe -- it is only ever additive, never used to skip).
          - A negative result, or the probe call itself failing, is
            logged and otherwise ignored: it is NOT written to
            daily_status and it does NOT stop job creation.
        DAILY_SYNC jobs are ALWAYS created for every active student
        (idempotently -- create_job() no-ops if the job already
        exists). Each student's own job discovers, when it actually
        runs, whether SCCE has attendance for THAT student on this
        date, via daily_sync.run_daily_sync()'s existing
        Firestore-first check + scrape.

        This also removes the old early-return that checked for a
        pre-existing NOT_AVAILABLE status before even probing --
        that early-return is exactly what let one bad probe result
        poison every future enqueue run for that date too.
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
    # 2. Informational probe (does NOT gate job creation)
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

        probe_available = daily_sync.check_date_available(
            probe_hall_ticket,
            target_date,
        )

    except Exception as exc:

        logger.warning(
            "DATE_PROBE_FAILED hall=%s date=%s error=%s "
            "-- continuing, this does not block job creation",
            probe_hall_ticket,
            target_date,
            exc,
        )

        probe_available = None

    if probe_available is True:

        logger.info(
            "DATE_AVAILABLE (probe) date=%s",
            target_date,
        )

        firestore_repo.set_daily_status(
            target_date,
            "AVAILABLE",
            checkedBy=probe_hall_ticket,
        )

    elif probe_available is False:

        logger.info(
            "DATE_NOT_AVAILABLE_FOR_PROBE "
            "hall=%s date=%s -- per-student signal only, "
            "other students still get their own job",
            probe_hall_ticket,
            target_date,
        )

        # Deliberately NOT calling
        # firestore_repo.set_daily_status(target_date, "NOT_AVAILABLE", ...)
        # here -- see Problem 2 above.

    # --------------------------------------------------------
    # 3. Create jobs for ALL active students, regardless of the
    #    probe outcome.
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
        "probe_student": probe_hall_ticket,
        "probe_available": probe_available,
    }


# ============================================================
# SINGLE JOB DISPATCH (isolation boundary)
# ============================================================

def _run_single_job(job: dict) -> dict:
    """
    Process exactly one job and ALWAYS return a result dict --
    never raise.

    This is the isolation boundary required by Problem 1: whatever
    goes wrong while processing one student's job (a bug, an
    unexpected exception type, anything not already handled inside
    daily_sync.run_daily_sync() / historical_sync.run_historical_sync()),
    it is caught here, logged, and turned into a FAILED-shaped result
    -- so it can never propagate up and stop the rest of the queue
    from being processed.
    """

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

    try:

        # ====================================================
        # INITIAL SYNC
        # ====================================================

        if job_type == "INITIAL_SYNC":

            return historical_sync.run_historical_sync(
                hall_ticket,
                worker_id=config.WORKER_ID,
            )

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

                return {
                    "job_id": job_id,
                    "hallTicket": hall_ticket,
                    "status": "INVALID",
                    "reason": "MISSING_DATE",
                }

            # ------------------------------------------------
            # Global date check.
            #
            # NOTE: as of the Problem 2 fix, cmd_enqueue_daily()
            # no longer writes NOT_AVAILABLE from a single probe
            # student, so this branch is now only ever reached
            # for dates that were confirmed NOT_AVAILABLE some
            # other, more trustworthy way (or by older data from
            # before this fix). It is kept as-is deliberately: a
            # cheap skip, never the source of a false verdict for
            # jobs created going forward.
            # ------------------------------------------------

            daily_status = (
                firestore_repo.get_daily_status(
                    target_date
                )
            )

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

                return {
                    "job_id": job_id,
                    "hallTicket": hall_ticket,
                    "date": target_date,
                    "status": "SKIPPED",
                    "reason": "DATE_NOT_AVAILABLE",
                }

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
            #
            # run_daily_sync() itself now catches its own
            # scraper/Firestore failures and returns a FAILED
            # result rather than raising (see daily_sync.py).
            # ------------------------------------------------

            return daily_sync.run_daily_sync(
                hall_ticket,
                target_date=target_date,
                worker_id=config.WORKER_ID,
            )

        # ====================================================
        # UNKNOWN JOB TYPE
        # ====================================================

        logger.warning(
            "UNKNOWN_JOB_TYPE job=%s type=%r",
            job_id,
            job_type,
        )

        return {
            "job_id": job_id,
            "hallTicket": hall_ticket,
            "status": "INVALID",
            "reason": "UNKNOWN_JOB_TYPE",
        }

    except Exception as exc:

        # ----------------------------------------------------
        # Final isolation boundary. Belt-and-suspenders on top
        # of daily_sync.run_daily_sync()'s own error handling:
        # no matter what goes wrong with THIS one student's job,
        # it must never take down the rest of the queue.
        # ----------------------------------------------------

        logger.exception(
            "QUEUE_JOB_FAILED job=%s hall=%s type=%s",
            job_id,
            hall_ticket,
            job_type,
        )

        return {
            "job_id": job_id,
            "hallTicket": hall_ticket,
            "status": "FAILED",
            "error": str(exc),
        }


# ============================================================
# QUEUE
# ============================================================

def cmd_queue(args) -> dict:
    """
    Process eligible jobs with bounded concurrency, each fully
    isolated from the others (Problem 1 + Problem 3 fix).

    Every job is dispatched through _run_single_job(), which never
    raises, and jobs run inside a small ThreadPoolExecutor so that:

        - one student's failure cannot affect any other student
          (each job's exceptions are contained within its own
          try/except, and even within its own thread),
        - SCCE request volume stays bounded no matter how many
          students are registered (concurrency is capped, not
          unlimited),
        - existing Firestore checkpoints, deterministic job IDs,
          and the Firestore-first "already scraped" check are all
          preserved exactly as before -- nothing about *what* each
          job does has changed, only that jobs run concurrently
          (bounded) instead of one at a time, and that a failure
          in one job can no longer abort the batch.

    Concurrency is capped at config.MAX_CONCURRENT_SCRAPES when that
    setting exists on the existing config module; otherwise a
    conservative default of 3 is used. config.py itself is not
    modified by this fix.
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

    max_workers = max(
        1,
        getattr(config, "MAX_CONCURRENT_SCRAPES", 3),
    )

    results = []

    skipped_not_available = 0
    skipped_invalid = 0

    # --------------------------------------------------------
    # Process jobs with bounded concurrency. Each job is fully
    # isolated by _run_single_job(), which never raises.
    # --------------------------------------------------------

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:

        future_to_job = {
            executor.submit(_run_single_job, job): job
            for job in jobs
        }

        for future in concurrent.futures.as_completed(
            future_to_job
        ):

            job = future_to_job[future]

            try:

                result = future.result()

            except Exception as exc:

                # Should be unreachable -- _run_single_job never
                # raises -- but guarded anyway so a freak thread
                # failure still cannot take down the batch.

                logger.exception(
                    "QUEUE_FUTURE_FAILED job=%s",
                    job.get("id"),
                )

                result = {
                    "job_id": job.get("id"),
                    "hallTicket": _normalize(
                        job.get("hallTicket")
                    ),
                    "status": "FAILED",
                    "error": str(exc),
                }

            if (
                result.get("status") == "SKIPPED"
                and result.get("reason") == "DATE_NOT_AVAILABLE"
            ):

                skipped_not_available += 1
                continue

            if result.get("status") == "INVALID":

                skipped_invalid += 1
                continue

            results.append(result)

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
            "Probe date availability (informational) and create "
            "DAILY_SYNC jobs for all active students"
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