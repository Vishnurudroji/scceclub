"""
Historical / initialization sync: scrapes a new student's full
attendance history, oldest to newest, on ONE SCCE session where
possible, checkpointing Firestore after every single date so a crash
never loses already-scraped work and never re-scrapes it either.
"""

from __future__ import annotations

import logging

import firestore_repo
import sync_common
import config
import scraper
from exceptions import JobClaimError, LoginError, ScraperError, ScraperError, ValidationError, FirebaseError

logger = logging.getLogger("scce.historical_sync")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | historical | %(message)s")
    )
    logger.addHandler(_handler)
logger.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))


def run_historical_sync(hall_ticket: str, worker_id: str = None) -> dict:
    """
    Process (or resume) the INITIAL_SYNC job for one student.

    Resumability: the student's own lastScrapedDate in Firestore is the
    checkpoint, not anything held in memory or on the job doc alone --
    so restarting this function after a crash, on any worker, any
    machine, always recomputes the correct pending-dates list from
    scratch. There is no separate "resume" code path.
    """
    worker_id = worker_id or config.WORKER_ID
    hall_ticket = hall_ticket.strip().upper()
    job_id = firestore_repo.initial_sync_job_id(hall_ticket)

    try:
        firestore_repo.claim_job(job_id, worker_id)
    except JobClaimError as exc:
        logger.info("CLAIM_SKIPPED job=%s reason=%s", job_id, exc)
        return {"claimed": False, "job_id": job_id, "reason": str(exc)}

    logger.info("START job=%s hall=%s worker=%s", job_id, hall_ticket, worker_id)

    session = None
    completed_count = 0

    try:
        session = sync_common.authenticate(hall_ticket)

        try:
            available_dates = scraper.get_available_dates_from_session(session)
        except Exception as exc:
            raise ScraperError(f"Could not discover available dates: {exc}") from exc

        if not available_dates:
            raise ScraperError(f"No attendance dates available for {hall_ticket}")

        student = firestore_repo.get_student(hall_ticket)
        if student is None:
            # Registration should normally create the student stub before
            # any job runs, but a job can still self-heal a missing record
            # rather than fail outright.
            firestore_repo.create_student(hall_ticket, first_scraped_date=available_dates[0])
            student = firestore_repo.get_student(hall_ticket)

        if not student.get("firstScrapedDate"):
            firestore_repo.update_student_checkpoint(
                hall_ticket, first_scraped_date=available_dates[0]
            )

        checkpoint = student.get("lastScrapedDate")
        pending = [d for d in available_dates if not checkpoint or d > checkpoint]
        total_count = len(available_dates)
        completed_count = total_count - len(pending)

        logger.info(
            "DISCOVERED hall=%s dates=%d pending=%d", hall_ticket, total_count, len(pending)
        )
        firestore_repo.update_job_checkpoint(
            job_id, total_count=total_count, completed_count=completed_count
        )

        if not pending:
            firestore_repo.finish_job(job_id, "SUCCESS")
            logger.info("OPERATION_SUCCESS job=%s (already complete)", job_id)
            return {"claimed": True, "job_id": job_id, "status": "SUCCESS", "processed": 0}

        for scrape_date in pending:
            firestore_repo.update_job_checkpoint(job_id, current_date=scrape_date)
            logger.info("SCRAPE_START hall=%s date=%s", hall_ticket, scrape_date)

            result, session = sync_common.scrape_date_with_retry(session, hall_ticket, scrape_date)

            logger.info(
                "SCRAPE_SUCCESS hall=%s date=%s records=%d",
                hall_ticket, scrape_date, len(result["records"]),
            )

            firestore_repo.save_attendance_date(hall_ticket, result)
            firestore_repo.update_student_checkpoint(hall_ticket, last_scraped_date=scrape_date)

            completed_count += 1
            firestore_repo.update_job_checkpoint(
                job_id, last_completed_date=scrape_date, completed_count=completed_count
            )
            logger.info("PERSIST_SUCCESS hall=%s date=%s", hall_ticket, scrape_date)

        firestore_repo.finish_job(job_id, "SUCCESS")
        logger.info("OPERATION_SUCCESS job=%s processed=%d", job_id, completed_count)
        return {"claimed": True, "job_id": job_id, "status": "SUCCESS", "processed": completed_count}

    except (LoginError, ScraperError, ValidationError, FirebaseError) as exc:
        error_message = str(exc)
        logger.error("OPERATION_FAILED job=%s processed=%d error=%s", job_id, completed_count, error_message)
        firestore_repo.finish_job(job_id, "FAILED", error=error_message)
        return {
            "claimed": True, "job_id": job_id, "status": "FAILED",
            "processed": completed_count, "error": error_message,
        }

    except Exception as exc:
        logger.exception("Unexpected failure job=%s", job_id)
        firestore_repo.finish_job(job_id, "FAILED", error=str(exc))
        return {
            "claimed": True, "job_id": job_id, "status": "FAILED",
            "processed": completed_count, "error": str(exc),
        }

    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass