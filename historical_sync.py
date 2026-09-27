"""
Historical / initialization sync.

Scrapes a student's full SCCE attendance history.

Important behavior:
    - Processes every missing date instead of stopping at the first
      temporary scrape failure.
    - Failed dates are remembered and retried after the first pass.
    - Already stored attendance dates are never unnecessarily scraped.
    - Firestore attendance documents are the source of truth for whether
      an individual date has actually been saved.
    - No Firestore schema changes are required.
    - If any date is still missing after the retry pass, the job remains
      FAILED and can be picked up by a later worker queue run.
"""

from __future__ import annotations

import logging

import config
import firestore_repo
import scraper
import sync_common

from exceptions import (
    FirebaseError,
    JobClaimError,
    LoginError,
    ScraperError,
    ValidationError,
)


logger = logging.getLogger("scce.historical_sync")

if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | historical | %(message)s"
        )
    )
    logger.addHandler(_handler)

logger.setLevel(
    getattr(config, "LOG_LEVEL", logging.INFO)
)


def _scrape_and_save_date(
    hall_ticket: str,
    scrape_date: str,
    session,
):
    """
    Scrape and immediately persist one date.

    Returns:
        (session, success)

    Raises:
        LoginError
        ScraperError
        ValidationError
        FirebaseError
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

    logger.info(
        "SCRAPE_SUCCESS hall=%s date=%s records=%d",
        hall_ticket,
        scrape_date,
        len(result["records"]),
    )

    # Save immediately.
    #
    # If the process crashes after this point, the attendance document
    # already exists and the next run will detect it.
    firestore_repo.save_attendance_date(
        hall_ticket,
        result,
    )

    logger.info(
        "PERSIST_SUCCESS hall=%s date=%s",
        hall_ticket,
        scrape_date,
    )

    return session, True


def _update_contiguous_checkpoint(
    hall_ticket: str,
    available_dates: list[str],
) -> str | None:
    """
    Advance lastScrapedDate only through a CONTIGUOUS sequence of
    actually stored attendance dates.

    Example:

        available:
            A1 A2 A3 A4 A5

        stored:
            A1 A2 A3 A5

        checkpoint becomes:
            A3

    A5 is stored, but A4 is missing, so the checkpoint must NOT jump
    over A4.

    This prevents a failed middle date from being permanently skipped.
    """

    stored_dates = set(
        firestore_repo.list_attendance_dates(hall_ticket)
    )

    contiguous_last = None

    for scrape_date in available_dates:

        if scrape_date not in stored_dates:
            break

        contiguous_last = scrape_date

    if contiguous_last is not None:

        firestore_repo.update_student_checkpoint(
            hall_ticket,
            last_scraped_date=contiguous_last,
        )

        logger.info(
            "CHECKPOINT_UPDATED hall=%s lastScrapedDate=%s",
            hall_ticket,
            contiguous_last,
        )

    return contiguous_last


def run_historical_sync(
    hall_ticket: str,
    worker_id: str = None,
) -> dict:
    """
    Process or resume the INITIAL_SYNC job for one student.

    The attendance collection is used as the per-date source of truth.

    This allows the worker to safely handle gaps:

        A8 -> success
        A9 -> failure
        A10 -> success
        A11 -> success

    A9 remains missing and will be retried.

    Already saved dates are skipped.
    """

    worker_id = worker_id or config.WORKER_ID
    hall_ticket = hall_ticket.strip().upper()

    job_id = firestore_repo.initial_sync_job_id(
        hall_ticket
    )

    # ==============================================================
    # CLAIM JOB
    # ==============================================================

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
            "reason": str(exc),
        }

    logger.info(
        "START job=%s hall=%s worker=%s",
        job_id,
        hall_ticket,
        worker_id,
    )

    session = None

    newly_scraped = 0
    failed_dates: list[str] = []

    try:

        # ==========================================================
        # AUTHENTICATE
        # ==========================================================

        session = sync_common.authenticate(
            hall_ticket
        )

        logger.info(
            "AUTHENTICATED hall=%s",
            hall_ticket,
        )

        # ==========================================================
        # DISCOVER AVAILABLE DATES
        # ==========================================================

        try:

            available_dates = (
                scraper.get_available_dates_from_session(
                    session
                )
            )

        except Exception as exc:

            raise ScraperError(
                f"Could not discover available dates: {exc}"
            ) from exc

        if not available_dates:

            raise ScraperError(
                f"No attendance dates available for {hall_ticket}"
            )

        # Make sure dates are ordered oldest -> newest.
        available_dates = sorted(
            set(available_dates)
        )

        total_count = len(available_dates)

        logger.info(
            "DISCOVERED hall=%s dates=%d",
            hall_ticket,
            total_count,
        )

        # ==========================================================
        # ENSURE STUDENT EXISTS
        # ==========================================================

        student = firestore_repo.get_student(
            hall_ticket
        )

        if student is None:

            firestore_repo.create_student(
                hall_ticket,
                first_scraped_date=available_dates[0],
            )

            student = firestore_repo.get_student(
                hall_ticket
            )

        if not student.get("firstScrapedDate"):

            firestore_repo.update_student_checkpoint(
                hall_ticket,
                first_scraped_date=available_dates[0],
            )

        # ==========================================================
        # GET ACTUALLY STORED ATTENDANCE DATES
        # ==========================================================

        stored_dates = set(
            firestore_repo.list_attendance_dates(
                hall_ticket
            )
        )

        stored_available_dates = {
            date
            for date in available_dates
            if date in stored_dates
        }

        pending_dates = [
            date
            for date in available_dates
            if date not in stored_dates
        ]

        completed_count = len(
            stored_available_dates
        )

        firestore_repo.update_job_checkpoint(
            job_id,
            total_count=total_count,
            completed_count=completed_count,
        )

        logger.info(
            "CHECKPOINT_STATE "
            "hall=%s stored=%d pending=%d total=%d",
            hall_ticket,
            completed_count,
            len(pending_dates),
            total_count,
        )

        # ==========================================================
        # ALREADY COMPLETE
        # ==========================================================

        if not pending_dates:

            firestore_repo.finish_job(
                job_id,
                "SUCCESS",
            )

            logger.info(
                "OPERATION_SUCCESS "
                "job=%s hall=%s already complete",
                job_id,
                hall_ticket,
            )

            return {
                "claimed": True,
                "job_id": job_id,
                "status": "SUCCESS",
                "processed": 0,
                "failed_dates": [],
                "completed_count": completed_count,
                "total_count": total_count,
            }

        # ==========================================================
        # FIRST PASS
        #
        # IMPORTANT:
        # A failed date does NOT stop the remaining dates.
        # ==========================================================

        logger.info(
            "FIRST_PASS_START hall=%s pending=%d",
            hall_ticket,
            len(pending_dates),
        )

        for scrape_date in pending_dates:

            firestore_repo.update_job_checkpoint(
                job_id,
                current_date=scrape_date,
                completed_count=completed_count,
            )

            try:

                session, _ = _scrape_and_save_date(
                    hall_ticket,
                    scrape_date,
                    session,
                )

                newly_scraped += 1
                completed_count += 1

                # IMPORTANT:
                #
                # Do NOT blindly update lastScrapedDate here.
                #
                # If A9 fails and A10 succeeds, the checkpoint must
                # not jump to A10.
                #
                # The contiguous checkpoint is updated later.

                firestore_repo.update_job_checkpoint(
                    job_id,
                    last_completed_date=scrape_date,
                    current_date=scrape_date,
                    completed_count=completed_count,
                )

            except (
                LoginError,
                ScraperError,
                ValidationError,
            ) as exc:

                logger.warning(
                    "DATE_FAILED "
                    "hall=%s date=%s error=%s",
                    hall_ticket,
                    scrape_date,
                    exc,
                )

                failed_dates.append(
                    scrape_date
                )

                # VERY IMPORTANT:
                #
                # Continue to the next date.
                #
                # This is the actual A9 fix.

                continue

            except FirebaseError:

                # Firestore failure is different from a scrape
                # failure. Do not continue blindly when persistence
                # itself is unavailable.
                raise

        # ==========================================================
        # RETRY FAILED DATES
        # ==========================================================

        if failed_dates:

            logger.info(
                "RETRY_FAILED_DATES "
                "hall=%s count=%d dates=%s",
                hall_ticket,
                len(failed_dates),
                ",".join(failed_dates),
            )

        retry_failed_dates: list[str] = []

        for scrape_date in failed_dates:

            firestore_repo.update_job_checkpoint(
                job_id,
                current_date=scrape_date,
                completed_count=completed_count,
            )

            logger.info(
                "RETRY_START hall=%s date=%s",
                hall_ticket,
                scrape_date,
            )

            try:

                session, _ = _scrape_and_save_date(
                    hall_ticket,
                    scrape_date,
                    session,
                )

                newly_scraped += 1
                completed_count += 1

                firestore_repo.update_job_checkpoint(
                    job_id,
                    last_completed_date=scrape_date,
                    current_date=scrape_date,
                    completed_count=completed_count,
                )

                logger.info(
                    "RETRY_SUCCESS hall=%s date=%s",
                    hall_ticket,
                    scrape_date,
                )

            except (
                LoginError,
                ScraperError,
                ValidationError,
            ) as exc:

                logger.warning(
                    "RETRY_FAILED "
                    "hall=%s date=%s error=%s",
                    hall_ticket,
                    scrape_date,
                    exc,
                )

                retry_failed_dates.append(
                    scrape_date
                )

                continue

            except FirebaseError:

                raise

        # ==========================================================
        # UPDATE CONTIGUOUS CHECKPOINT
        # ==========================================================

        contiguous_checkpoint = (
            _update_contiguous_checkpoint(
                hall_ticket,
                available_dates,
            )
        )

        # ==========================================================
        # FINAL RESULT
        # ==========================================================

        if retry_failed_dates:

            error_message = (
                "Historical sync completed with missing dates: "
                + ", ".join(retry_failed_dates)
            )

            logger.warning(
                "OPERATION_PARTIAL_FAILURE "
                "job=%s hall=%s failed_dates=%s",
                job_id,
                hall_ticket,
                ",".join(retry_failed_dates),
            )

            firestore_repo.finish_job(
                job_id,
                "FAILED",
                error=error_message,
            )

            return {
                "claimed": True,
                "job_id": job_id,
                "status": "FAILED",
                "processed": newly_scraped,
                "failed_dates": retry_failed_dates,
                "completed_count": completed_count,
                "total_count": total_count,
                "lastScrapedDate": contiguous_checkpoint,
                "error": error_message,
            }

        # ==========================================================
        # EVERYTHING SUCCESSFUL
        # ==========================================================

        firestore_repo.finish_job(
            job_id,
            "SUCCESS",
        )

        logger.info(
            "OPERATION_SUCCESS "
            "job=%s hall=%s processed=%d",
            job_id,
            hall_ticket,
            newly_scraped,
        )

        return {
            "claimed": True,
            "job_id": job_id,
            "status": "SUCCESS",
            "processed": newly_scraped,
            "failed_dates": [],
            "completed_count": completed_count,
            "total_count": total_count,
            "lastScrapedDate": contiguous_checkpoint,
        }

    # ==============================================================
    # SYSTEM / JOB FAILURE
    # ==============================================================

    except (
        LoginError,
        ScraperError,
        ValidationError,
        FirebaseError,
    ) as exc:

        error_message = str(exc)

        logger.error(
            "OPERATION_FAILED "
            "job=%s hall=%s processed=%d error=%s",
            job_id,
            hall_ticket,
            newly_scraped,
            error_message,
        )

        try:

            firestore_repo.finish_job(
                job_id,
                "FAILED",
                error=error_message,
            )

        except Exception as finalize_exc:

            logger.error(
                "FAILED_TO_FINALIZE_JOB "
                "job=%s error=%s",
                job_id,
                finalize_exc,
            )

        return {
            "claimed": True,
            "job_id": job_id,
            "status": "FAILED",
            "processed": newly_scraped,
            "failed_dates": failed_dates,
            "error": error_message,
        }

    # ==============================================================
    # UNEXPECTED FAILURE
    # ==============================================================

    except Exception as exc:

        logger.exception(
            "Unexpected failure job=%s hall=%s",
            job_id,
            hall_ticket,
        )

        try:

            firestore_repo.finish_job(
                job_id,
                "FAILED",
                error=str(exc),
            )

        except Exception as finalize_exc:

            logger.error(
                "FAILED_TO_FINALIZE_JOB "
                "job=%s error=%s",
                job_id,
                finalize_exc,
            )

        return {
            "claimed": True,
            "job_id": job_id,
            "status": "FAILED",
            "processed": newly_scraped,
            "failed_dates": failed_dates,
            "error": str(exc),
        }

    finally:

        if session is not None:

            try:
                session.close()
            except Exception:
                pass