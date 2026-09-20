"""
Firestore repository layer.

Firestore is the single source of truth. This module is the only place
that talks to it -- workers and job logic call these functions, never
the Firestore client directly, so the schema stays in one place.

Schema
------
students/{hallTicket}
    hallTicket: str
    registeredAt: timestamp
    firstScrapedDate: "YYYY-MM-DD" | None
    lastScrapedDate: "YYYY-MM-DD" | None
    status: "ACTIVE"
    updatedAt: timestamp

students/{hallTicket}/attendance/{YYYY-MM-DD}
    date: "YYYY-MM-DD"
    attended: int
    conducted: int
    percentage: float
    records: [ {sno, hour, subject, attended, conducted}, ... ]
    updatedAt: timestamp

jobs/{jobId}
    type: "INITIAL_SYNC" | "DAILY_SYNC"
    hallTicket: str
    status: "PENDING" | "PROCESSING" | "SUCCESS" | "FAILED"
    workerId: str | None
    attempts: int
    maxAttempts: int
    lastError: str | None
    lastCompletedDate: "YYYY-MM-DD" | None
    currentDate: "YYYY-MM-DD" | None
    completedCount: int
    totalCount: int | None
    createdAt: timestamp
    updatedAt: timestamp
    startedAt: timestamp | None

Deterministic job IDs:
    initial sync : f"initial_{hall_ticket}"
    daily sync   : f"daily_{hall_ticket}_{date}"
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core.exceptions import AlreadyExists
from google.cloud.firestore_v1 import DocumentReference

import config
from exceptions import FirebaseError, JobClaimError


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger("scce.firestore_repo")

if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | firestore | %(message)s"
        )
    )
    logger.addHandler(_handler)

logger.setLevel(
    getattr(logging, config.LOG_LEVEL, logging.INFO)
)


# ============================================================
# FIREBASE CLIENT
# ============================================================

_app = None
_client = None


def init_firebase() -> None:
    """
    Initialise the Firebase Admin app exactly once.

    Safe to call multiple times.
    """
    global _app

    if _app is not None:
        return

    try:
        cred = credentials.Certificate(
            config.FIREBASE_CREDENTIALS_PATH
        )

        kwargs = {}

        if config.FIRESTORE_PROJECT_ID:
            kwargs["projectId"] = config.FIRESTORE_PROJECT_ID

        _app = firebase_admin.initialize_app(
            cred,
            kwargs,
        )

        logger.info(
            "Firebase initialised | project=%s",
            config.FIRESTORE_PROJECT_ID or "(default)",
        )

    except Exception as exc:
        raise FirebaseError(
            f"Could not initialise Firebase: {exc}"
        ) from exc


def get_client():
    """
    Return the Firestore client.

    Firebase is initialised on first use.
    """
    global _client

    if _client is None:
        init_firebase()
        _client = firestore.client()

    return _client


def _now():
    """
    Firestore server timestamp.
    """
    return firestore.SERVER_TIMESTAMP


def _utcnow() -> datetime:
    """
    Current UTC datetime.
    """
    return datetime.now(timezone.utc)


# ============================================================
# STUDENTS
# ============================================================
# ============================================================
# DAILY DATE STATUS
# ============================================================

def daily_status_ref(target_date: str) -> DocumentReference:
    """
    Global status for one calendar date.

    Example:
        daily_status/2026-09-20
    """
    return (
        get_client()
        .collection("daily_status")
        .document(target_date)
    )


def get_daily_status(target_date: str) -> Optional[dict]:
    """
    Return the global status for a date.

    Example:
        {
            "date": "2026-09-20",
            "status": "NOT_AVAILABLE",
            "checkedBy": "23N01A0501",
            "updatedAt": ...
        }
    """

    try:
        snap = daily_status_ref(target_date).get()

    except Exception as exc:
        raise FirebaseError(
            f"Failed to read daily status {target_date}: {exc}"
        ) from exc

    if not snap.exists:
        return None

    return snap.to_dict()


def set_daily_status(
    target_date: str,
    status: str,
    **extra_fields,
) -> None:
    """
    Store the global availability status for a date.

    Valid statuses:
        AVAILABLE
        NOT_AVAILABLE
    """

    if status not in (
        "AVAILABLE",
        "NOT_AVAILABLE",
    ):
        raise ValueError(
            f"Invalid daily status: {status!r}"
        )

    data = {
        "date": target_date,
        "status": status,
        "updatedAt": _now(),
    }

    data.update(extra_fields)

    try:
        daily_status_ref(target_date).set(
            data,
            merge=True,
        )

        logger.info(
            "DAILY_STATUS date=%s status=%s",
            target_date,
            status,
        )

    except Exception as exc:
        raise FirebaseError(
            f"Failed to save daily status "
            f"{target_date}: {exc}"
        ) from exc
def student_ref(hall_ticket: str) -> DocumentReference:
    return (
        get_client()
        .collection("students")
        .document(hall_ticket)
    )


def get_student(hall_ticket: str) -> Optional[dict]:
    try:
        snap = student_ref(hall_ticket).get()

    except Exception as exc:
        raise FirebaseError(
            f"Failed to read student {hall_ticket}: {exc}"
        ) from exc

    if not snap.exists:
        return None

    data = snap.to_dict()
    data["hallTicket"] = hall_ticket

    return data


def student_exists(hall_ticket: str) -> bool:
    return get_student(hall_ticket) is not None


def create_student(
    hall_ticket: str,
    first_scraped_date: Optional[str] = None,
) -> None:
    """
    Create the student document.

    Creation is idempotent. If another worker already created
    the same student, AlreadyExists is treated as a normal no-op.
    """
    try:
        student_ref(hall_ticket).create(
            {
                "hallTicket": hall_ticket,
                "registeredAt": _now(),
                "firstScrapedDate": first_scraped_date,
                "lastScrapedDate": None,
                "status": "ACTIVE",
                "updatedAt": _now(),
            }
        )

        logger.info(
            "Student created | hall=%s",
            hall_ticket,
        )

    except AlreadyExists:
        logger.info(
            "Student already exists, skipping create | hall=%s",
            hall_ticket,
        )

    except Exception as exc:
        raise FirebaseError(
            f"Failed to create student {hall_ticket}: {exc}"
        ) from exc


def update_student_checkpoint(
    hall_ticket: str,
    last_scraped_date: Optional[str] = None,
    first_scraped_date: Optional[str] = None,
) -> None:

    updates = {
        "updatedAt": _now()
    }

    if last_scraped_date is not None:
        updates["lastScrapedDate"] = last_scraped_date

    if first_scraped_date is not None:
        updates["firstScrapedDate"] = first_scraped_date

    try:
        student_ref(hall_ticket).set(
            updates,
            merge=True,
        )

    except Exception as exc:
        raise FirebaseError(
            f"Failed to update student checkpoint "
            f"{hall_ticket}: {exc}"
        ) from exc


def list_students() -> list[dict]:
    try:
        docs = (
            get_client()
            .collection("students")
            .where("status", "==", "ACTIVE")
            .stream()
        )

    except Exception as exc:
        raise FirebaseError(
            f"Failed to list students: {exc}"
        ) from exc

    results = []

    for doc in docs:
        data = doc.to_dict()
        data["hallTicket"] = doc.id
        results.append(data)

    return results


# ============================================================
# ATTENDANCE
# ============================================================

def attendance_ref(
    hall_ticket: str,
    date: str,
) -> DocumentReference:

    return (
        student_ref(hall_ticket)
        .collection("attendance")
        .document(date)
    )


def get_attendance_date(
    hall_ticket: str,
    date: str,
) -> Optional[dict]:

    try:
        snap = attendance_ref(
            hall_ticket,
            date,
        ).get()

    except Exception as exc:
        raise FirebaseError(
            f"Failed to read attendance "
            f"{hall_ticket}/{date}: {exc}"
        ) from exc

    return snap.to_dict() if snap.exists else None


def save_attendance_date(
    hall_ticket: str,
    scraped: dict,
) -> None:
    """
    Save one attendance date.

    Deterministic document ID = date.
    Re-saving the same date overwrites the same document,
    making the operation idempotent.
    """

    date = scraped["date"]

    try:
        attendance_ref(
            hall_ticket,
            date,
        ).set(
            {
                "date": date,
                "attended": scraped["attended"],
                "conducted": scraped["conducted"],
                "percentage": scraped["percentage"],
                "records": scraped["records"],
                "updatedAt": _now(),
            }
        )

        logger.info(
            "PERSIST_SUCCESS hall=%s date=%s",
            hall_ticket,
            date,
        )

    except Exception as exc:
        raise FirebaseError(
            f"Failed to save attendance "
            f"{hall_ticket}/{date}: {exc}"
        ) from exc


def list_attendance_dates(
    hall_ticket: str,
) -> list[str]:

    """
    Return all attendance dates already stored.
    """

    try:
        docs = (
            student_ref(hall_ticket)
            .collection("attendance")
            .stream()
        )

        return sorted(
            doc.id
            for doc in docs
        )

    except Exception as exc:
        raise FirebaseError(
            f"Failed to list attendance "
            f"for {hall_ticket}: {exc}"
        ) from exc


# ============================================================
# JOBS
# ============================================================

def initial_sync_job_id(
    hall_ticket: str,
) -> str:

    return f"initial_{hall_ticket}"


def daily_sync_job_id(
    hall_ticket: str,
    date: str,
) -> str:

    return f"daily_{hall_ticket}_{date}"


def job_ref(
    job_id: str,
) -> DocumentReference:

    return (
        get_client()
        .collection("jobs")
        .document(job_id)
    )


def get_job(
    job_id: str,
) -> Optional[dict]:

    try:
        snap = job_ref(job_id).get()

    except Exception as exc:
        raise FirebaseError(
            f"Failed to read job {job_id}: {exc}"
        ) from exc

    if not snap.exists:
        return None

    data = snap.to_dict()
    data["id"] = job_id

    return data


def create_job(
    job_id: str,
    job_type: str,
    hall_ticket: str,
    total_count: Optional[int] = None,
    extra_fields: Optional[dict] = None,
) -> bool:

    """
    Create a job in PENDING state.

    Returns:
        True  -> newly created
        False -> already existed
    """

    payload = {
        "type": job_type,
        "hallTicket": hall_ticket,
        "status": "PENDING",
        "workerId": None,
        "attempts": 0,
        "maxAttempts": config.MAX_JOB_ATTEMPTS,
        "lastError": None,
        "lastCompletedDate": None,
        "currentDate": None,
        "completedCount": 0,
        "totalCount": total_count,
        "createdAt": _now(),
        "updatedAt": _now(),
        "startedAt": None,
    }

    if extra_fields:
        payload.update(extra_fields)

    try:
        job_ref(job_id).create(payload)

        logger.info(
            "Job created | id=%s type=%s hall=%s",
            job_id,
            job_type,
            hall_ticket,
        )

        return True

    except AlreadyExists:
        logger.info(
            "Job already exists, not re-created | id=%s",
            job_id,
        )

        return False

    except Exception as exc:
        raise FirebaseError(
            f"Failed to create job {job_id}: {exc}"
        ) from exc


def _is_stale(
    job_data: dict,
    stale_timeout_seconds: int,
) -> bool:

    updated_at = job_data.get("updatedAt")

    if updated_at is None:
        return True

    age_seconds = (
        _utcnow() - updated_at
    ).total_seconds()

    return age_seconds > stale_timeout_seconds


def claim_job(
    job_id: str,
    worker_id: str,
    stale_timeout_seconds: int = config.STALE_JOB_TIMEOUT_SECONDS,
) -> dict:

    """
    Atomically transition a job to PROCESSING.

    Claimable:
        PENDING
        stale PROCESSING
        FAILED with attempts remaining

    Not claimable:
        active PROCESSING
        SUCCESS
        FAILED with attempts exhausted
    """

    client = get_client()
    ref = job_ref(job_id)

    @firestore.transactional
    def _claim(transaction):

        snapshot = ref.get(
            transaction=transaction
        )

        if not snapshot.exists:
            raise JobClaimError(
                f"Job {job_id} does not exist"
            )

        data = snapshot.to_dict()

        status = data.get("status")
        attempts = data.get("attempts", 0)
        max_attempts = data.get(
            "maxAttempts",
            config.MAX_JOB_ATTEMPTS,
        )

        if status == "SUCCESS":
            raise JobClaimError(
                f"Job {job_id} already completed successfully"
            )

        if (
            status == "PROCESSING"
            and not _is_stale(
                data,
                stale_timeout_seconds,
            )
        ):
            raise JobClaimError(
                f"Job {job_id} is actively being "
                f"processed by {data.get('workerId')}"
            )

        if (
            status == "FAILED"
            and attempts >= max_attempts
        ):
            raise JobClaimError(
                f"Job {job_id} has exhausted "
                f"its {max_attempts} attempts"
            )

        transaction.update(
            ref,
            {
                "status": "PROCESSING",
                "workerId": worker_id,
                "attempts": attempts + 1,
                "startedAt": _now(),
                "updatedAt": _now(),
            },
        )

        return data

    transaction = client.transaction()

    try:
        pre_claim_data = _claim(transaction)

    except JobClaimError:
        raise

    except Exception as exc:
        raise FirebaseError(
            f"Transaction failed while claiming "
            f"job {job_id}: {exc}"
        ) from exc

    logger.info(
        "Job claimed | id=%s worker=%s",
        job_id,
        worker_id,
    )

    claimed = dict(pre_claim_data)

    claimed["id"] = job_id
    claimed["status"] = "PROCESSING"
    claimed["workerId"] = worker_id

    return claimed


def update_job_checkpoint(
    job_id: str,
    last_completed_date: Optional[str] = None,
    current_date: Optional[str] = None,
    completed_count: Optional[int] = None,
    total_count: Optional[int] = None,
) -> None:

    """
    Called after every successfully persisted date.
    """

    updates = {
        "updatedAt": _now()
    }

    if last_completed_date is not None:
        updates["lastCompletedDate"] = last_completed_date

    if current_date is not None:
        updates["currentDate"] = current_date

    if completed_count is not None:
        updates["completedCount"] = completed_count

    if total_count is not None:
        updates["totalCount"] = total_count

    try:
        job_ref(job_id).set(
            updates,
            merge=True,
        )

    except Exception as exc:
        raise FirebaseError(
            f"Failed to update checkpoint "
            f"for job {job_id}: {exc}"
        ) from exc


def finish_job(
    job_id: str,
    status: str,
    error: Optional[str] = None,
) -> None:

    if status not in (
        "SUCCESS",
        "FAILED",
    ):
        raise ValueError(
            "finish_job status must be "
            f"SUCCESS or FAILED, got {status!r}"
        )

    updates = {
        "status": status,
        "updatedAt": _now(),
        "lastError": error,
    }

    try:
        job_ref(job_id).set(
            updates,
            merge=True,
        )

        logger.info(
            "Job finished | id=%s status=%s",
            job_id,
            status,
        )

    except Exception as exc:
        raise FirebaseError(
            f"Failed to finish job {job_id}: {exc}"
        ) from exc


# ============================================================
# QUEUE
# ============================================================

def query_pending_jobs(
    job_type: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:

    """
    Find jobs that the queue worker should inspect.

    Candidate states:

        PENDING
            Normal queued work.

        PROCESSING
            claim_job() will determine whether it is stale.

        FAILED
            Included only when retry attempts remain.

    IMPORTANT:
        This function only finds candidates.

        claim_job() is still the final authority and uses a
        Firestore transaction to prevent two workers from claiming
        the same job.
    """

    try:
        query = (
            get_client()
            .collection("jobs")
        )

        if job_type:
            query = query.where(
                "type",
                "==",
                job_type,
            )

        docs = list(
            query
            .limit(limit)
            .stream()
        )

    except Exception as exc:
        raise FirebaseError(
            f"Failed to query jobs: {exc}"
        ) from exc

    results = []

    for doc in docs:

        data = doc.to_dict()
        data["id"] = doc.id

        status = data.get("status")
        attempts = data.get(
            "attempts",
            0,
        )

        max_attempts = data.get(
            "maxAttempts",
            config.MAX_JOB_ATTEMPTS,
        )

        if status == "PENDING":
            results.append(data)

        elif status == "PROCESSING":
            # claim_job() checks whether it is stale.
            results.append(data)

        elif (
            status == "FAILED"
            and attempts < max_attempts
        ):
            # Retryable failed job.
            results.append(data)

        if len(results) >= limit:
            break

    logger.info(
        "QUEUE_CANDIDATES found=%d type=%s",
        len(results),
        job_type or "any",
    )

    return results