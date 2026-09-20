"""
Exception taxonomy for the orchestration layer (jobs, Firestore, worker).

scraper.py keeps its own single ScraperError -- that is the reference
implementation's working error handling and is not touched. These
types live one layer up, in the code that decides what a ScraperError
*means* for a job (expired session? permanent failure? bad data?).
"""


class LoginError(Exception):
    """SCCE authentication itself failed (bad portal response, no login form found)."""


class SessionExpiredError(Exception):
    """
    A previously-authenticated session appears to have expired mid-job
    (the Dailywise page came back looking like a login page, or lost
    its date selector). The caller should create a fresh session and
    retry the current date -- never advance the checkpoint on this.
    """


class ScraperError(Exception):
    """A specific date could not be scraped (network, timeout, HTTP error)."""


class ParseError(Exception):
    """A response was received but its attendance table could not be parsed."""


class ValidationError(Exception):
    """
    A scraped result was structurally parseable but failed a sanity
    check (e.g. attended > conducted, empty records, date mismatch).
    Existing stored data must never be overwritten because of this.
    """


class FirebaseError(Exception):
    """A Firestore read/write/transaction failed."""


class JobClaimError(Exception):
    """
    Raised when a worker tries to claim a job that is not actually
    claimable (already PROCESSING by someone else and not stale, or
    already terminal). This is an expected, routine outcome when two
    workers race for the same job -- not a bug.
    """