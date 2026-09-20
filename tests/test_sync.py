import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fake_firestore as ff  # noqa: E402  (tests/ dir, see sys.path above)

import config  # noqa: E402
import firestore_repo  # noqa: E402
import scraper  # noqa: E402
import sync_common  # noqa: E402
import historical_sync  # noqa: E402
import daily_sync  # noqa: E402
import worker  # noqa: E402

failures = []


def check(label, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {label}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(label)


# =====================================================================
# WIRE THE FAKES IN
# =====================================================================

_fake_client = ff.FakeFirestoreClient()


def reset_firestore():
    _fake_client.store.clear()
    ff.FakeClock._t = 0


firestore_repo.get_client = lambda: _fake_client
firestore_repo.firestore.transactional = ff.transactional
firestore_repo.firestore.SERVER_TIMESTAMP = ff.SERVER_TIMESTAMP
firestore_repo._now = lambda: ff.SERVER_TIMESTAMP
firestore_repo._utcnow = lambda: ff.FakeClock.now()


def _is_stale_ticks(job_data, stale_timeout_ticks):
    updated_at = job_data.get("updatedAt")
    if updated_at is None:
        return True
    return (ff.FakeClock.now() - updated_at) > stale_timeout_ticks


firestore_repo._is_stale = _is_stale_ticks


# Fake scraper: controllable per-date behaviour, no real network.
class FakeScraperState:
    def __init__(self):
        self.all_dates = []
        self.fail_on = set()
        self.expire_session_on = set()   # date at which the CURRENT session looks expired
        self.transient_fail_count = {}   # date -> number of times to fail before succeeding
        self.calls = {"auth": [], "dates": [], "scrape": []}
        self.session_counter = 0


FS = FakeScraperState()


class FakeSession:
    def __init__(self, hall_ticket, generation):
        self.hall_ticket = hall_ticket
        self.generation = generation
        self.closed = False

    def close(self):
        self.closed = True


def fake_create_authenticated_session(hall_ticket):
    FS.session_counter += 1
    FS.calls["auth"].append(hall_ticket)
    return FakeSession(hall_ticket, FS.session_counter)


def fake_get_available_dates_from_session(session):
    FS.calls["dates"].append(session.hall_ticket)
    return list(FS.all_dates)


def fake_scrape_daily_attendance_with_session(session, date):
    FS.calls["scrape"].append((session.hall_ticket, date, session.generation))

    if date in FS.expire_session_on and session.generation == 1:
        # Simulate: this session's cookies are stale, portal serves the
        # login page instead of the Dailywise report.
        raise scraper.ScraperError("Could not find the date selector on the Dailywise report page")

    remaining = FS.transient_fail_count.get(date, 0)
    if remaining > 0:
        FS.transient_fail_count[date] = remaining - 1
        raise scraper.ScraperError(f"Read timed out scraping {date}")

    if date in FS.fail_on:
        raise scraper.ScraperError(f"Attendance table not found for {date}")

    return {
        "date": date,
        "records": [{"sno": "1", "hour": "1", "subject": "CD", "attended": 1, "conducted": 1}],
        "attended": 1,
        "conducted": 1,
        "percentage": 100.0,
    }


scraper.create_authenticated_session = fake_create_authenticated_session
scraper.get_available_dates_from_session = fake_get_available_dates_from_session
scraper.scrape_daily_attendance_with_session = fake_scrape_daily_attendance_with_session

config.RETRY_BASE_DELAY_SECONDS = 0  # don't actually sleep in tests
config.RETRY_MAX_ATTEMPTS = 3
config.STALE_JOB_TIMEOUT_SECONDS = 5


def reset_scraper(all_dates):
    FS.all_dates = list(all_dates)
    FS.fail_on = set()
    FS.expire_session_on = set()
    FS.transient_fail_count = {}
    FS.calls = {"auth": [], "dates": [], "scrape": []}
    FS.session_counter = 0


# =====================================================================
print("=" * 70); print("TEST 1: successful historical sync, all dates")
reset_firestore(); reset_scraper(["2026-07-06", "2026-07-07", "2026-07-08"])

worker.cmd_register(type("A", (), {"student": "23N01A0001"})())
result = historical_sync.run_historical_sync("23N01A0001")
check("status SUCCESS", result["status"] == "SUCCESS", result)
check("3 dates processed", result["processed"] == 3, result["processed"])
student = firestore_repo.get_student("23N01A0001")
check("lastScrapedDate advanced to newest", student["lastScrapedDate"] == "2026-07-08")
check("firstScrapedDate set", student["firstScrapedDate"] == "2026-07-06")
job = firestore_repo.get_job(firestore_repo.initial_sync_job_id("23N01A0001"))
check("job status SUCCESS", job["status"] == "SUCCESS")
check("job completedCount == totalCount", job["completedCount"] == job["totalCount"] == 3)
for d in ["2026-07-06", "2026-07-07", "2026-07-08"]:
    check(f"attendance doc exists for {d}", firestore_repo.get_attendance_date("23N01A0001", d) is not None)


# =====================================================================
print("=" * 70); print("TEST 2: worker crash mid-historical-sync, then resume (checkpoint after every date)")
reset_firestore(); reset_scraper([f"2026-07-{d:02d}" for d in range(6, 16)])  # 10 dates
FS.fail_on = {"2026-07-11"}  # 6th date fails

worker.cmd_register(type("A", (), {"student": "23N01A0002"})())
r1 = historical_sync.run_historical_sync("23N01A0002")
check("first run PARTIAL-as-FAILED with progress", r1["status"] == "FAILED" and r1["processed"] == 5, r1)
student = firestore_repo.get_student("23N01A0002")
check("checkpoint reflects 5 successful dates", student["lastScrapedDate"] == "2026-07-10")
for d in [f"2026-07-{n:02d}" for n in range(6, 11)]:
    check(f"date {d} persisted before the crash", firestore_repo.get_attendance_date("23N01A0002", d) is not None)
check("failed date NOT persisted", firestore_repo.get_attendance_date("23N01A0002", "2026-07-11") is None)

# "Restart": SCCE recovers, worker (possibly a different process) reruns the same command.
FS.fail_on = set()
scrape_calls_before = len(FS.calls["scrape"])
r2 = historical_sync.run_historical_sync("23N01A0002")
scraped_after_resume = [d for _, d, _ in FS.calls["scrape"][scrape_calls_before:]]
check("resume did NOT rescrape dates 1-5", not any(d in [f"2026-07-{n:02d}" for n in range(6, 11)] for d in scraped_after_resume), scraped_after_resume)
check("resume started at the failed date", scraped_after_resume[0] == "2026-07-11" if scraped_after_resume else False)
check("resume completes SUCCESS", r2["status"] == "SUCCESS", r2)
student2 = firestore_repo.get_student("23N01A0002")
check("checkpoint now at the latest date", student2["lastScrapedDate"] == "2026-07-15")


# =====================================================================
print("=" * 70); print("TEST 3: duplicate job claim -- two workers race for the same job")
reset_firestore(); reset_scraper(["2026-09-18"])
worker.cmd_register(type("A", (), {"student": "23N01A0003"})())
job_id = firestore_repo.initial_sync_job_id("23N01A0003")

claim_a = firestore_repo.claim_job(job_id, "worker-A")
check("worker A claims successfully", claim_a["workerId"] == "worker-A")

try:
    firestore_repo.claim_job(job_id, "worker-B")
    check("worker B is rejected (JobClaimError)", False)
except Exception as exc:
    from exceptions import JobClaimError
    check("worker B is rejected (JobClaimError)", isinstance(exc, JobClaimError), str(exc))

job_after = firestore_repo.get_job(job_id)
check("job still shows worker A as owner", job_after["workerId"] == "worker-A")


# =====================================================================
print("=" * 70); print("TEST 4: stale job recovery -- a crashed worker's lock expires")
reset_firestore(); reset_scraper(["2026-09-18"])
worker.cmd_register(type("A", (), {"student": "23N01A0004"})())
job_id4 = firestore_repo.initial_sync_job_id("23N01A0004")

firestore_repo.claim_job(job_id4, "worker-crashed", stale_timeout_seconds=5)
# Time passes (simulated) -- worker-crashed never checkpoints again.
ff.FakeClock._t += 10

try:
    firestore_repo.claim_job(job_id4, "worker-B", stale_timeout_seconds=5)
    reclaimed = True
except Exception:
    reclaimed = False
check("a stale PROCESSING job becomes claimable again", reclaimed)
job4 = firestore_repo.get_job(job_id4)
check("new worker now owns the job", job4["workerId"] == "worker-B")

# But an ACTIVE (recently-checkpointed) job must NOT be reclaimed.
reset_firestore(); reset_scraper(["2026-09-18"])
worker.cmd_register(type("A", (), {"student": "23N01A0005"})())
job_id5 = firestore_repo.initial_sync_job_id("23N01A0005")
firestore_repo.claim_job(job_id5, "worker-active", stale_timeout_seconds=100)
firestore_repo.update_job_checkpoint(job_id5, current_date="2026-09-18")  # refreshes updatedAt
ff.FakeClock._t += 10  # well under the 100-tick stale timeout
try:
    firestore_repo.claim_job(job_id5, "worker-B", stale_timeout_seconds=100)
    wrongly_reclaimed = True
except Exception:
    wrongly_reclaimed = False
check("an actively-checkpointing job is NOT reclaimed", not wrongly_reclaimed)


# =====================================================================
print("=" * 70); print("TEST 5: session expiration mid-job -- transparent re-authentication")
reset_firestore(); reset_scraper(["2026-09-16", "2026-09-17", "2026-09-18"])
FS.expire_session_on = {"2026-09-17"}  # session dies right on the 2nd date

worker.cmd_register(type("A", (), {"student": "23N01A0006"})())
result5 = historical_sync.run_historical_sync("23N01A0006")
check("job still completes SUCCESS despite mid-job expiry", result5["status"] == "SUCCESS", result5)
check("all 3 dates stored", result5["processed"] == 3)
generations_used = sorted({gen for _, _, gen in FS.calls["scrape"]})
check("a second (fresh) session generation was used", len(generations_used) >= 2, generations_used)
check("exactly one re-authentication happened", len(FS.calls["auth"]) == 2, FS.calls["auth"])


# =====================================================================
print("=" * 70); print("TEST 6: HTTP 500 / timeout -- transient failure recovers via retry-with-backoff")
reset_firestore(); reset_scraper(["2026-09-18"])
FS.transient_fail_count = {"2026-09-18": 2}  # fails twice, succeeds on 3rd attempt

worker.cmd_register(type("A", (), {"student": "23N01A0007"})())
result6 = historical_sync.run_historical_sync("23N01A0007")
check("eventually succeeds after transient failures", result6["status"] == "SUCCESS", result6)
attempts_for_date = [c for c in FS.calls["scrape"] if c[1] == "2026-09-18"]
check("retried the expected number of times", len(attempts_for_date) == 3, len(attempts_for_date))


# =====================================================================
print("=" * 70); print("TEST 7: permanent parser/attendance failure -> FAILED, not silently zero")
reset_firestore(); reset_scraper(["2026-09-18"])
FS.fail_on = {"2026-09-18"}  # every attempt fails (permanent-looking)

worker.cmd_register(type("A", (), {"student": "23N01A0008"})())
result7 = historical_sync.run_historical_sync("23N01A0008")
check("reports FAILED", result7["status"] == "FAILED", result7)
check("no attendance fabricated", firestore_repo.get_attendance_date("23N01A0008", "2026-09-18") is None)
check("student's checkpoint stays None (never touched)", firestore_repo.get_student("23N01A0008")["lastScrapedDate"] is None)


# =====================================================================
print("=" * 70); print("TEST 8: daily sync skips a date already in Firestore (no SCCE contact)")
reset_firestore(); reset_scraper(["2026-09-19"])
worker.cmd_register(type("A", (), {"student": "23N01A0009"})())
firestore_repo.update_student_checkpoint("23N01A0009", last_scraped_date="2026-09-18")
firestore_repo.save_attendance_date("23N01A0009", {
    "date": "2026-09-19", "records": [{"sno": "1", "hour": "1", "subject": "CD", "attended": 1, "conducted": 1}],
    "attended": 1, "conducted": 1, "percentage": 100.0,
})

result8 = daily_sync.run_daily_sync("23N01A0009", target_date="2026-09-19")
check("reports SUCCESS/skipped", result8.get("skipped") is True and result8["status"] == "SUCCESS", result8)
check("no SCCE auth call was made", len(FS.calls["auth"]) == 0, FS.calls["auth"])


# =====================================================================
print("=" * 70); print("TEST 9: daily sync idempotency -- processing the same date twice never duplicates")
reset_firestore(); reset_scraper(["2026-09-19"])
worker.cmd_register(type("A", (), {"student": "23N01A0010"})())
firestore_repo.update_student_checkpoint("23N01A0010", last_scraped_date="2026-09-18")

r_first = daily_sync.run_daily_sync("23N01A0010", target_date="2026-09-19", worker_id="w1")
check("first run succeeds", r_first["status"] == "SUCCESS", r_first)

# Simulate a retry of the exact same job (e.g. an operator or a
# duplicate cron trigger) after it already finished.
job_id10 = firestore_repo.daily_sync_job_id("23N01A0010", "2026-09-19")
try:
    firestore_repo.claim_job(job_id10, "w2")
    reclaimed_after_success = True
except Exception:
    reclaimed_after_success = False
check("a SUCCESS job cannot be re-claimed", not reclaimed_after_success)

before_docs = dict(_fake_client.store)
r_again = daily_sync.run_daily_sync("23N01A0010", target_date="2026-09-19", worker_id="w2")
check("re-running the same daily sync is a safe no-op", r_again.get("claimed") is False)
check("attendance document identical, no duplicate", _fake_client.store == before_docs)


# =====================================================================
print("=" * 70); print("TEST 10: Firebase failure during save preserves old data (no overwrite with bad state)")
reset_firestore(); reset_scraper(["2026-09-18"])
worker.cmd_register(type("A", (), {"student": "23N01A0011"})())

original_save = firestore_repo.save_attendance_date
def failing_save(hall_ticket, scraped):
    raise Exception("Simulated Firestore outage")
firestore_repo.save_attendance_date = failing_save

result10 = historical_sync.run_historical_sync("23N01A0011")
check("job reports FAILED on Firestore write failure", result10["status"] == "FAILED", result10)
check("student checkpoint untouched", firestore_repo.get_student("23N01A0011")["lastScrapedDate"] is None)
firestore_repo.save_attendance_date = original_save


# =====================================================================
print("=" * 70); print("TEST 11: two students processed independently (own sessions, own checkpoints)")
reset_firestore(); reset_scraper(["2026-09-18"])
worker.cmd_register(type("A", (), {"student": "23N01A0012"})())
worker.cmd_register(type("A", (), {"student": "23N01A0013"})())

r_s1 = historical_sync.run_historical_sync("23N01A0012")
r_s2 = historical_sync.run_historical_sync("23N01A0013")
check("both succeed independently", r_s1["status"] == "SUCCESS" and r_s2["status"] == "SUCCESS")
check("two separate auth calls", FS.calls["auth"] == ["23N01A0012", "23N01A0013"])
check("each has its own attendance doc",
      firestore_repo.get_attendance_date("23N01A0012", "2026-09-18") is not None and
      firestore_repo.get_attendance_date("23N01A0013", "2026-09-18") is not None)


# =====================================================================
print("=" * 70)
if failures:
    print(f"{len(failures)} FAILURE(S): {failures}")
else:
    print("ALL TESTS PASSED")