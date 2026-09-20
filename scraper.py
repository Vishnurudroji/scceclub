#!/usr/bin/env python3
"""
SCCE Dailywise attendance scraper.

Standalone module. No Flask, no SQLite, no FastAPI, no Selenium.
It only fetches and parses SCCE Dailywise data and returns plain
Python structures. Persistence, duplicate prevention and
last_scraped_date bookkeeping belong to db.py.

Source of truth for overall attendance is the Dailywise report, NOT
updateddata.php. Overall totals are derived by summing the per-date
results this module returns (see calculate_overall), or, in the real
application, by summing the rows stored in SQLite.

Public API
----------
    create_authenticated_session(hall_ticket)             -> requests.Session
    get_available_dates(hall_ticket)                      -> list[str]   (ISO, oldest->newest)
    get_available_dates_from_session(session)             -> list[str]
    scrape_daily_attendance(hall_ticket, date)            -> list[dict]  (records only)
    scrape_daily_attendance_with_summary(hall_ticket, date) -> dict
    scrape_daily_attendance_with_session(session, date)   -> dict
    scrape_all_available_dates(hall_ticket)               -> list[dict]
    calculate_overall(daily_results)                      -> dict

Attendance semantics
--------------------
Atnd and Cnctd are NUMERIC COUNTS, not present/absent flags. A lab row
may legitimately read Atnd=3 / Cnctd=3. Those values are preserved
verbatim:

    daily_attended  = sum(row Atnd)
    daily_conducted = sum(row Cnctd)

No row is ever collapsed to 1/0.
"""

from __future__ import annotations
from exceptions import ScraperError
import re
import sys
import time
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = "https://scce.ac.in/parent12/"
BASE = urljoin(BASE_DIR, "index.php")
DAILY_URL = urljoin(BASE_DIR, "Dailywisereport.php")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0.0.0 Safari/537.36"
)

CONNECT_TIMEOUT = 8
READ_TIMEOUT = 20
REQUEST_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

# One retry, as specified. SCCE sometimes serves HTTP 500 with a
# perfectly usable body, so status code alone never decides.
MAX_RETRIES = 1
RETRY_BACKOFF_SECONDS = 1.5

# Courtesy delay between sequential requests to the college server.
INTER_REQUEST_DELAY = 0.7

# Input names that may carry the hall ticket on the portal's login form.
HALL_INPUT_CANDIDATES = (
    "hallticket", "hall_ticket", "hallticketno", "halltkt",
    "ht", "htno", "ht_no", "regno", "rollno", "usn",
    "userid", "username", "studentid",
)

# Date formats SCCE may use inside <option value=...> or its label.
DATE_FORMATS = (
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%d-%b-%Y",
    "%d %b %Y",
    "%d-%B-%Y",
    "%d %B %Y",
    "%m/%d/%Y",
)


logger = logging.getLogger("scce.scraper")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | scraper | %(message)s")
    )
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)





# ============================================================
# SESSION / TRANSPORT
# ============================================================

def _new_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    adapter = HTTPAdapter(pool_connections=2, pool_maxsize=2, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _has_usable_body(html: str) -> bool:
    """A 5xx response is still usable if it carries a real table."""
    if not html:
        return False
    lowered = html.lower()
    return "<table" in lowered and "</table>" in lowered


def _request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    data: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> requests.Response:
    """
    Sequential GET/POST with one retry.

    A 5xx whose body still contains a table is returned as-is, because
    the SCCE portal routinely does this. Anything else 5xx is retried
    once, then raises ScraperError.
    """
    method = method.upper()
    if method not in ("GET", "POST"):
        raise ValueError(f"Unsupported HTTP method: {method}")

    last_problem = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            if method == "GET":
                response = session.get(
                    url, timeout=REQUEST_TIMEOUT, headers=headers,
                    allow_redirects=True,
                )
            else:
                response = session.post(
                    url, data=data, timeout=REQUEST_TIMEOUT, headers=headers,
                    allow_redirects=True,
                )

            if response.status_code >= 500:
                if _has_usable_body(response.text):
                    logger.info(
                        "%s %s -> %s (body usable, continuing)",
                        method, url, response.status_code,
                    )
                    return response

                last_problem = f"HTTP {response.status_code} with no usable body"
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "%s %s -> %s | retrying once", method, url, response.status_code
                    )
                    time.sleep(RETRY_BACKOFF_SECONDS)
                    continue
                break

            if response.status_code >= 400:
                last_problem = f"HTTP {response.status_code}"
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "%s %s -> %s | retrying once", method, url, response.status_code
                    )
                    time.sleep(RETRY_BACKOFF_SECONDS)
                    continue
                break

            return response

        except requests.RequestException as exc:
            last_problem = str(exc)
            if attempt < MAX_RETRIES:
                logger.warning("%s %s failed (%s) | retrying once", method, url, exc)
                time.sleep(RETRY_BACKOFF_SECONDS)
                continue
            break

    raise ScraperError(f"{method} {url} failed: {last_problem}")


def _pause() -> None:
    time.sleep(INTER_REQUEST_DELAY)


# ============================================================
# AUTHENTICATION
# ============================================================

def _find_login_form(soup: BeautifulSoup):
    """Locate the portal form that accepts a hall ticket."""
    forms = soup.find_all("form")
    if not forms:
        return None

    for form in forms:
        for inp in form.find_all(["input", "select", "textarea"]):
            name = (inp.get("name") or "").lower()
            ident = (inp.get("id") or "").lower()
            if any(c in name or c in ident for c in HALL_INPUT_CANDIDATES):
                return form

    # Fall back to the form with the most fields.
    return max(
        forms,
        key=lambda f: len(f.find_all(["input", "select", "textarea"])),
        default=None,
    )


def _build_login_payload(form, hall_ticket: str) -> dict:
    payload: dict[str, str] = {}
    hall_assigned = False

    for inp in form.find_all(["input", "select", "textarea"]):
        name = inp.get("name")
        if not name:
            continue
        name_lower = name.lower()
        input_type = (inp.get("type") or "").lower()

        if input_type == "submit":
            payload[name] = inp.get("value") or "Submit"
            continue

        if not hall_assigned and any(c in name_lower for c in HALL_INPUT_CANDIDATES):
            payload[name] = hall_ticket
            hall_assigned = True
            continue

        if inp.name == "select":
            selected = inp.find("option", selected=True) or inp.find("option")
            if selected is not None:
                value = selected.get("value")
                payload[name] = value if value is not None else selected.get_text(strip=True)
            else:
                payload[name] = ""
            continue

        payload[name] = inp.get("value") or ""

    if not hall_assigned:
        # The portal's field name is unknown to us; use the conventional one
        # rather than failing outright.
        payload.setdefault("HallticketNo", hall_ticket)

    return payload


def create_authenticated_session(hall_ticket: str) -> requests.Session:
    """
    Open the SCCE parent portal and submit the hall ticket, returning a
    session whose cookies are bound to that student.

    Reuse the returned session for every date you need to scrape --
    re-submitting the hall ticket per date is both slow and unfriendly
    to the college server.
    """
    hall_ticket = (hall_ticket or "").strip().upper()
    if not hall_ticket:
        raise ScraperError("Hall ticket is required")

    logger.info("Starting authentication | hall=%s", hall_ticket)

    session = _new_session()
    session.headers["Referer"] = BASE

    response = _request(session, "GET", BASE)
    soup = BeautifulSoup(response.text, "lxml")

    form = _find_login_form(soup)
    if form is None:
        raise ScraperError("Could not find the hall ticket form on the SCCE portal")

    payload = _build_login_payload(form, hall_ticket)

    action = form.get("action")
    if not action:
        target = BASE
    elif action.lower().startswith("http"):
        target = action
    else:
        target = urljoin(BASE_DIR, action)

    method = (form.get("method") or "post").upper()
    if method not in ("GET", "POST"):
        method = "POST"

    _pause()
    _request(
        session,
        method,
        target,
        data=payload,
        headers={"Referer": BASE, "Origin": "https://scce.ac.in"},
    )

    logger.info("Hall ticket submitted | hall=%s", hall_ticket)
    session.headers["Referer"] = target
    return session


# ============================================================
# DATE HANDLING
# ============================================================

def _parse_date_token(token: str) -> Optional[str]:
    """Normalise one option value/label to an ISO date, or None."""
    if not token:
        return None

    token = token.strip()
    if not token or not any(ch.isdigit() for ch in token):
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(token, fmt).date().isoformat()
        except ValueError:
            continue

    # Pull a date out of a longer label, e.g. "Attendance for 18-09-2026".
    match = re.search(r"\d{1,4}[-/][A-Za-z0-9]{1,9}[-/]\d{1,4}", token)
    if match:
        fragment = match.group(0)
        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(fragment, fmt).date().isoformat()
            except ValueError:
                continue

    return None


def _find_date_select(soup: BeautifulSoup):
    """
    Locate <select name="date"> on the Dailywise page.

    Falls back to any select whose options look like dates, so a
    renamed field does not break the scraper outright.
    """
    select = soup.find("select", attrs={"name": "date"})
    if select is not None:
        return select

    for candidate in soup.find_all("select"):
        options = candidate.find_all("option")
        parsed = [
            _parse_date_token(opt.get("value") or opt.get_text(strip=True))
            for opt in options[:25]
        ]
        if sum(1 for value in parsed if value) >= 2:
            return candidate

    return None


def _extract_date_options(soup: BeautifulSoup) -> dict[str, str]:
    """
    Map ISO date -> the exact option value that must be POSTed back.

    Keeping the raw value matters: the portal may expect '18-09-2026'
    even though we key everything internally on '2026-09-18'.
    """
    select = _find_date_select(soup)
    if select is None:
        raise ScraperError("Could not find the date selector on the Dailywise report page")

    mapping: dict[str, str] = {}

    for option in select.find_all("option"):
        raw_value = option.get("value")
        label = option.get_text(strip=True)
        raw = raw_value if raw_value not in (None, "") else label

        iso = _parse_date_token(raw) or _parse_date_token(label)
        if iso is None:
            continue

        # First occurrence wins; this de-duplicates repeated dates.
        mapping.setdefault(iso, raw)

    if not mapping:
        raise ScraperError("Date selector found but contains no recognisable dates")

    return mapping


def _open_dailywise(session: requests.Session) -> BeautifulSoup:
    logger.info("Opening Dailywise Report")
    response = _request(session, "GET", DAILY_URL, headers={"Referer": BASE})
    soup = BeautifulSoup(response.text, "lxml")
    if soup.find("select") is None and not _has_usable_body(response.text):
        raise ScraperError("Dailywise report page could not be opened")
    return soup


def get_available_dates_from_session(session: requests.Session) -> list[str]:
    """Available attendance dates as ISO strings, oldest -> newest, de-duplicated."""
    soup = _open_dailywise(session)
    mapping = _extract_date_options(soup)
    dates = sorted(mapping)
    logger.info("Available dates found: %d", len(dates))
    return dates


def get_available_dates(hall_ticket: str) -> list[str]:
    """Convenience wrapper that authenticates first. Prefer the session variant in loops."""
    session = create_authenticated_session(hall_ticket)
    try:
        _pause()
        return get_available_dates_from_session(session)
    finally:
        session.close()


# ============================================================
# TABLE PARSING
# ============================================================

_HEADER_ALIASES = {
    "sno": ("sno", "s.no", "s no", "slno", "sl.no", "serial"),
    "hour": ("hour", "period", "hr"),
    "subject": ("sub", "subject", "subj", "subcode", "sub code"),
    "attended": ("atnd", "attended", "attend", "present"),
    "conducted": ("cnctd", "conducted", "conduct", "held", "total"),
}


def _normalise_header(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (text or "").lower())


def _classify_header(cell_text: str) -> Optional[str]:
    """Map one header cell to a semantic field name."""
    normalised = _normalise_header(cell_text)
    if not normalised:
        return None

    for field, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            if normalised == _normalise_header(alias):
                return field

    # Loose contains-match, longest alias first, so 'cnctd' is not
    # shadowed by a shorter alias of another field.
    best: tuple[int, Optional[str]] = (0, None)
    for field, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            key = _normalise_header(alias)
            if key and key in normalised and len(key) > best[0]:
                best = (len(key), field)
    return best[1]


def _row_cells(tr) -> list[str]:
    return [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]


def _find_attendance_table(soup: BeautifulSoup):
    """
    Return (table, column_map) for the table carrying Atnd and Cnctd.

    column_map maps semantic field -> column index, derived from the
    header row rather than from fixed positions.
    """
    for table in soup.find_all("table"):
        for tr in table.find_all("tr")[:5]:
            cells = _row_cells(tr)
            if len(cells) < 2:
                continue

            column_map: dict[str, int] = {}
            for index, cell in enumerate(cells):
                field = _classify_header(cell)
                if field and field not in column_map:
                    column_map[field] = index

            if "attended" in column_map and "conducted" in column_map:
                return table, column_map

    return None, None


def _to_int(value: str) -> Optional[int]:
    """Strict-ish integer parse. '3' -> 3, '3.0' -> 3, '' / '-' / 'NA' -> None."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    match = re.fullmatch(r"(\d+)(?:\.0+)?", text)
    if match:
        return int(match.group(1))
    return None


def _looks_like_total_row(cells: list[str]) -> bool:
    joined = " ".join(cells).lower()
    return "total" in joined or "grand" in joined


def _parse_attendance_rows(soup: BeautifulSoup, date: str) -> list[dict]:
    """
    Parse the Dailywise table into per-row records.

    Atnd and Cnctd are preserved as the integers SCCE reports. A lab
    row of Atnd=3 / Cnctd=3 stays 3 and 3 -- it is never collapsed to
    a present/absent flag.

    Any 'Total' row is ignored; the prompt's warning that it can be
    empty or unreliable is why totals are recomputed from the rows.
    """
    table, column_map = _find_attendance_table(soup)
    if table is None:
        raise ScraperError(f"Attendance table not found for {date}")

    idx_sno = column_map.get("sno")
    idx_hour = column_map.get("hour")
    idx_subject = column_map.get("subject")
    idx_attended = column_map["attended"]
    idx_conducted = column_map["conducted"]

    needed = max(idx_attended, idx_conducted) + 1

    records: list[dict] = []
    malformed = 0

    for tr in table.find_all("tr"):
        cells = _row_cells(tr)
        if len(cells) < needed:
            continue

        # Skip the header row itself.
        if _classify_header(cells[idx_attended]) == "attended":
            continue

        if _looks_like_total_row(cells):
            continue

        attended = _to_int(cells[idx_attended])
        conducted = _to_int(cells[idx_conducted])

        if attended is None or conducted is None:
            # A row with non-numeric Atnd/Cnctd that is not a header or
            # total row is genuinely malformed -- count it, don't guess.
            if any(cell.strip() for cell in cells):
                malformed += 1
            continue

        if attended > conducted:
            raise ScraperError(
                f"Invalid row on {date}: attended={attended} exceeds conducted={conducted}"
            )

        records.append({
            "sno": cells[idx_sno].strip() if idx_sno is not None and idx_sno < len(cells) else str(len(records) + 1),
            "hour": cells[idx_hour].strip() if idx_hour is not None and idx_hour < len(cells) else "",
            "subject": cells[idx_subject].strip() if idx_subject is not None and idx_subject < len(cells) else "",
            "attended": attended,
            "conducted": conducted,
        })

    if not records:
        raise ScraperError(
            f"No parsable attendance rows for {date} "
            f"({malformed} malformed row(s) seen)"
        )

    return records


def _summarise(date: str, records: list[dict]) -> dict:
    attended = sum(r["attended"] for r in records)
    conducted = sum(r["conducted"] for r in records)

    if conducted <= 0:
        raise ScraperError(f"Conducted count is zero for {date}; refusing to report attendance")

    if attended > conducted:
        raise ScraperError(
            f"Invalid totals for {date}: attended={attended} exceeds conducted={conducted}"
        )

    return {
        "date": date,
        "records": records,
        "attended": attended,
        "conducted": conducted,
        "percentage": round(attended / conducted * 100, 2),
    }


# ============================================================
# SCRAPING ONE DATE
# ============================================================

def _build_daily_payload(soup: BeautifulSoup, raw_date_value: str) -> tuple[str, dict]:
    """
    Build the POST target and payload for the Dailywise date form.

    Mirrors the real form: <form id="foo55" name="Reg" method="post"
    action="Dailywisereport.php"> containing <select name="date"> and
    <input type="submit" name="dayatten">.
    """
    select = _find_date_select(soup)
    if select is None:
        raise ScraperError("Date selector disappeared from the Dailywise page")

    select_name = select.get("name") or "date"
    form = select.find_parent("form")

    payload: dict[str, str] = {select_name: raw_date_value}
    submit_assigned = False

    if form is not None:
        for inp in form.find_all("input"):
            name = inp.get("name")
            if not name:
                continue
            input_type = (inp.get("type") or "").lower()

            if input_type == "submit":
                payload[name] = inp.get("value") or "Submit"
                submit_assigned = True
            elif input_type in ("checkbox", "radio"):
                if inp.has_attr("checked"):
                    payload[name] = inp.get("value") or "on"
            else:
                payload.setdefault(name, inp.get("value") or "")

        for other in form.find_all("select"):
            other_name = other.get("name")
            if not other_name or other_name == select_name:
                continue
            chosen = other.find("option", selected=True) or other.find("option")
            if chosen is not None:
                payload.setdefault(
                    other_name,
                    chosen.get("value") if chosen.get("value") is not None
                    else chosen.get_text(strip=True),
                )

    if not submit_assigned:
        # The portal's submit control is named 'dayatten'; PHP branches on it.
        payload.setdefault("dayatten", "Submit")

    target = DAILY_URL
    if form is not None and form.get("action"):
        action = form.get("action")
        target = action if action.lower().startswith("http") else urljoin(BASE_DIR, action)

    return target, payload


def scrape_daily_attendance_with_session(session: requests.Session, date: str) -> dict:
    """
    Scrape one date using an already-authenticated session.

    date: ISO 'YYYY-MM-DD'. It must appear in SCCE's own date selector;
    if it does not, ScraperError is raised rather than substituting a
    nearby date.
    """
    iso = _parse_date_token(date)
    if iso is None:
        raise ScraperError(f"Unrecognised date format: {date!r} (expected YYYY-MM-DD)")

    soup = _open_dailywise(session)
    mapping = _extract_date_options(soup)

    if iso not in mapping:
        raise ScraperError(f"Date {iso} is not offered by the SCCE date selector")

    target, payload = _build_daily_payload(soup, mapping[iso])

    logger.info("Scraping date: %s", iso)
    _pause()
    response = _request(
        session, "POST", target, data=payload, headers={"Referer": DAILY_URL}
    )

    result_soup = BeautifulSoup(response.text, "lxml")
    records = _parse_attendance_rows(result_soup, iso)
    summary = _summarise(iso, records)

    logger.info(
        "Parsed: attended=%d conducted=%d | rows=%d",
        summary["attended"], summary["conducted"], len(records),
    )
    logger.info("Date completed: %s", iso)
    return summary


def scrape_daily_attendance_with_summary(hall_ticket: str, date: str) -> dict:
    """Authenticate, scrape one date, return records plus totals."""
    session = create_authenticated_session(hall_ticket)
    try:
        return scrape_daily_attendance_with_session(session, date)
    except ScraperError:
        logger.warning("Date failed: %s", date)
        raise
    finally:
        session.close()


def scrape_daily_attendance(hall_ticket: str, date: str) -> list[dict]:
    """
    Records-only variant.

    Returns the per-period rows for one date. Each row keeps its real
    numeric Atnd/Cnctd values.
    """
    return scrape_daily_attendance_with_summary(hall_ticket, date)["records"]


# ============================================================
# FULL SYNCHRONISATION (NEW STUDENT)
# ============================================================

def scrape_all_available_dates(
    hall_ticket: str,
    after_date: Optional[str] = None,
) -> list[dict]:
    """
    Scrape every date SCCE offers, oldest -> newest, on ONE session.

    after_date (optional, ISO): only dates strictly newer are scraped.
    Pass a student's last_scraped_date here to do an incremental update
    without re-authenticating per date.

    Stops at the first failing date and raises ScraperError. Nothing is
    skipped silently and no date is fabricated -- whatever succeeded
    before the failure is lost to the caller, so the caller should
    persist results as it goes if partial progress matters. (See
    scrape_dates_with_session for a callback-free variant that lets you
    save incrementally.)
    """
    hall_ticket = (hall_ticket or "").strip().upper()
    session = create_authenticated_session(hall_ticket)

    try:
        _pause()
        dates = get_available_dates_from_session(session)

        if after_date:
            cutoff = _parse_date_token(after_date)
            if cutoff is None:
                raise ScraperError(f"Unrecognised after_date: {after_date!r}")
            dates = [d for d in dates if d > cutoff]

        if not dates:
            logger.info("No dates to synchronise | hall=%s", hall_ticket)
            return []

        results: list[dict] = []
        for iso in dates:
            try:
                results.append(scrape_daily_attendance_with_session(session, iso))
            except ScraperError as exc:
                logger.error("Date failed: %s | %s", iso, exc)
                raise ScraperError(
                    f"Synchronisation stopped at {iso}: {exc}"
                ) from exc

        logger.info(
            "Initial synchronization completed | hall=%s | dates=%d",
            hall_ticket, len(results),
        )
        return results

    finally:
        session.close()


def scrape_dates_with_session(
    session: requests.Session,
    dates: list[str],
    stop_on_error: bool = True,
) -> tuple[list[dict], Optional[str]]:
    """
    Scrape a caller-supplied list of dates on one session.

    Returns (results, error_message). This is the function to use from
    app.py when you want to persist each date as it arrives, so a
    failure partway through does not discard earlier work.

    With stop_on_error=True (default) the first failure ends the run --
    a date is never skipped over, which keeps stored history contiguous.
    """
    results: list[dict] = []

    for iso in dates:
        try:
            results.append(scrape_daily_attendance_with_session(session, iso))
        except ScraperError as exc:
            logger.error("Date failed: %s | %s", iso, exc)
            if stop_on_error:
                return results, f"{iso}: {exc}"
            return results, f"{iso}: {exc}"

    return results, None


# ============================================================
# AGGREGATION
# ============================================================

def calculate_overall(daily_results: list[dict]) -> dict:
    """
    Sum daily results into overall totals.

    Accepts any iterable of dicts carrying 'attended' and 'conducted'.
    The percentage is computed from the summed counts, never by
    averaging per-day or per-subject percentages.
    """
    attended = 0
    conducted = 0

    for entry in daily_results or []:
        try:
            day_attended = int(entry["attended"])
            day_conducted = int(entry["conducted"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ScraperError(f"Malformed daily result passed to calculate_overall: {entry!r}") from exc

        if day_attended < 0 or day_conducted < 0:
            raise ScraperError(f"Negative counts in daily result: {entry!r}")
        if day_attended > day_conducted:
            raise ScraperError(f"attended exceeds conducted in daily result: {entry!r}")

        attended += day_attended
        conducted += day_conducted

    percentage = round(attended / conducted * 100, 2) if conducted > 0 else 0.0

    return {"attended": attended, "conducted": conducted, "percentage": percentage}


# ============================================================
# MANUAL TEST
# ============================================================

def _main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scraper.py <HALL_TICKET> [YYYY-MM-DD]")
        return 1

    hall_ticket = sys.argv[1].strip().upper()
    requested_date = sys.argv[2].strip() if len(sys.argv) >= 3 else None

    try:
        session = create_authenticated_session(hall_ticket)
        try:
            _pause()
            dates = get_available_dates_from_session(session)
            if not dates:
                print("No attendance dates available on the portal.")
                return 1

            target_date = requested_date or dates[-1]
            if requested_date and requested_date not in dates:
                print(f"Date {requested_date} is not offered by SCCE.")
                print(f"Available: {', '.join(dates)}")
                return 1

            result = scrape_daily_attendance_with_session(session, target_date)
        finally:
            session.close()

    except ScraperError as exc:
        print(f"ERROR: {exc}")
        return 1

    print("\n================ RESULT ================\n")
    print(f"Hall Ticket : {hall_ticket}")
    print(f"Date        : {result['date']}")
    print(f"Attended    : {result['attended']}")
    print(f"Conducted   : {result['conducted']}")
    print(f"Percentage  : {result['percentage']}%")
    print("\nPeriod records:")
    print(f"  {'Sno':<5}{'Hour':<6}{'Subject':<14}{'Atnd':>5}{'Cnctd':>7}")
    for row in result["records"]:
        print(
            f"  {row['sno']:<5}{row['hour']:<6}{row['subject']:<14}"
            f"{row['attended']:>5}{row['conducted']:>7}"
        )
    print()
    return 0


if __name__ == "__main__":
    sys.exit(_main())