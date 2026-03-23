"""Economic calendar helpers for event-date aware strategies."""

from __future__ import annotations

import json
import re
from calendar import monthrange
from datetime import UTC, date, datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Iterable

import requests

FED_BASE_URL = "https://www.federalreserve.gov"
FOMC_CALENDAR_URL = f"{FED_BASE_URL}/monetarypolicy/fomccalendars.htm"
FOMC_HISTORICAL_INDEX_URL = f"{FED_BASE_URL}/monetarypolicy/fomc_historical_year.htm"
FOMC_CACHE_MAX_AGE_DAYS = 30
FOMC_HISTORICAL_MIN_YEAR = 1990

NFP_FRED_RELEASE_ID = 50  # Employment Situation
NFP_FRED_CALENDAR_URL_TEMPLATE = "https://fred.stlouisfed.org/releases/calendar?rid={rid}&y={year}"
NFP_BLS_ARCHIVE_URL_TEMPLATE = "https://www.bls.gov/news.release/archives/empsit_{mmddyyyy}.htm"
NFP_CACHE_MAX_AGE_DAYS = 30
NFP_HISTORY_YEARS = 15
NFP_SCAN_OFFSETS = (0, 7, 1, -1, 2, -2, 8, 3, -3, 9, 10, 11, 12, 13, 14)
NFP_MIN_EXPECTED_DATES = 24

FOMC_CACHE_FILENAME = "fomc_dates_cache.json"
NFP_CACHE_FILENAME = "nfp_dates_cache.json"
_HISTORICAL_YEAR_LINK_RE = re.compile(
    r'href=["\'](/monetarypolicy/fomchistorical(\d{4})\.htm)["\']',
    re.IGNORECASE,
)
_DATE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"fomc(?:press|pres)conf(\d{8})", re.IGNORECASE),
    re.compile(r"fomcminutes(\d{8})", re.IGNORECASE),
    re.compile(r"/fomc/minutes/(\d{8})\.htm", re.IGNORECASE),
    re.compile(
        r"FOMC(\d{8})(?:meeting|agenda|material|tealbook|gbpt|gbsup|bluebook|sep|confcall)",
        re.IGNORECASE,
    ),
)
_FRED_RELEASE_DATE_LINE_RE = re.compile(
    r"^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+"
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+"
    r"\d{1,2},\s+\d{4}$"
)


def _default_cache_path(filename: str) -> Path:
    return Path(__file__).resolve().parent.parent / "download" / filename


def _is_cache_fresh(cache_path: Path, max_age_days: int) -> bool:
    if not cache_path.exists():
        return False
    modified = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=UTC)
    return datetime.now(tz=UTC) - modified <= timedelta(days=max_age_days)


def _load_cached_dates(cache_path: Path) -> set[str]:
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    raw_dates = payload.get("dates", [])
    if not isinstance(raw_dates, list):
        return set()
    return {str(value) for value in raw_dates if isinstance(value, str)}


def _write_cache(cache_path: Path, dates: Iterable[str], source: str) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source": source,
        "fetched_at_utc": datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dates": sorted(set(dates)),
    }
    cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _to_iso_date(raw_yyyymmdd: str) -> str | None:
    try:
        parsed = datetime.strptime(raw_yyyymmdd, "%Y%m%d")
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


def _extract_dates_from_html(html: str) -> set[str]:
    dates: set[str] = set()
    for pattern in _DATE_PATTERNS:
        for raw in pattern.findall(html):
            normalized = _to_iso_date(raw)
            if normalized:
                dates.add(normalized)
    return dates


def _fetch_url_text(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def _discover_historical_page_urls(index_html: str) -> list[str]:
    page_urls: set[str] = set()
    for rel_path, raw_year in _HISTORICAL_YEAR_LINK_RE.findall(index_html):
        try:
            year = int(raw_year)
        except ValueError:
            continue
        if year < FOMC_HISTORICAL_MIN_YEAR:
            continue
        page_urls.add(f"{FED_BASE_URL}{rel_path}")
    return sorted(page_urls)


def _fetch_fomc_dates_from_fed() -> set[str]:
    with requests.Session() as session:
        session.headers.update({"User-Agent": "ResearchTool/1.0 (FOMC date fetcher)"})

        all_dates: set[str] = set()

        calendar_html = _fetch_url_text(session, FOMC_CALENDAR_URL)
        all_dates.update(_extract_dates_from_html(calendar_html))

        historical_index_html = _fetch_url_text(session, FOMC_HISTORICAL_INDEX_URL)
        for page_url in _discover_historical_page_urls(historical_index_html):
            try:
                page_html = _fetch_url_text(session, page_url)
            except requests.RequestException:
                continue
            all_dates.update(_extract_dates_from_html(page_html))

        return all_dates


def _bls_request_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }


def _first_friday_of_month(year: int, month: int) -> date:
    day = date(year, month, 1)
    while day.weekday() != 4:
        day += timedelta(days=1)
    return day


def _candidate_dates_for_nfp_month(year: int, month: int) -> list[date]:
    base = _first_friday_of_month(year, month)
    days_in_month = monthrange(year, month)[1]
    candidates: list[date] = []
    seen: set[date] = set()
    for offset in NFP_SCAN_OFFSETS:
        candidate = base + timedelta(days=int(offset))
        if candidate.month != month:
            continue
        if not (1 <= candidate.day <= days_in_month):
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        candidates.append(candidate)
    return candidates


def _bls_archive_url_for_day(day: date) -> str:
    return NFP_BLS_ARCHIVE_URL_TEMPLATE.format(mmddyyyy=day.strftime("%m%d%Y"))


def _head_url_status(session: requests.Session, url: str) -> int | None:
    response = session.head(url, timeout=10, allow_redirects=True)
    return int(response.status_code)


def _approximate_nfp_dates(*, history_years: int = NFP_HISTORY_YEARS) -> set[str]:
    """Fallback approximation when remote calendars are temporarily unavailable."""
    today = datetime.now(tz=UTC).date()
    current_year = today.year
    start_year = max(1990, current_year - int(max(1, history_years)))
    approx: set[str] = set()
    for year in range(start_year, current_year + 1):
        for month in range(1, 13):
            if year == current_year and month > today.month:
                continue
            first_friday = _first_friday_of_month(year, month)
            approx.add(first_friday.strftime("%Y-%m-%d"))
    return approx


def _fetch_nfp_archive_dates_from_bls(*, history_years: int = NFP_HISTORY_YEARS) -> set[str]:
    today = datetime.now(tz=UTC).date()
    current_year = today.year
    start_year = max(1990, current_year - int(max(1, history_years)))
    discovered: set[str] = set()
    rate_limited_count = 0

    with requests.Session() as session:
        session.headers.update(_bls_request_headers())
        for year in range(int(start_year), current_year + 1):
            for month in range(1, 13):
                if year == current_year and month > today.month:
                    continue
                month_match: str | None = None
                for candidate in _candidate_dates_for_nfp_month(year, month):
                    if candidate > today:
                        continue
                    try:
                        status = _head_url_status(session, _bls_archive_url_for_day(candidate))
                    except requests.RequestException:
                        continue
                    if status == 429:
                        rate_limited_count += 1
                        if rate_limited_count >= 5:
                            return discovered
                        continue
                    if status == 200:
                        month_match = candidate.strftime("%Y-%m-%d")
                        break
                if month_match:
                    discovered.add(month_match)

    return discovered


def _extract_fred_employment_dates_from_calendar_html(html: str) -> set[str]:
    text = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "\n", text)
    lines = [line.strip() for line in unescape(text).splitlines() if line.strip()]

    dates: set[str] = set()
    for idx, line in enumerate(lines):
        if line != "Employment Situation":
            continue
        for back_idx in range(idx - 1, max(-1, idx - 9), -1):
            candidate = lines[back_idx]
            if not _FRED_RELEASE_DATE_LINE_RE.fullmatch(candidate):
                continue
            try:
                parsed = datetime.strptime(candidate, "%A %B %d, %Y")
            except ValueError:
                break
            dates.add(parsed.strftime("%Y-%m-%d"))
            break
    return dates


def _fetch_nfp_forward_dates_from_fred() -> set[str]:
    current_year = datetime.now(tz=UTC).year
    years = (current_year, current_year + 1)
    found: set[str] = set()
    with requests.Session() as session:
        session.headers.update({"User-Agent": "ResearchTool/1.0 (NFP date fetcher)"})
        for year in years:
            url = NFP_FRED_CALENDAR_URL_TEMPLATE.format(rid=NFP_FRED_RELEASE_ID, year=year)
            try:
                html = _fetch_url_text(session, url)
            except requests.RequestException:
                continue
            found.update(_extract_fred_employment_dates_from_calendar_html(html))
    return found


def _fetch_nfp_dates() -> set[str]:
    archive_dates = _fetch_nfp_archive_dates_from_bls(history_years=NFP_HISTORY_YEARS)
    forward_dates = _fetch_nfp_forward_dates_from_fred()
    if not archive_dates:
        archive_dates = _approximate_nfp_dates(history_years=NFP_HISTORY_YEARS)
    return archive_dates | forward_dates


def _is_nfp_cache_complete(dates: set[str]) -> bool:
    return len(dates) >= NFP_MIN_EXPECTED_DATES


def get_fomc_dates(
    *,
    cache_path: str | Path | None = None,
    max_age_days: int = FOMC_CACHE_MAX_AGE_DAYS,
) -> set[str]:
    """Return known FOMC decision dates as ISO strings (YYYY-MM-DD).

    The function first checks a local cache. If cache is missing/stale it refreshes
    from federalreserve.gov and writes an updated cache file.
    """

    resolved_cache = Path(cache_path) if cache_path is not None else _default_cache_path(FOMC_CACHE_FILENAME)
    if _is_cache_fresh(resolved_cache, max_age_days=max_age_days):
        cached_dates = _load_cached_dates(resolved_cache)
        if cached_dates:
            return cached_dates

    try:
        fetched_dates = _fetch_fomc_dates_from_fed()
    except requests.RequestException:
        return _load_cached_dates(resolved_cache)

    if fetched_dates:
        _write_cache(resolved_cache, fetched_dates, source="federalreserve.gov")
        return fetched_dates

    return _load_cached_dates(resolved_cache)


def get_nfp_dates(
    *,
    cache_path: str | Path | None = None,
    max_age_days: int = NFP_CACHE_MAX_AGE_DAYS,
) -> set[str]:
    """Return known NFP release dates as ISO strings (YYYY-MM-DD)."""

    resolved_cache = Path(cache_path) if cache_path is not None else _default_cache_path(NFP_CACHE_FILENAME)
    if _is_cache_fresh(resolved_cache, max_age_days=max_age_days):
        cached_dates = _load_cached_dates(resolved_cache)
        if cached_dates and _is_nfp_cache_complete(cached_dates):
            return cached_dates

    try:
        fetched_dates = _fetch_nfp_dates()
    except requests.RequestException:
        cached_dates = _load_cached_dates(resolved_cache)
        if cached_dates:
            return cached_dates
        return _approximate_nfp_dates(history_years=NFP_HISTORY_YEARS)

    if fetched_dates:
        _write_cache(resolved_cache, fetched_dates, source="bls.gov + fred.stlouisfed.org")
        return fetched_dates

    cached_dates = _load_cached_dates(resolved_cache)
    if cached_dates:
        return cached_dates
    return _approximate_nfp_dates(history_years=NFP_HISTORY_YEARS)
