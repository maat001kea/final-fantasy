"""Dukascopy intraday loader (no account login) via dukascopy-node CLI."""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .data_validation import validate_intraday_data


TIMEFRAME_TO_DUKASCOPY = {
    "5m": "m5",
    "15m": "m15",
}
CANONICAL_TIMEFRAME_MAP = {
    "m5": "5m",
    "m15": "15m",
    "m30": "30m",
    "h1": "1hr",
    "1h": "1hr",
}

# TD365-style index labels -> Dukascopy symbols (dukascopy-node expects lowercase values).
DUKASCOPY_TD365_INDEX_SYMBOL_MAP: dict[str, str] = {
    "DAX": "deuidxeur",
    "EURO STOXX 50": "eusidxeur",
    "CAC 40": "fraidxeur",
    "AEX": "nldidxeur",
    "IBEX 35": "espidxeur",
    "SMI": "cheidxchf",
    "FTSE": "gbridxgbp",
    "DOW": "usa30idxusd",
    "S&P 500": "usa500idxusd",
    "NASDAQ 100": "usatechidxusd",
    "RUSSELL 2000": "ussc2000idxusd",
    "ITALY 40": "itaidxeur",
    "AUSTRALIA 200": "ausidxaud",
    "HONG KONG 40": "hkgidxhkd",
    "JAPAN 225": "jpnidxjpy",
    "CHINA A50": "chiidxusd",
    "INDIA 50": "indidxusd",
    "US DOLLAR INDEX": "dollaridxusd",
    "VOLATILITY INDEX": "volidxusd",
}

# Full Dukascopy list exposed in app (indices + existing commodities).
DUKASCOPY_SYMBOL_MAP: dict[str, str] = {
    **DUKASCOPY_TD365_INDEX_SYMBOL_MAP,
    "WTI CRUDE OIL": "lightcmdusd",
    "BRENT CRUDE OIL": "brentcmdusd",
}
PERSISTED_CACHE_DIR = Path(__file__).resolve().parents[1] / "download" / "dukascopy_ohlc_cache"
# Keep enough calendar history to satisfy long trading-session windows (e.g. 1000 sessions).
PERSISTED_CACHE_RETENTION_DAYS = 2500
MAX_FETCH_CHUNK_DAYS = 180
MIN_EXPECTED_SESSION_RATIO = 0.65
TRADING_SESSION_TO_CALENDAR_BUFFER = 2.0
TARGET_SESSION_FULFILL_RATIO = 0.92
SESSION_TIMEZONE = "Europe/Copenhagen"


class DukascopyApiError(RuntimeError):
    """Raised when dukascopy-node download fails or returns invalid data."""


def _range_key_to_days(range_key: str) -> int:
    key = str(range_key).strip().lower()
    if key.endswith("d") and key[:-1].isdigit():
        return max(1, int(key[:-1]))
    if key.endswith("mo") and key[:-2].isdigit():
        return max(1, int(key[:-2]) * 30)
    if key.endswith("y") and key[:-1].isdigit():
        return max(1, int(key[:-1]) * 365)
    raise DukascopyApiError(f"Unsupported range key: {range_key}")


def _safe_file_token(text: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9_\-]+", "_", text.strip())
    token = re.sub(r"_+", "_", token)
    return token.strip("_") or "data"


def _split_fetch_range(*, from_date: date, to_date: date, chunk_days: int = MAX_FETCH_CHUNK_DAYS) -> list[tuple[date, date]]:
    if from_date > to_date:
        return []
    size = max(1, int(chunk_days))
    ranges: list[tuple[date, date]] = []
    cursor = from_date
    while cursor <= to_date:
        chunk_end = min(cursor + timedelta(days=size - 1), to_date)
        ranges.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return ranges


def _dukascopy_cli_path() -> str:
    # Prefer npx to avoid global install requirements.
    if shutil.which("npx"):
        return "npx"
    raise DukascopyApiError("npx was not found. Install Node.js to use Dukascopy provider.")


def _run_dukascopy_cli(
    *,
    symbol: str,
    timeframe: str,
    from_date: date,
    to_date: date,
    output_dir: Path,
    file_stem: str,
) -> Path:
    cli = _dukascopy_cli_path()
    cache_dir = Path.home() / ".dukascopy-cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        cli,
        "--yes",
        "dukascopy-node",
        "-i",
        symbol.lower(),
        "-from",
        from_date.isoformat(),
        "-to",
        to_date.isoformat(),
        "-t",
        timeframe,
        "-f",
        "csv",
        "-dir",
        str(output_dir),
        "-fn",
        file_stem,
        "-ch",
        "-chpath",
        str(cache_dir),
        "-r",
        "2",
        "-rp",
        "500",
        "-re",
        "-s",
    ]

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
        timeout=180,
    )
    if proc.returncode != 0:
        output_tail = "\n".join([line for line in proc.stdout.splitlines() if line.strip()][-8:])
        raise DukascopyApiError(f"dukascopy-node failed for {symbol}/{timeframe}: {output_tail}")

    csv_path = output_dir / f"{file_stem}.csv"
    if csv_path.exists():
        return csv_path

    csv_candidates = sorted(output_dir.glob("*.csv"))
    if csv_candidates:
        return csv_candidates[0]
    raise DukascopyApiError(
        f"dukascopy-node completed but produced no CSV for {symbol}/{timeframe}."
    )


def _canonical_timeframe(timeframe: str) -> str:
    token = str(timeframe).strip().lower()
    return CANONICAL_TIMEFRAME_MAP.get(token, token)


def _empty_frame(*, instrument_label: str, timeframe: str) -> pd.DataFrame:
    tf = _canonical_timeframe(timeframe)
    return pd.DataFrame(
        columns=["timestamp", "open", "high", "low", "close", "volume", "instrument", "timeframe"]
    ).assign(instrument=instrument_label, timeframe=tf)


def _persisted_cache_path(*, symbol: str, timeframe: str) -> Path:
    safe_symbol = _safe_file_token(symbol.lower())
    safe_timeframe = _safe_file_token(timeframe.lower())
    return PERSISTED_CACHE_DIR / f"{safe_symbol}_{safe_timeframe}.csv"


def _read_persisted_cache(*, symbol: str, timeframe: str, instrument_label: str) -> pd.DataFrame:
    cache_path = _persisted_cache_path(symbol=symbol, timeframe=timeframe)
    if not cache_path.exists():
        return _empty_frame(instrument_label=instrument_label, timeframe=timeframe)

    try:
        raw = pd.read_csv(cache_path)
    except Exception:
        return _empty_frame(instrument_label=instrument_label, timeframe=timeframe)

    required = {"timestamp", "open", "high", "low", "close"}
    if not required.issubset(set(raw.columns)):
        return _empty_frame(instrument_label=instrument_label, timeframe=timeframe)

    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(raw["timestamp"], errors="coerce"),
            "open": pd.to_numeric(raw["open"], errors="coerce"),
            "high": pd.to_numeric(raw["high"], errors="coerce"),
            "low": pd.to_numeric(raw["low"], errors="coerce"),
            "close": pd.to_numeric(raw["close"], errors="coerce"),
        }
    ).dropna(subset=["timestamp", "open", "high", "low", "close"])

    if frame.empty:
        return _empty_frame(instrument_label=instrument_label, timeframe=timeframe)

    frame["high"] = frame[["high", "open", "close"]].max(axis=1)
    frame["low"] = frame[["low", "open", "close"]].min(axis=1)
    for col in ("open", "high", "low", "close"):
        frame[col] = np.round(frame[col], 2)
    frame["volume"] = 0
    frame["instrument"] = instrument_label
    frame["timeframe"] = _canonical_timeframe(timeframe)
    return frame[["timestamp", "open", "high", "low", "close", "volume", "instrument", "timeframe"]]


def _write_persisted_cache(*, symbol: str, timeframe: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    PERSISTED_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = _persisted_cache_path(symbol=symbol, timeframe=timeframe)
    to_save = frame.copy()
    to_save["timestamp"] = pd.to_datetime(to_save["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    to_save.to_csv(cache_path, index=False)


def _trim_frame_to_range(frame: pd.DataFrame, *, from_date: date, to_date: date) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    cutoff_start = pd.Timestamp(from_date)
    cutoff_end = pd.Timestamp(to_date) + pd.Timedelta(days=1)
    trimmed = frame[(frame["timestamp"] >= cutoff_start) & (frame["timestamp"] < cutoff_end)].copy()
    return trimmed.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)


def _limit_bars(frame: pd.DataFrame, *, max_bars: int | None) -> pd.DataFrame:
    if frame.empty or max_bars is None:
        return frame.copy()
    limit = int(max_bars)
    if limit <= 0:
        return frame.copy()
    if len(frame) <= limit:
        return frame.copy()
    return frame.sort_values("timestamp").tail(limit).reset_index(drop=True)


def _limit_sessions(frame: pd.DataFrame, *, max_sessions: int | None) -> pd.DataFrame:
    if frame.empty or max_sessions is None:
        return frame.copy()
    limit = int(max_sessions)
    if limit <= 0:
        return frame.copy()

    ordered = frame.sort_values("timestamp").copy()
    session_dates = _session_series(ordered["timestamp"])
    unique_sessions = pd.Index(session_dates.dropna().unique()).sort_values()
    if len(unique_sessions) <= limit:
        return ordered.reset_index(drop=True)

    keep_sessions = set(unique_sessions[-limit:])
    keep_mask = session_dates.isin(keep_sessions)
    return ordered.loc[keep_mask].reset_index(drop=True)


def _session_count(frame: pd.DataFrame) -> int:
    if frame.empty or "timestamp" not in frame.columns:
        return 0
    sessions = _session_series(frame["timestamp"])
    return int(sessions.nunique()) if not sessions.empty else 0


def _session_series(values: pd.Series) -> pd.Series:
    """Normalize timestamps to DK-local trading sessions (Mon-Fri)."""
    ts = pd.to_datetime(values, errors="coerce", utc=True)
    local = ts.dt.tz_convert(SESSION_TIMEZONE).dt.tz_localize(None)
    local = local.where(local.dt.dayofweek < 5)
    return local.dt.normalize()


def _merge_frames(cached: pd.DataFrame, fresh: pd.DataFrame, *, instrument_label: str, timeframe: str) -> pd.DataFrame:
    if cached.empty and fresh.empty:
        return _empty_frame(instrument_label=instrument_label, timeframe=timeframe)
    if cached.empty:
        return fresh.copy()
    if fresh.empty:
        return cached.copy()
    return pd.concat([cached, fresh], ignore_index=True)


def _read_candles_csv(path: Path, *, instrument_label: str, timeframe: str) -> pd.DataFrame:
    try:
        raw = pd.read_csv(path)
    except Exception as exc:  # pragma: no cover - defensive
        raise DukascopyApiError(f"Could not read Dukascopy CSV: {path.name}: {exc}") from exc

    required = {"timestamp", "open", "high", "low", "close"}
    if not required.issubset(set(raw.columns)):
        raise DukascopyApiError(
            f"Dukascopy CSV missing fields {sorted(required)} in {path.name}."
        )

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(raw["timestamp"], unit="ms", utc=True, errors="coerce").dt.tz_localize(None),
            "open": pd.to_numeric(raw["open"], errors="coerce"),
            "high": pd.to_numeric(raw["high"], errors="coerce"),
            "low": pd.to_numeric(raw["low"], errors="coerce"),
            "close": pd.to_numeric(raw["close"], errors="coerce"),
        }
    ).dropna(subset=["timestamp", "open", "high", "low", "close"])

    if df.empty:
        raise DukascopyApiError(f"Dukascopy CSV had no valid OHLC rows: {path.name}")

    # Enforce candle consistency.
    df["high"] = df[["high", "open", "close"]].max(axis=1)
    df["low"] = df[["low", "open", "close"]].min(axis=1)
    for col in ("open", "high", "low", "close"):
        df[col] = np.round(df[col], 2)

    df["volume"] = 0
    df["instrument"] = instrument_label
    df["timeframe"] = timeframe
    return df[["timestamp", "open", "high", "low", "close", "volume", "instrument", "timeframe"]]


def load_dukascopy_cached_intraday_data(
    *,
    symbol: str,
    instrument_label: str,
    range_key: str,
    timeframes: list[str],
    max_bars_per_timeframe: int | None = 1000,
) -> pd.DataFrame:
    """Load only persisted local cache for selected symbol/range/timeframes (no network)."""
    symbol_clean = str(symbol).strip().lower()
    if not symbol_clean:
        raise DukascopyApiError("Symbol is required.")
    if not timeframes:
        raise DukascopyApiError("At least one timeframe is required.")

    selected_timeframes: list[str] = []
    for tf in timeframes:
        if tf in TIMEFRAME_TO_DUKASCOPY and tf not in selected_timeframes:
            selected_timeframes.append(tf)
    if not selected_timeframes:
        raise DukascopyApiError("No supported timeframe selected (supported: 5m, 15m).")

    target_sessions = _range_key_to_days(range_key)
    lookback_calendar_days = max(target_sessions, int(round(target_sessions * TRADING_SESSION_TO_CALENDAR_BUFFER)))
    to_date = datetime.now(UTC).date()
    from_date = to_date - timedelta(days=lookback_calendar_days)

    parts: list[pd.DataFrame] = []
    for tf in selected_timeframes:
        duka_tf = TIMEFRAME_TO_DUKASCOPY[tf]
        cached = _read_persisted_cache(symbol=symbol_clean, timeframe=duka_tf, instrument_label=instrument_label)
        cached = _trim_frame_to_range(cached, from_date=from_date, to_date=to_date)
        cached = _limit_sessions(cached, max_sessions=target_sessions)
        cached = _limit_bars(cached, max_bars=max_bars_per_timeframe)
        if not cached.empty:
            parts.append(cached)

    if not parts:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "instrument", "timeframe"])

    data = pd.concat(parts, ignore_index=True)
    data, _warnings = validate_intraday_data(data)
    return data.sort_values(["instrument", "timeframe", "timestamp"]).reset_index(drop=True)


def load_dukascopy_intraday_data(
    *,
    symbol: str,
    instrument_label: str,
    range_key: str,
    timeframes: list[str],
    max_bars_per_timeframe: int | None = 1000,
) -> pd.DataFrame:
    """Load 5m/15m intraday OHLC from Dukascopy via dukascopy-node."""
    symbol_clean = str(symbol).strip().lower()
    if not symbol_clean:
        raise DukascopyApiError("Symbol is required.")
    if not timeframes:
        raise DukascopyApiError("At least one timeframe is required.")

    selected_timeframes: list[str] = []
    for tf in timeframes:
        if tf in TIMEFRAME_TO_DUKASCOPY and tf not in selected_timeframes:
            selected_timeframes.append(tf)
    if not selected_timeframes:
        raise DukascopyApiError("No supported timeframe selected (supported: 5m, 15m).")

    target_sessions = _range_key_to_days(range_key)
    lookback_calendar_days = max(target_sessions, int(round(target_sessions * TRADING_SESSION_TO_CALENDAR_BUFFER)))
    to_date = datetime.now(UTC).date()
    from_date = to_date - timedelta(days=lookback_calendar_days)
    persisted_from_date = to_date - timedelta(days=max(lookback_calendar_days, PERSISTED_CACHE_RETENTION_DAYS))

    parts: list[pd.DataFrame] = []
    with tempfile.TemporaryDirectory(prefix="sa_duka_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        for tf in selected_timeframes:
            duka_tf = TIMEFRAME_TO_DUKASCOPY[tf]
            cached_all = _read_persisted_cache(symbol=symbol_clean, timeframe=duka_tf, instrument_label=instrument_label)
            cached_all = _trim_frame_to_range(cached_all, from_date=persisted_from_date, to_date=to_date)

            fetch_ranges: list[tuple[date, date]] = []
            if cached_all.empty:
                fetch_ranges.append((from_date, to_date))
            else:
                min_cached_ts = pd.to_datetime(cached_all["timestamp"], errors="coerce").min()
                max_cached_ts = pd.to_datetime(cached_all["timestamp"], errors="coerce").max()

                if pd.notna(min_cached_ts):
                    min_cached_date = min_cached_ts.date()
                    if from_date < min_cached_date:
                        fetch_ranges.append((from_date, min_cached_date))

                if pd.notna(max_cached_ts):
                    refresh_from = max(from_date, max_cached_ts.date() - timedelta(days=2))
                    if refresh_from <= to_date:
                        fetch_ranges.append((refresh_from, to_date))
                else:
                    fetch_ranges.append((from_date, to_date))

            fetched_parts: list[pd.DataFrame] = []
            chunk_errors: list[str] = []
            stem = _safe_file_token(f"{instrument_label}_{symbol_clean}_{duka_tf}_{target_sessions}s")
            for range_from, range_to in fetch_ranges:
                if range_from > range_to:
                    continue
                chunked_ranges = _split_fetch_range(from_date=range_from, to_date=range_to)
                for chunk_from, chunk_to in chunked_ranges:
                    chunk_stem = _safe_file_token(f"{stem}_{chunk_from.isoformat()}_{chunk_to.isoformat()}")
                    try:
                        csv_path = _run_dukascopy_cli(
                            symbol=symbol_clean,
                            timeframe=duka_tf,
                            from_date=chunk_from,
                            to_date=chunk_to,
                            output_dir=tmp_path,
                            file_stem=chunk_stem,
                        )
                        fetched_frame = _read_candles_csv(csv_path, instrument_label=instrument_label, timeframe=tf)
                        if not fetched_frame.empty:
                            fetched_parts.append(fetched_frame)
                    except DukascopyApiError as exc:
                        chunk_errors.append(str(exc))
                        continue

            fresh_all = (
                pd.concat(fetched_parts, ignore_index=True)
                if fetched_parts
                else _empty_frame(instrument_label=instrument_label, timeframe=tf)
            )
            combined_all = _merge_frames(
                cached_all,
                fresh_all,
                instrument_label=instrument_label,
                timeframe=tf,
            )
            combined_all = _trim_frame_to_range(combined_all, from_date=persisted_from_date, to_date=to_date)
            if not combined_all.empty:
                _write_persisted_cache(symbol=symbol_clean, timeframe=duka_tf, frame=combined_all)

            requested = _trim_frame_to_range(combined_all, from_date=from_date, to_date=to_date)
            requested = _limit_sessions(requested, max_sessions=target_sessions)
            requested = _limit_bars(requested, max_bars=max_bars_per_timeframe)

            # Robustness fallback:
            # If chunking yields suspiciously low session coverage, run one full-range request.
            expected_min_sessions = max(8, int(target_sessions * MIN_EXPECTED_SESSION_RATIO))
            requested_sessions = _session_count(requested)
            should_fallback_full_fetch = bool(
                target_sessions >= 120
                and requested_sessions < expected_min_sessions
                and max_bars_per_timeframe is None
            )
            if should_fallback_full_fetch:
                full_stem = _safe_file_token(f"{stem}_full_{from_date.isoformat()}_{to_date.isoformat()}")
                try:
                    full_csv = _run_dukascopy_cli(
                        symbol=symbol_clean,
                        timeframe=duka_tf,
                        from_date=from_date,
                        to_date=to_date,
                        output_dir=tmp_path,
                        file_stem=full_stem,
                    )
                    full_frame = _read_candles_csv(full_csv, instrument_label=instrument_label, timeframe=tf)
                    if not full_frame.empty:
                        merged_full = _merge_frames(
                            cached_all,
                            full_frame,
                            instrument_label=instrument_label,
                            timeframe=tf,
                        )
                        merged_full = _trim_frame_to_range(
                            merged_full,
                            from_date=persisted_from_date,
                            to_date=to_date,
                        )
                        requested_full = _trim_frame_to_range(
                            merged_full,
                            from_date=from_date,
                            to_date=to_date,
                        )
                        requested_full = _limit_sessions(requested_full, max_sessions=target_sessions)
                        requested_full = _limit_bars(requested_full, max_bars=max_bars_per_timeframe)
                        if _session_count(requested_full) >= requested_sessions:
                            combined_all = merged_full
                            requested = requested_full
                            if not combined_all.empty:
                                _write_persisted_cache(symbol=symbol_clean, timeframe=duka_tf, frame=combined_all)
                except DukascopyApiError as exc:
                    chunk_errors.append(str(exc))

            # Deep backfill fallback:
            # If coverage is still materially below target sessions, fetch an expanded
            # older window and then trim back to the newest target session count.
            target_min_sessions = max(expected_min_sessions, int(target_sessions * TARGET_SESSION_FULFILL_RATIO))
            requested_sessions = _session_count(requested)
            should_backfill_older = bool(
                target_sessions >= 120
                and requested_sessions < target_min_sessions
                and max_bars_per_timeframe is None
            )
            if should_backfill_older:
                extra_days = max(120, int(round(target_sessions * TRADING_SESSION_TO_CALENDAR_BUFFER)))
                extended_from = from_date - timedelta(days=extra_days)
                backfill_stem = _safe_file_token(
                    f"{stem}_backfill_{extended_from.isoformat()}_{to_date.isoformat()}"
                )
                try:
                    backfill_csv = _run_dukascopy_cli(
                        symbol=symbol_clean,
                        timeframe=duka_tf,
                        from_date=extended_from,
                        to_date=to_date,
                        output_dir=tmp_path,
                        file_stem=backfill_stem,
                    )
                    backfill_frame = _read_candles_csv(
                        backfill_csv, instrument_label=instrument_label, timeframe=tf
                    )
                    if not backfill_frame.empty:
                        merged_backfill = _merge_frames(
                            combined_all,
                            backfill_frame,
                            instrument_label=instrument_label,
                            timeframe=tf,
                        )
                        merged_backfill = _trim_frame_to_range(
                            merged_backfill,
                            from_date=max(extended_from, persisted_from_date),
                            to_date=to_date,
                        )
                        requested_backfill = _trim_frame_to_range(
                            merged_backfill,
                            from_date=extended_from,
                            to_date=to_date,
                        )
                        requested_backfill = _limit_sessions(requested_backfill, max_sessions=target_sessions)
                        requested_backfill = _limit_bars(
                            requested_backfill, max_bars=max_bars_per_timeframe
                        )
                        if _session_count(requested_backfill) >= requested_sessions:
                            combined_all = merged_backfill
                            requested = requested_backfill
                            if not combined_all.empty:
                                _write_persisted_cache(
                                    symbol=symbol_clean, timeframe=duka_tf, frame=combined_all
                                )
                except DukascopyApiError as exc:
                    chunk_errors.append(str(exc))

            if requested.empty and cached_all.empty and fetch_ranges:
                detail = chunk_errors[-1] if chunk_errors else "no data returned by all Dukascopy fetch attempts"
                raise DukascopyApiError(
                    f"Unable to fetch {symbol_clean}/{duka_tf} for {from_date}..{to_date}: {detail}"
                )
            if not requested.empty:
                parts.append(requested)

    if not parts:
        raise DukascopyApiError("Dukascopy returned no candles for this symbol/range.")

    data = pd.concat(parts, ignore_index=True)
    data, _warnings = validate_intraday_data(data)
    return data.sort_values(["instrument", "timeframe", "timestamp"]).reset_index(drop=True)
