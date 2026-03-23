"""Data validation helpers for intraday OHLC DataFrames."""

from __future__ import annotations

from typing import Any

import pandas as pd


_TIMEFRAME_MINUTES: dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
    "1hr": 60,
    "1h": 60,
}


def validate_intraday_data(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    """Validate and clean an intraday OHLC DataFrame.

    Checks performed:
    - Duplicate timestamp removal (per instrument/timeframe)
    - Timezone normalisation (ensure UTC-naive timestamps)
    - Missing bar detection (gaps larger than 2× expected interval)

    Parameters
    ----------
    df:
        DataFrame with at minimum a ``timestamp`` column.
        Optionally ``instrument``, ``timeframe`` columns.

    Returns
    -------
    cleaned : pd.DataFrame
        Cleaned copy of the input.
    warnings : list[str]
        Human-readable warning messages describing any issues found.
    """
    if df.empty:
        return df.copy(), []

    warnings: list[str] = []
    result = df.copy()

    # ── 1. Timezone normalisation ──────────────────────────────────────────
    if "timestamp" in result.columns:
        result["timestamp"] = pd.to_datetime(result["timestamp"], errors="coerce")
        tz_aware = result["timestamp"].dt.tz is not None
        if tz_aware:
            result["timestamp"] = result["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
            warnings.append("Timestamps were tz-aware; converted to UTC-naive.")

    # ── 2. Duplicate removal ───────────────────────────────────────────────
    group_cols = [c for c in ("instrument", "timeframe", "timestamp") if c in result.columns]
    if group_cols:
        before = len(result)
        result = result.drop_duplicates(subset=group_cols, keep="first")
        removed = before - len(result)
        if removed > 0:
            warnings.append(f"Removed {removed} duplicate rows (same instrument/timeframe/timestamp).")

    # ── 3. Missing bar detection ───────────────────────────────────────────
    if "timestamp" in result.columns and "timeframe" in result.columns:
        for tf, grp in result.groupby("timeframe", sort=False):
            expected_mins = _TIMEFRAME_MINUTES.get(str(tf))
            if expected_mins is None:
                continue
            sorted_ts = grp["timestamp"].dropna().sort_values()
            if len(sorted_ts) < 2:
                continue
            diffs = sorted_ts.diff().dropna()
            expected_delta = pd.Timedelta(minutes=expected_mins)
            gap_mask = diffs > expected_delta * 2
            gap_count = int(gap_mask.sum())
            if gap_count > 0:
                warnings.append(
                    f"Timeframe {tf}: detected {gap_count} potential missing-bar gap(s) "
                    f"(intervals > {expected_mins * 2} min)."
                )

    return result.reset_index(drop=True), warnings
