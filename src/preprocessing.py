"""Data preprocessing for daily context and bar-level labels."""

from __future__ import annotations

from datetime import datetime
from datetime import time

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = {"instrument", "timeframe", "timestamp", "open", "high", "low", "close"}
MAX_CONTEXT_BARS = 40
APP_TIMEZONE = "Europe/Copenhagen"
OVERNIGHT_TIMEZONE = APP_TIMEZONE
EU_CASH_SESSION_DK = (time(9, 0), time(16, 30))
US_CASH_SESSION_DK = (time(14, 30), time(22, 0))
DEFAULT_SESSION_WINDOW_LOCAL = EU_CASH_SESSION_DK
DEFAULT_ASIA_RANGE_DK = (time(0, 0), time(8, 0))
# Backward compatibility alias used by existing strategy modules.
DEFAULT_OVERNIGHT_RANGE_DK = DEFAULT_ASIA_RANGE_DK
SESSION_WINDOW_BY_INSTRUMENT_LOCAL: dict[str, tuple[time, time]] = {
    # Europe cash-session proxies (DK local time)
    "DAX": EU_CASH_SESSION_DK,
    "EURO STOXX 50": EU_CASH_SESSION_DK,
    "CAC 40": EU_CASH_SESSION_DK,
    "AEX": EU_CASH_SESSION_DK,
    "IBEX 35": EU_CASH_SESSION_DK,
    "SMI": EU_CASH_SESSION_DK,
    "FTSE": EU_CASH_SESSION_DK,
    "ITALY 40": EU_CASH_SESSION_DK,
    # US cash-session proxies (DK local time)
    "DOW": US_CASH_SESSION_DK,
    "S&P 500": US_CASH_SESSION_DK,
    "NASDAQ 100": US_CASH_SESSION_DK,
    "RUSSELL 2000": US_CASH_SESSION_DK,
    "US DOLLAR INDEX": US_CASH_SESSION_DK,
    "VOLATILITY INDEX": US_CASH_SESSION_DK,
    # Asia cash-session proxies (DK local time)
    "AUSTRALIA 200": (time(23, 0), time(6, 0)),
    "HONG KONG 40": (time(2, 30), time(9, 0)),
    "JAPAN 225": (time(1, 0), time(7, 0)),
    "CHINA A50": (time(2, 30), time(8, 0)),
    "INDIA 50": (time(4, 45), time(11, 0)),
    # Energy futures/CFD proxy windows (DK local time)
    "WTI CRUDE OIL": (time(9, 0), time(21, 30)),
    "BRENT CRUDE OIL": (time(9, 0), time(21, 30)),
}
EU_CASH_INSTRUMENTS = {
    "DAX",
    "EURO STOXX 50",
    "CAC 40",
    "AEX",
    "IBEX 35",
    "SMI",
    "FTSE",
    "ITALY 40",
}
US_CASH_INSTRUMENTS = {
    "DOW",
    "S&P 500",
    "NASDAQ 100",
    "RUSSELL 2000",
    "US DOLLAR INDEX",
    "VOLATILITY INDEX",
}
EU_CASH_INSTRUMENTS_UPPER = {_normalized.upper() for _normalized in EU_CASH_INSTRUMENTS}
US_CASH_INSTRUMENTS_UPPER = {_normalized.upper() for _normalized in US_CASH_INSTRUMENTS}
_SESSION_WINDOW_BY_INSTRUMENT_UPPER = {str(k).strip().upper(): v for k, v in SESSION_WINDOW_BY_INSTRUMENT_LOCAL.items()}


def _validate_required_columns(df: pd.DataFrame) -> None:
    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Dataset is missing required columns: {missing}")


def _bar_status(open_price: pd.Series, close_price: pd.Series) -> pd.Series:
    return pd.Series(
        np.select(
            [close_price > open_price, close_price < open_price],
            ["Positive", "Negative"],
            default="Flat",
        ),
        index=open_price.index,
    )


def _in_session_window(ts: pd.Series, start: time, end: time) -> pd.Series:
    clock = ts.dt.time
    if start <= end:
        return (clock >= start) & (clock <= end)
    return (clock >= start) | (clock <= end)


def _to_timezone(ts: pd.Series, timezone_name: str) -> pd.Series:
    # All live providers are normalized to UTC-naive. Treat naive as UTC and convert to target local wall-clock.
    parsed = pd.to_datetime(ts, errors="coerce", utc=True)
    return parsed.dt.tz_convert(timezone_name).dt.tz_localize(None)


def _normalized_instrument_name(value: object) -> str:
    return str(value).strip().upper()


def _session_window_for_instrument(instrument: object) -> tuple[time, time]:
    name = _normalized_instrument_name(instrument)
    if not name:
        return DEFAULT_SESSION_WINDOW_LOCAL

    mapped = _SESSION_WINDOW_BY_INSTRUMENT_UPPER.get(name)
    if mapped is not None:
        return mapped

    if name in EU_CASH_INSTRUMENTS_UPPER:
        return EU_CASH_SESSION_DK
    if name in US_CASH_INSTRUMENTS_UPPER:
        return US_CASH_SESSION_DK

    eu_keywords = ("DAX", "FTSE", "STOXX", "CAC", "AEX", "IBEX", "SMI", "EURO")
    us_keywords = ("DOW", "NASDAQ", "S&P", "RUSSELL", "US ")
    if any(keyword in name for keyword in us_keywords):
        return US_CASH_SESSION_DK
    if any(keyword in name for keyword in eu_keywords):
        return EU_CASH_SESSION_DK
    return DEFAULT_SESSION_WINDOW_LOCAL


def _is_strict_cash_index_instrument(instrument: object) -> bool:
    """Return True for EU/US indices that must use strict RTH-only aggregation."""
    name = _normalized_instrument_name(instrument)
    if name in EU_CASH_INSTRUMENTS_UPPER or name in US_CASH_INSTRUMENTS_UPPER:
        return True
    strict_keywords = ("DAX", "FTSE", "STOXX", "CAC", "AEX", "IBEX", "SMI", "DOW", "NASDAQ", "S&P", "RUSSELL")
    return any(keyword in name for keyword in strict_keywords)


def _coerce_time(value: time | str | None, fallback: time) -> time:
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        token = value.strip()
        if token:
            for fmt in ("%H:%M", "%H:%M:%S"):
                try:
                    return datetime.strptime(token, fmt).time()
                except ValueError:
                    continue
    return fallback


def _compute_named_range(
    data: pd.DataFrame,
    *,
    range_start_dk: time,
    range_end_dk: time,
    prefix: str,
) -> pd.DataFrame:
    required = {"instrument", "timeframe", "timestamp_dk", "high", "low", "close"}
    base_columns = [
        "instrument",
        "timeframe",
        "trade_date",
        f"{prefix}_high",
        f"{prefix}_low",
        f"{prefix}_bars",
    ]
    if data.empty or not required.issubset(data.columns):
        return pd.DataFrame(columns=base_columns)

    session_mask = _in_session_window(data["timestamp_dk"], range_start_dk, range_end_dk)
    sliced = data.loc[session_mask].copy()
    if sliced.empty:
        return pd.DataFrame(columns=base_columns)

    sliced["trade_date"] = sliced["timestamp_dk"].dt.normalize()
    return (
        sliced.groupby(["instrument", "timeframe", "trade_date"], as_index=False)
        .agg(
            **{
                f"{prefix}_high": ("high", "max"),
                f"{prefix}_low": ("low", "min"),
                f"{prefix}_bars": ("close", "size"),
            }
        )
        .sort_values(["instrument", "timeframe", "trade_date"])
        .reset_index(drop=True)
    )


def _compute_asia_range(
    data: pd.DataFrame,
    *,
    asia_range_start_dk: time,
    asia_range_end_dk: time,
) -> pd.DataFrame:
    """Compute Asia-session High/Low/Bars in DK local time."""
    return _compute_named_range(
        data,
        range_start_dk=asia_range_start_dk,
        range_end_dk=asia_range_end_dk,
        prefix="asia",
    )


def _compute_overnight_range(
    data: pd.DataFrame,
    *,
    overnight_start_dk: time,
    overnight_end_dk: time,
) -> pd.DataFrame:
    """Backward-compatible wrapper around Asia range naming."""
    asia = _compute_asia_range(
        data,
        asia_range_start_dk=overnight_start_dk,
        asia_range_end_dk=overnight_end_dk,
    )
    return asia.rename(
        columns={
            "asia_high": "overnight_high",
            "asia_low": "overnight_low",
            "asia_bars": "overnight_bars",
        }
    )


def _filter_to_session_windows(data: pd.DataFrame) -> pd.DataFrame:
    """Restrict bars to instrument cash-session windows (DK local RTH)."""
    parts: list[pd.DataFrame] = []
    ts_col = "timestamp_dk" if "timestamp_dk" in data.columns else "timestamp"
    for instrument, chunk in data.groupby("instrument", sort=False):
        start, end = _session_window_for_instrument(instrument)
        session_mask = _in_session_window(chunk[ts_col], start, end)
        filtered = chunk.loc[session_mask].copy()
        # Strict EU/US cash-index handling: never fall back to 24h bars.
        if filtered.empty and not _is_strict_cash_index_instrument(instrument):
            filtered = chunk.copy()
        if not filtered.empty:
            parts.append(filtered)

    if not parts:
        return data.iloc[0:0].copy()
    return pd.concat(parts, ignore_index=True)


def _append_derived_10m_bars(data: pd.DataFrame) -> pd.DataFrame:
    """Derive 10m OHLC bars from 5m data when 10m is not directly loaded."""
    required = {"instrument", "timeframe", "timestamp", "open", "high", "low", "close"}
    if data.empty or not required.issubset(data.columns):
        return data

    base = data.copy()
    if "volume" not in base.columns:
        base["volume"] = 0

    tf_tokens = base["timeframe"].astype(str).str.strip().str.lower()
    source_5m = base.loc[tf_tokens == "5m"].copy()
    if source_5m.empty:
        return base

    derived_parts: list[pd.DataFrame] = []
    for instrument, chunk in source_5m.groupby("instrument", sort=False):
        chunk = chunk.sort_values("timestamp").dropna(subset=["timestamp", "open", "high", "low", "close"])
        if chunk.empty:
            continue

        resampled = (
            chunk.set_index("timestamp")
            .resample("10min", label="left", closed="left")
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            .dropna(subset=["open", "high", "low", "close"])
            .reset_index()
        )
        if resampled.empty:
            continue

        resampled["instrument"] = instrument
        resampled["timeframe"] = "10m"
        derived_parts.append(
            resampled[["timestamp", "open", "high", "low", "close", "volume", "instrument", "timeframe"]]
        )

    if not derived_parts:
        return base

    derived = pd.concat(derived_parts, ignore_index=True)
    derived["volume"] = pd.to_numeric(derived["volume"], errors="coerce").fillna(0)

    for col in base.columns:
        if col not in derived.columns:
            if col == "timestamp_dk":
                derived[col] = derived["timestamp"]
            else:
                derived[col] = np.nan
    derived = derived[base.columns]

    combined = pd.concat([base, derived], ignore_index=True)
    combined = combined.sort_values(["instrument", "timeframe", "timestamp"])
    combined = combined.drop_duplicates(subset=["instrument", "timeframe", "timestamp"], keep="first")
    return combined.reset_index(drop=True)


def _drop_flat_sessions(data: pd.DataFrame) -> pd.DataFrame:
    """
    Drop synthetic/invalid sessions where all OHLC bars are identical.
    Such sessions can appear in some broker exports during closed markets.
    """
    required = {"instrument", "timeframe", "trade_date", "open", "high", "low", "close"}
    if data.empty or not required.issubset(data.columns):
        return data

    session_stats = (
        data.groupby(["instrument", "timeframe", "trade_date"], as_index=False)
        .agg(
            session_high=("high", "max"),
            session_low=("low", "min"),
            bar_count=("close", "count"),
            open_n=("open", "nunique"),
            high_n=("high", "nunique"),
            low_n=("low", "nunique"),
            close_n=("close", "nunique"),
        )
        .assign(session_range=lambda d: d["session_high"] - d["session_low"])
    )

    flat_sessions = session_stats[
        (session_stats["session_range"].abs() <= 1e-9)
        | (
            (session_stats["bar_count"] > 1)
            & (session_stats["open_n"] <= 1)
            & (session_stats["high_n"] <= 1)
            & (session_stats["low_n"] <= 1)
            & (session_stats["close_n"] <= 1)
        )
    ][["instrument", "timeframe", "trade_date"]]

    if flat_sessions.empty:
        return data

    tagged = data.merge(
        flat_sessions.assign(_drop_flat_session=True),
        on=["instrument", "timeframe", "trade_date"],
        how="left",
    )
    cleaned = tagged[tagged["_drop_flat_session"] != True].drop(columns=["_drop_flat_session"])
    return cleaned.reset_index(drop=True)


def prepare_intraday_data(
    df: pd.DataFrame,
    *,
    timezone_name: str = APP_TIMEZONE,
    asia_range_start_dk: time | str = time(0, 0),
    asia_range_end_dk: time | str = time(8, 0),
    overnight_start_dk: time | str | None = None,
    overnight_end_dk: time | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return enriched intraday rows and daily summary rows."""
    _validate_required_columns(df)

    data = df.copy()
    _ = timezone_name  # Preserved signature; RTH aggregation is always DK-local.
    raw_timestamp = data["timestamp"]
    # Normalize to DK local wall-clock first (required for RTH definitions).
    data["timestamp"] = _to_timezone(raw_timestamp, APP_TIMEZONE)
    data["timestamp_dk"] = _to_timezone(raw_timestamp, APP_TIMEZONE)
    data["instrument"] = data["instrument"].astype(str)
    data["timeframe"] = data["timeframe"].astype(str)
    # Drop weekend bars (Sat/Sun). Dukascopy/CFD feeds can include synthetic flat weekend candles.
    if "timestamp_dk" in data.columns:
        data = data[data["timestamp_dk"].dt.dayofweek < 5].copy()
    data = _append_derived_10m_bars(data)
    data = data.sort_values(["instrument", "timeframe", "timestamp"]).reset_index(drop=True)
    asia_start = _coerce_time(asia_range_start_dk, DEFAULT_ASIA_RANGE_DK[0])
    asia_end = _coerce_time(asia_range_end_dk, DEFAULT_ASIA_RANGE_DK[1])
    # If legacy overnight arguments are supplied, treat them as Asia-range overrides.
    if overnight_start_dk is not None:
        asia_start = _coerce_time(overnight_start_dk, asia_start)
    if overnight_end_dk is not None:
        asia_end = _coerce_time(overnight_end_dk, asia_end)

    asia_daily = _compute_asia_range(
        data,
        asia_range_start_dk=asia_start,
        asia_range_end_dk=asia_end,
    )
    data = _filter_to_session_windows(data)
    data = data.sort_values(["instrument", "timeframe", "timestamp"]).reset_index(drop=True)

    data["trade_date"] = data["timestamp"].dt.normalize()
    data = _drop_flat_sessions(data)
    data["bar_index"] = data.groupby(["instrument", "timeframe", "trade_date"]).cumcount() + 1
    data["bar_status"] = _bar_status(open_price=data["open"], close_price=data["close"])

    daily = (
        data.groupby(["instrument", "timeframe", "trade_date"], as_index=False)
        .agg(
            day_open=("open", "first"),
            day_high=("high", "max"),
            day_low=("low", "min"),
            day_close=("close", "last"),
            day_last_bar_high=("high", "last"),
            day_last_bar_low=("low", "last"),
            bars=("close", "size"),
        )
        .sort_values(["instrument", "timeframe", "trade_date"])
    )

    grouped_daily = daily.groupby(["instrument", "timeframe"], sort=False)
    daily["prev_day_high"] = grouped_daily["day_high"].shift(1)
    daily["prev_day_low"] = grouped_daily["day_low"].shift(1)
    daily["prev_day_close"] = grouped_daily["day_close"].shift(1)
    daily["prev_day_last_bar_high"] = grouped_daily["day_last_bar_high"].shift(1)
    daily["prev_day_last_bar_low"] = grouped_daily["day_last_bar_low"].shift(1)
    daily["day_name"] = daily["trade_date"].dt.day_name()
    daily["gap_pct"] = (daily["day_open"] / daily["prev_day_close"] - 1.0) * 100.0
    daily["gap_size_pct"] = daily["gap_pct"].abs()

    daily["gap_direction"] = np.select(
        [daily["day_open"] > daily["prev_day_close"], daily["day_open"] < daily["prev_day_close"]],
        ["Gap Up", "Gap Down"],
        default="Flat",
    )
    daily["gap_relation"] = np.select(
        [daily["day_open"] < daily["prev_day_low"], daily["day_open"] > daily["prev_day_high"]],
        ["Below Previous Day Low", "Above Previous Day High"],
        default="Inside Previous Day Range",
    )

    no_prev_day_mask = daily["prev_day_close"].isna()
    daily.loc[no_prev_day_mask, "gap_direction"] = "No Previous Day"
    daily.loc[no_prev_day_mask, "gap_relation"] = "No Previous Day"
    daily.loc[no_prev_day_mask, "gap_pct"] = np.nan
    daily.loc[no_prev_day_mask, "gap_size_pct"] = np.nan
    daily = daily.merge(asia_daily, on=["instrument", "timeframe", "trade_date"], how="left")
    daily["asia_has_data"] = daily["asia_high"].notna() & daily["asia_low"].notna()
    daily["asia_open_outside"] = (
        daily["asia_has_data"] & ((daily["day_open"] > daily["asia_high"]) | (daily["day_open"] < daily["asia_low"]))
    )

    # Backward-compatible aliases used by existing strategy code.
    daily["overnight_high"] = daily["asia_high"]
    daily["overnight_low"] = daily["asia_low"]
    daily["overnight_bars"] = daily["asia_bars"]
    daily["overnight_has_data"] = daily["overnight_high"].notna() & daily["overnight_low"].notna()
    daily["overnight_open_outside"] = (
        daily["overnight_has_data"]
        & ((daily["day_open"] > daily["overnight_high"]) | (daily["day_open"] < daily["overnight_low"]))
    )

    bar_context = data[data["bar_index"] <= MAX_CONTEXT_BARS].copy()
    bar_status_pivot = (
        bar_context.pivot_table(
            index=["instrument", "timeframe", "trade_date"],
            columns="bar_index",
            values="bar_status",
            aggfunc="first",
        )
        .reset_index()
    )
    bar_status_pivot.columns = [
        col if isinstance(col, str) else f"bar_{int(col)}_status"
        for col in bar_status_pivot.columns.to_flat_index()
    ]

    bar_ohlc_pivot = (
        bar_context.pivot_table(
            index=["instrument", "timeframe", "trade_date"],
            columns="bar_index",
            values=["open", "high", "low", "close"],
            aggfunc="first",
        )
        .reset_index()
    )
    flat_columns: list[str] = []
    for col in bar_ohlc_pivot.columns.to_flat_index():
        if isinstance(col, str):
            flat_columns.append(col)
            continue
        field_name, bar_number = col
        if bar_number == "":
            flat_columns.append(str(field_name))
        else:
            flat_columns.append(f"bar_{int(bar_number)}_{field_name}")
    bar_ohlc_pivot.columns = flat_columns

    daily = daily.merge(bar_status_pivot, on=["instrument", "timeframe", "trade_date"], how="left")
    daily = daily.merge(bar_ohlc_pivot, on=["instrument", "timeframe", "trade_date"], how="left")
    for bar_num in range(1, MAX_CONTEXT_BARS + 1):
        status_col = f"bar_{bar_num}_status"
        if status_col in daily.columns:
            daily[status_col] = daily[status_col].fillna("Missing")

    merge_columns = [
        "instrument",
        "timeframe",
        "trade_date",
        "day_name",
        "prev_day_high",
        "prev_day_low",
        "prev_day_close",
        "gap_direction",
        "gap_relation",
        "gap_pct",
        "gap_size_pct",
        "asia_high",
        "asia_low",
        "asia_bars",
        "asia_has_data",
        "asia_open_outside",
        "overnight_high",
        "overnight_low",
        "overnight_bars",
        "overnight_has_data",
        "overnight_open_outside",
    ]
    merge_columns.extend(sorted([col for col in daily.columns if col.startswith("bar_")]))
    enriched_intraday = data.merge(daily[merge_columns], on=["instrument", "timeframe", "trade_date"], how="left")

    return enriched_intraday, daily
