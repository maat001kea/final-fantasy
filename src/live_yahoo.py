"""Yahoo Finance intraday loader without account."""

from __future__ import annotations

from typing import Any, Iterator
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
import yfinance as yf

from .data_validation import validate_intraday_data


YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
TIMEFRAME_TO_INTERVAL = {
    "5m": "5m",
    "15m": "15m",
}
INTRADAY_MAX_DAYS = 60


class YahooApiError(RuntimeError):
    """Raised when Yahoo Finance returns an error payload."""


def _range_key_to_days(range_key: str) -> int | None:
    key = range_key.strip().lower()
    if key.endswith("d") and key[:-1].isdigit():
        return int(key[:-1])
    if key.endswith("mo") and key[:-2].isdigit():
        return int(key[:-2]) * 30
    if key.endswith("y") and key[:-1].isdigit():
        return int(key[:-1]) * 365
    return None


def _history_windows(total_days: int, chunk_days: int = 59) -> Iterator[tuple[pd.Timestamp, pd.Timestamp]]:
    end_ts = pd.Timestamp.now(tz="UTC").tz_localize(None)
    start_ts = end_ts - pd.Timedelta(days=max(total_days, 1))
    cursor = start_ts
    while cursor < end_ts:
        window_end = min(cursor + pd.Timedelta(days=chunk_days), end_ts)
        yield cursor, window_end
        cursor = window_end


def _request_chart(symbol: str, interval: str, range_key: str) -> dict[str, Any]:
    encoded_symbol = quote(symbol.strip(), safe="")
    url = YAHOO_CHART_URL.format(symbol=encoded_symbol)
    params = {
        "interval": interval,
        "range": range_key,
        "includePrePost": "false",
        "events": "div,splits",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "application/json",
    }
    response = requests.get(url, params=params, headers=headers, timeout=20)
    if response.status_code >= 400:
        raise YahooApiError(f"Yahoo request failed ({response.status_code}).")
    payload = response.json()
    chart = payload.get("chart", {})
    error = chart.get("error")
    if error:
        message = error.get("description") or error.get("code") or "Unknown Yahoo error"
        raise YahooApiError(f"Yahoo error: {message}")
    results = chart.get("result") or []
    if not isinstance(results, list) or not results:
        raise YahooApiError("Yahoo returned empty chart result.")
    result = results[0]
    if not isinstance(result, dict):
        raise YahooApiError("Yahoo result payload invalid.")
    return result


def _request_chart_period(symbol: str, interval: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> dict[str, Any]:
    encoded_symbol = quote(symbol.strip(), safe="")
    url = YAHOO_CHART_URL.format(symbol=encoded_symbol)
    params = {
        "interval": interval,
        "period1": int(start_ts.timestamp()),
        "period2": int(end_ts.timestamp()),
        "includePrePost": "false",
        "events": "div,splits",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "application/json",
    }
    response = requests.get(url, params=params, headers=headers, timeout=20)
    if response.status_code >= 400:
        raise YahooApiError(f"Yahoo request failed ({response.status_code}).")
    payload = response.json()
    chart = payload.get("chart", {})
    error = chart.get("error")
    if error:
        message = error.get("description") or error.get("code") or "Unknown Yahoo error"
        raise YahooApiError(f"Yahoo error: {message}")
    results = chart.get("result") or []
    if not isinstance(results, list) or not results:
        raise YahooApiError("Yahoo returned empty chart result.")
    result = results[0]
    if not isinstance(result, dict):
        raise YahooApiError("Yahoo result payload invalid.")
    return result


def _normalize_yfinance_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    normalized = df.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        normalized.columns = [str(col[0]) for col in normalized.columns.to_flat_index()]
    else:
        normalized.columns = [str(col) for col in normalized.columns]
    return normalized


def _frame_from_yfinance_raw(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        raise YahooApiError("yfinance returned empty dataset.")

    df = _normalize_yfinance_columns(raw)
    required = {"Open", "High", "Low", "Close"}
    if not required.issubset(set(df.columns)):
        raise YahooApiError("yfinance payload is missing OHLC fields.")

    output = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(df.index, utc=True, errors="coerce").tz_localize(None),
            "open": pd.to_numeric(df["Open"], errors="coerce"),
            "high": pd.to_numeric(df["High"], errors="coerce"),
            "low": pd.to_numeric(df["Low"], errors="coerce"),
            "close": pd.to_numeric(df["Close"], errors="coerce"),
            "volume": pd.to_numeric(df.get("Volume", 0), errors="coerce").fillna(0).astype(int),
        }
    )
    output = output.dropna(subset=["timestamp", "open", "high", "low", "close"])
    if output.empty:
        raise YahooApiError("yfinance data had no valid OHLC rows.")
    return output.reset_index(drop=True)


def _download_with_yfinance(symbol: str, interval: str, range_key: str) -> pd.DataFrame:
    total_days = _range_key_to_days(range_key)
    effective_range = range_key
    # Yahoo intraday data is hard-capped to roughly the latest 60d.
    # Use a direct capped request instead of trying historical chunks.
    if total_days is not None and total_days > INTRADAY_MAX_DAYS:
        effective_range = f"{INTRADAY_MAX_DAYS}d"

    raw = yf.download(
        symbol,
        period=effective_range,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    return _frame_from_yfinance_raw(raw)


def _result_to_frame(result: dict[str, Any], timeframe: str, instrument_label: str) -> pd.DataFrame:
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators", {})
    quotes = indicators.get("quote") or []
    quote = quotes[0] if quotes and isinstance(quotes[0], dict) else {}
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    lengths = [len(timestamps), len(opens), len(highs), len(lows), len(closes)]
    if not lengths or min(lengths) == 0:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume", "instrument", "timeframe"]
        )
    n = min(lengths)
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(timestamps[:n], unit="s", utc=True).tz_localize(None),
            "open": pd.to_numeric(opens[:n], errors="coerce"),
            "high": pd.to_numeric(highs[:n], errors="coerce"),
            "low": pd.to_numeric(lows[:n], errors="coerce"),
            "close": pd.to_numeric(closes[:n], errors="coerce"),
        }
    )
    if len(volumes) >= n:
        frame["volume"] = pd.to_numeric(volumes[:n], errors="coerce").fillna(0).astype(int)
    else:
        frame["volume"] = 0

    frame = frame.dropna(subset=["timestamp", "open", "high", "low", "close"])
    if frame.empty:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume", "instrument", "timeframe"]
        )

    frame["high"] = frame[["high", "open", "close"]].max(axis=1)
    frame["low"] = frame[["low", "open", "close"]].min(axis=1)
    frame["instrument"] = instrument_label
    frame["timeframe"] = timeframe
    for col in ("open", "high", "low", "close"):
        frame[col] = np.round(frame[col], 2)
    return frame.sort_values("timestamp").reset_index(drop=True)


def _download_with_chart_api(
    *,
    symbol: str,
    interval: str,
    range_key: str,
    timeframe: str,
    instrument_label: str,
) -> pd.DataFrame:
    total_days = _range_key_to_days(range_key)
    effective_range = range_key
    if total_days is not None and total_days > INTRADAY_MAX_DAYS:
        effective_range = f"{INTRADAY_MAX_DAYS}d"

    result = _request_chart(symbol=symbol, interval=interval, range_key=effective_range)
    return _result_to_frame(result=result, timeframe=timeframe, instrument_label=instrument_label)


def load_yahoo_intraday_data(
    *,
    symbol: str,
    instrument_label: str,
    range_key: str,
    timeframes: list[str],
) -> pd.DataFrame:
    """Load combined Yahoo intraday candles in standard app schema."""
    if not symbol.strip():
        raise YahooApiError("Symbol is required.")
    if not timeframes:
        raise YahooApiError("At least one timeframe is required.")

    parts: list[pd.DataFrame] = []
    for timeframe in timeframes:
        interval = TIMEFRAME_TO_INTERVAL.get(timeframe)
        if interval is None:
            continue
        try:
            base_frame = _download_with_yfinance(symbol=symbol, interval=interval, range_key=range_key)
            frame = base_frame.copy()
            frame["instrument"] = instrument_label
            frame["timeframe"] = timeframe
            for col in ("open", "high", "low", "close"):
                frame[col] = np.round(frame[col], 2)
            frame = frame[
                ["timestamp", "open", "high", "low", "close", "volume", "instrument", "timeframe"]
            ]
        except Exception:
            frame = _download_with_chart_api(
                symbol=symbol,
                interval=interval,
                range_key=range_key,
                timeframe=timeframe,
                instrument_label=instrument_label,
            )
        if not frame.empty:
            parts.append(frame)

    if not parts:
        raise YahooApiError("Yahoo returned no candles for this symbol/range.")
    data = pd.concat(parts, ignore_index=True)
    data, _warnings = validate_intraday_data(data)
    return data.sort_values(["instrument", "timeframe", "timestamp"]).reset_index(drop=True)
