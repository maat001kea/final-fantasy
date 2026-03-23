"""Strategy simulation and performance helpers."""

from __future__ import annotations

from datetime import datetime, time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import numpy as np
import pandas as pd

from .economic_calendar import get_fomc_dates, get_nfp_dates
from .trading.fee_model import FeeModel


EXECUTION_MODEL_SIMPLIFIED = "simplified"
EXECUTION_MODEL_TOM_LIVE = "tom_live"
EXECUTION_MODEL_AGGRESSIVE = "aggressive"
# Backward-compatible alias constant name kept for existing imports/usages.
EXECUTION_MODEL_TOM_AGGRESSIVE = EXECUTION_MODEL_AGGRESSIVE
EXECUTION_MODEL_TOM_AGGRESSIVE_ALIAS = "tom_aggressive"
EXECUTION_MODEL_DEFAULT = EXECUTION_MODEL_SIMPLIFIED
DEFAULT_TRIGGER_EXPIRY_BARS = 8


STRATEGY_CONFIG: dict[str, dict[str, Any]] = {
    "School Run": {
        "column_prefix": "school_run",
        "mode": "dual_breakout",
        "long_entry_source": "bar_2_high",
        "long_stop_source": "bar_2_low",
        "short_entry_source": "bar_2_low",
        "short_stop_source": "bar_2_high",
        "entry_offset_pts": 1.0,
        "start_bar": 3,
        "max_trigger_bars": DEFAULT_TRIGGER_EXPIRY_BARS,
        "requires": (),
        "overnight_filter": "outside_or_break",
    },
    "Advanced SR": {
        "column_prefix": "advanced_sr",
        "mode": "dual_breakout",
        "long_entry_source": "bar_3_high",
        "long_stop_source": "bar_2_low",
        "short_entry_source": "bar_3_low",
        "short_stop_source": "bar_2_high",
        "entry_offset_pts": 0.0,
        "start_bar": 4,
        "max_trigger_bars": DEFAULT_TRIGGER_EXPIRY_BARS,
        "requires": (),
        "overnight_filter": "outside_or_break",
    },
    "Anti SR": {
        "column_prefix": "anti_sr",
        "mode": "dual_breakout",
        "long_entry_source": "bar_2_high",
        "long_stop_source": "bar_2_low",
        "short_entry_source": "bar_2_low",
        "short_stop_source": "bar_2_high",
        "entry_offset_pts": 0.0,
        "start_bar": 3,
        "requires": (),
        "overnight_filter": "inside_at_trigger",
    },
    "1BN": {
        "column_prefix": "one_bar_negative",
        "direction": "short",
        "entry_source": "bar_1_low",
        "stop_source": "bar_1_high",
        "start_bar": 2,
        "requires": ("bar_1_status", "Negative"),
    },
    "1BP": {
        "column_prefix": "one_bar_positive",
        "direction": "long",
        "entry_source": "bar_1_high",
        "stop_source": "bar_1_low",
        "start_bar": 2,
        "requires": ("bar_1_status", "Positive"),
    },
    "Rule of 4": {
        "column_prefix": "rule_of_4",
        "direction": "long",
        "entry_source": "bar_4_high",
        "stop_source": "bar_4_low",
        "start_bar": 5,
        "requires": (),
    },
    "New Rule NFP": {
        "column_prefix": "new_rule_nfp",
        "mode": "dual_breakout",
        "long_entry_source": "bar_4_high",
        "long_stop_source": "bar_2_low",
        "short_entry_source": "bar_4_low",
        "short_stop_source": "bar_2_high",
        "start_bar": 5,
        "requires": (),
        "news_filter": "NFP",
    },
    "FOMC Bar 8": {
        "column_prefix": "fomc_bar_8",
        "mode": "dual_breakout",
        "long_entry_source": "bar_8_high",
        "long_stop_source": "bar_8_low",
        "short_entry_source": "bar_8_low",
        "short_stop_source": "bar_8_high",
        "start_bar": 9,
        "requires": (),
        "news_filter": "FOMC",
    },
    "UK100 Bar 13": {
        "column_prefix": "uk100_bar_13",
        "direction": "short",
        "entry_source": "bar_13_low",
        "stop_source": "bar_13_high",
        "start_bar": 14,
        "requires": (),
    },
    "Lunchbreak Reclaim": {
        "column_prefix": "lunchbreak_reclaim",
        "mode": "lunchbreak_reclaim",
        # Keep a preferred timeframe hint, but do not hard-lock the strategy.
        # The simulator now auto-detects minutes from the selected timeframe.
        "timeframe_minutes": 15,
        # Observation window (DK/CET): define high/low range from 12:00 to 13:00.
        "observation_start": "12:00",
        "observation_end": "13:00",
        # Trading window (DK/CET): entries from 13:00, force flat by 14:25.
        "trading_start": "13:00",
        "trading_end": "14:25",
        # Momentum confirmation: candle close must break outside the range by this offset.
        "entry_offset_pts": 1.0,
        # Hougaard midpoint model + scaling.
        "risk_percent": 0.5,  # percent of equity
        "default_equity": 10_000.0,
        "break_even_trigger_r": 1.0,
        "pyramid_trigger_r": 1.0,
        "pyramid_same_size": True,
        "start_bar": 1,  # compatibility field
        "requires": (),
    },
    "Mon Last5 Reversal": {
        "column_prefix": "mon_last5_reversal",
        "mode": "sa_mon_last5_reversal",
        "timeframes": ("5m",),
        # Uses SA condition: Monday low <= Friday last 5m bar low.
        "start_bar": 1,
        # Entry rule: Monday 5m close must be above Friday close after EU open (DK time).
        "eu_open_time": "09:00",
        "friday_close_break_offset_pts": 0.0,
        # Build targets from historical SA event stats (same instrument/timeframe).
        "min_history_events": 20,
        "stop_buffer_pct_above_avg_up": 1.00,
        "requires": (),
    },
    "Breakout Addendum": {
        "column_prefix": "breakout_addendum",
        "direction": "short",
        "entry_source": "bar_3_low",
        "stop_source": "bar_3_high",
        "start_bar": 4,
        "requires": (),
    },
}


def _normalize_execution_model(model: str | None) -> str:
    normalized = str(model or EXECUTION_MODEL_DEFAULT).strip().lower()
    if normalized == EXECUTION_MODEL_TOM_LIVE:
        return EXECUTION_MODEL_TOM_LIVE
    if normalized in {EXECUTION_MODEL_AGGRESSIVE, EXECUTION_MODEL_TOM_AGGRESSIVE_ALIAS,
                      "aggressiv",        # dansk UI-label
                      "agressiv",         # common typo
                      "tom aggressive",
                      }:
        return EXECUTION_MODEL_TOM_AGGRESSIVE
    return EXECUTION_MODEL_SIMPLIFIED


@dataclass(frozen=True)
class StrategyResult:
    triggered: bool
    entry_price: float | None
    stop_price: float | None
    exit_price: float | None
    risk_pts: float | None
    result_pts: float | None
    result_r: float | None
    max_favorable_pts: float | None
    max_adverse_pts: float | None
    stop_hit: bool
    entry_bar_index: int | None = None
    executed_legs: int = 0
    direction: str | None = None


@dataclass(frozen=True)
class TomLiveManagementConfig:
    break_even_trigger_r: float
    trail_activation_r: float
    trail_giveback_r: float
    add_on_trigger_r: float
    max_add_ons: int
    max_stop_pts: float | None = None
    add_on_step_pts: float | None = None


def _tom_live_management_config(strategy_name: str) -> TomLiveManagementConfig:
    default = TomLiveManagementConfig(
        break_even_trigger_r=0.50,
        trail_activation_r=0.80,
        trail_giveback_r=0.35,
        add_on_trigger_r=1.00,
        max_add_ons=1,
        max_stop_pts=None,
    )
    mapping: dict[str, TomLiveManagementConfig] = {
        "1BN": TomLiveManagementConfig(
            break_even_trigger_r=0.40,
            trail_activation_r=0.70,
            trail_giveback_r=0.25,
            add_on_trigger_r=0.90,
            max_add_ons=1,
            max_stop_pts=20.0,
        ),
        "1BP": TomLiveManagementConfig(
            break_even_trigger_r=0.45,
            trail_activation_r=0.75,
            trail_giveback_r=0.30,
            add_on_trigger_r=1.00,
            max_add_ons=1,
            max_stop_pts=None,
        ),
        "Rule of 4": TomLiveManagementConfig(
            break_even_trigger_r=0.70,
            trail_activation_r=1.00,
            trail_giveback_r=0.45,
            add_on_trigger_r=1.30,
            max_add_ons=1,
            max_stop_pts=None,
        ),
        "New Rule NFP": TomLiveManagementConfig(
            break_even_trigger_r=0.70,
            trail_activation_r=1.00,
            trail_giveback_r=0.45,
            add_on_trigger_r=1.30,
            max_add_ons=1,
            max_stop_pts=None,
        ),
        "FOMC Bar 8": TomLiveManagementConfig(
            break_even_trigger_r=0.70,
            trail_activation_r=1.00,
            trail_giveback_r=0.45,
            add_on_trigger_r=1.30,
            max_add_ons=1,
            max_stop_pts=None,
        ),
        "Lunchbreak Reclaim": TomLiveManagementConfig(
            break_even_trigger_r=1.50,
            trail_activation_r=1.50,
            trail_giveback_r=0.50,
            add_on_trigger_r=99.0,
            max_add_ons=0,
            max_stop_pts=None,
        ),
    }
    return mapping.get(strategy_name, default)


def _tom_aggressive_management_config(strategy_name: str) -> TomLiveManagementConfig:
    default = TomLiveManagementConfig(
        break_even_trigger_r=0.25,
        trail_activation_r=0.45,
        trail_giveback_r=0.20,
        add_on_trigger_r=0.55,
        max_add_ons=2,
        max_stop_pts=None,
    )
    mapping: dict[str, TomLiveManagementConfig] = {
        "1BN": TomLiveManagementConfig(
            break_even_trigger_r=0.20,
            trail_activation_r=0.35,
            trail_giveback_r=0.18,
            add_on_trigger_r=0.45,
            max_add_ons=20,
            max_stop_pts=20.0,
            add_on_step_pts=1.0,
        ),
        "1BP": TomLiveManagementConfig(
            break_even_trigger_r=0.22,
            trail_activation_r=0.40,
            trail_giveback_r=0.18,
            add_on_trigger_r=0.50,
            max_add_ons=2,
            max_stop_pts=None,
        ),
        "Rule of 4": TomLiveManagementConfig(
            break_even_trigger_r=0.45,
            trail_activation_r=0.70,
            trail_giveback_r=0.30,
            add_on_trigger_r=0.90,
            max_add_ons=2,
            max_stop_pts=None,
        ),
        "New Rule NFP": TomLiveManagementConfig(
            break_even_trigger_r=0.45,
            trail_activation_r=0.70,
            trail_giveback_r=0.30,
            add_on_trigger_r=0.90,
            max_add_ons=2,
            max_stop_pts=None,
        ),
        "FOMC Bar 8": TomLiveManagementConfig(
            break_even_trigger_r=0.45,
            trail_activation_r=0.70,
            trail_giveback_r=0.30,
            add_on_trigger_r=0.90,
            max_add_ons=2,
            max_stop_pts=None,
        ),
        "Lunchbreak Reclaim": TomLiveManagementConfig(
            break_even_trigger_r=1.50,
            trail_activation_r=1.50,
            trail_giveback_r=0.50,
            add_on_trigger_r=99.0,
            max_add_ons=0,
            max_stop_pts=None,
        ),
    }
    return mapping.get(strategy_name, default)


def _management_config_for_model(execution_model: str, strategy_name: str) -> TomLiveManagementConfig:
    normalized_model = _normalize_execution_model(execution_model)
    if normalized_model == EXECUTION_MODEL_TOM_AGGRESSIVE:
        return _tom_aggressive_management_config(strategy_name=strategy_name)
    return _tom_live_management_config(strategy_name=strategy_name)


def _apply_stop_cap(
    *,
    entry_level: float,
    stop_level: float,
    direction: str,
    max_stop_pts: float | None,
) -> float:
    if max_stop_pts is None or max_stop_pts <= 0:
        return float(stop_level)

    current_distance = abs(float(entry_level) - float(stop_level))
    if current_distance <= float(max_stop_pts):
        return float(stop_level)

    if direction == "long":
        return float(entry_level - float(max_stop_pts))
    return float(entry_level + float(max_stop_pts))


def _empty_strategy_values(prefix: str, row: dict[str, Any]) -> None:
    row[f"{prefix}_triggered"] = False
    row[f"{prefix}_direction"] = np.nan
    row[f"{prefix}_entry"] = np.nan
    row[f"{prefix}_stop"] = np.nan
    row[f"{prefix}_exit"] = np.nan
    row[f"{prefix}_risk_pts"] = np.nan
    row[f"{prefix}_result_pts_gross"] = np.nan
    row[f"{prefix}_result_pts_net"] = np.nan
    row[f"{prefix}_trade_fee"] = np.nan
    row[f"{prefix}_result_pts"] = np.nan
    row[f"{prefix}_result_r"] = np.nan
    row[f"{prefix}_mfe_pts"] = np.nan
    row[f"{prefix}_mae_pts"] = np.nan
    row[f"{prefix}_stop_hit"] = False
    row[f"{prefix}_win"] = False


def _meta_value(day_meta: Any, field_name: str) -> Any:
    if isinstance(day_meta, pd.Series):
        return day_meta.get(field_name)
    return getattr(day_meta, field_name, None)


def _matches_requirement(actual: Any, expected: Any) -> bool:
    if isinstance(expected, (tuple, list, set, frozenset)):
        return actual in expected
    return actual == expected


@lru_cache(maxsize=1)
def _cached_fomc_dates() -> frozenset[str]:
    return frozenset(get_fomc_dates())


@lru_cache(maxsize=1)
def _cached_nfp_dates() -> frozenset[str]:
    return frozenset(get_nfp_dates())


def _coerce_trade_date(trade_date: Any) -> pd.Timestamp | None:
    if trade_date is None or pd.isna(trade_date):
        return None
    try:
        parsed = pd.Timestamp(trade_date)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _passes_news_filter(day_meta: Any, config: dict[str, Any]) -> bool:
    raw_news_filter = str(config.get("news_filter", "")).strip().upper()
    if not raw_news_filter:
        return True

    trade_date = _coerce_trade_date(_meta_value(day_meta, "trade_date"))
    if trade_date is None:
        return False

    trade_date_str = trade_date.strftime("%Y-%m-%d")

    if raw_news_filter == "NFP":
        return trade_date_str in _cached_nfp_dates()

    if raw_news_filter == "FOMC":
        return trade_date_str in _cached_fomc_dates()

    return True


def _can_run_strategy(day_meta: pd.Series, config: dict[str, Any]) -> bool:
    instruments = config.get("instruments", ())
    if instruments and _meta_value(day_meta, "instrument") not in set(instruments):
        return False

    timeframes = config.get("timeframes", ())
    if timeframes and _meta_value(day_meta, "timeframe") not in set(timeframes):
        return False

    requirement = config.get("requires")
    if requirement:
        column_name, expected = requirement
        if not _matches_requirement(_meta_value(day_meta, column_name), expected):
            return False

    for req in config.get("requires_all", ()):
        if len(req) != 2:
            continue
        column_name, expected = req
        if not _matches_requirement(_meta_value(day_meta, column_name), expected):
            return False

    if not _passes_news_filter(day_meta, config):
        return False

    return True


def _can_run_strategy_tuple(day_meta: Any, config: dict[str, Any]) -> bool:
    instruments = config.get("instruments", ())
    if instruments and _meta_value(day_meta, "instrument") not in set(instruments):
        return False

    timeframes = config.get("timeframes", ())
    if timeframes and _meta_value(day_meta, "timeframe") not in set(timeframes):
        return False

    requirement = config.get("requires")
    if requirement:
        column_name, expected = requirement
        if not _matches_requirement(_meta_value(day_meta, column_name), expected):
            return False

    for req in config.get("requires_all", ()):
        if len(req) != 2:
            continue
        column_name, expected = req
        if not _matches_requirement(_meta_value(day_meta, column_name), expected):
            return False

    if not _passes_news_filter(day_meta, config):
        return False

    return True


def _overnight_bounds(day_meta: Any) -> tuple[float | None, float | None]:
    low = _meta_value(day_meta, "overnight_low")
    high = _meta_value(day_meta, "overnight_high")
    if pd.isna(low) or pd.isna(high):
        return None, None
    low_f = float(low)
    high_f = float(high)
    if high_f < low_f:
        return None, None
    return low_f, high_f


def _passes_overnight_outside_or_break(
    *,
    day_meta: Any,
    entry_level: float,
    direction: str,
) -> bool:
    overnight_low, overnight_high = _overnight_bounds(day_meta)
    if overnight_low is None or overnight_high is None:
        return False

    day_open_value = _meta_value(day_meta, "day_open")
    open_outside = False
    if pd.notna(day_open_value):
        day_open = float(day_open_value)
        open_outside = day_open < overnight_low or day_open > overnight_high

    if direction == "long":
        break_outside = float(entry_level) > overnight_high
    else:
        break_outside = float(entry_level) < overnight_low
    return bool(open_outside or break_outside)


def _passes_overnight_outside_or_break_dual(
    *,
    day_meta: Any,
    long_entry_level: float,
    short_entry_level: float,
) -> bool:
    return bool(
        _passes_overnight_outside_or_break(
            day_meta=day_meta,
            entry_level=float(long_entry_level),
            direction="long",
        )
        or _passes_overnight_outside_or_break(
            day_meta=day_meta,
            entry_level=float(short_entry_level),
            direction="short",
        )
    )


def _first_dual_trigger(
    *,
    bar_index: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    start_bar: int,
    long_entry_level: float,
    short_entry_level: float,
    max_trigger_bars: int | None = None,
    allow_long: bool = True,
    allow_short: bool = True,
) -> tuple[str | None, float | None]:
    start_idx = int(np.searchsorted(bar_index, start_bar, side="left"))
    if start_idx >= bar_index.size:
        return None, None

    search_highs = highs[start_idx:]
    search_lows = lows[start_idx:]
    search_bars = bar_index[start_idx:]
    if max_trigger_bars is not None and max_trigger_bars > 0:
        end_bar = int(start_bar) + int(max_trigger_bars) - 1
        within = search_bars <= end_bar
        search_highs = search_highs[within]
        search_lows = search_lows[within]
        search_bars = search_bars[within]
        if search_bars.size == 0:
            return None, None

    long_pos = (
        np.flatnonzero(search_highs >= float(long_entry_level))
        if allow_long
        else np.array([], dtype=int)
    )
    short_pos = (
        np.flatnonzero(search_lows <= float(short_entry_level))
        if allow_short
        else np.array([], dtype=int)
    )
    if long_pos.size == 0 and short_pos.size == 0:
        return None, None

    long_idx = int(long_pos[0]) if long_pos.size > 0 else None
    short_idx = int(short_pos[0]) if short_pos.size > 0 else None

    if long_idx is not None and short_idx is not None:
        if long_idx < short_idx:
            return "long", float(long_entry_level)
        if short_idx < long_idx:
            return "short", float(short_entry_level)
        # Ambiguous same-bar double-touch in OHLC data: skip to avoid sequencing bias.
        return None, None

    if long_idx is not None:
        return "long", float(long_entry_level)
    return "short", float(short_entry_level)


def _reference_bar_index_position(bar_index: np.ndarray, reference_bar: int) -> int | None:
    matches = np.flatnonzero(bar_index == int(reference_bar))
    if matches.size == 0:
        return None
    return int(matches[0])


def _compute_atr_until_reference_bar(
    *,
    bar_index: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    reference_bar: int,
    atr_period: int,
) -> float | None:
    ref_pos = _reference_bar_index_position(bar_index, reference_bar)
    if ref_pos is None:
        return None

    highs_slice = highs[: ref_pos + 1].astype(float)
    lows_slice = lows[: ref_pos + 1].astype(float)
    closes_slice = closes[: ref_pos + 1].astype(float)
    if highs_slice.size == 0:
        return None

    tr = highs_slice - lows_slice
    if highs_slice.size > 1:
        prev_close = np.roll(closes_slice, 1)
        prev_close[0] = closes_slice[0]
        tr = np.maximum.reduce(
            [
                tr,
                np.abs(highs_slice - prev_close),
                np.abs(lows_slice - prev_close),
            ]
        )
    if tr.size == 0:
        return None

    period = max(1, int(atr_period))
    tail = tr[-period:]
    atr = float(np.nanmean(tail)) if tail.size > 0 else np.nan
    if np.isnan(atr) or atr <= 0:
        return None
    return atr


def _passes_reference_bar_quality_filter(
    *,
    bar_index: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    reference_bar: int,
    atr_period: int,
    doji_atr_multiplier: float,
    anomaly_atr_multiplier: float,
) -> bool:
    ref_pos = _reference_bar_index_position(bar_index, reference_bar)
    if ref_pos is None:
        return False

    bar_range = float(highs[ref_pos]) - float(lows[ref_pos])
    if np.isnan(bar_range) or bar_range <= 0:
        return False

    atr = _compute_atr_until_reference_bar(
        bar_index=bar_index,
        highs=highs,
        lows=lows,
        closes=closes,
        reference_bar=reference_bar,
        atr_period=atr_period,
    )
    if atr is None:
        return False

    doji_limit = float(doji_atr_multiplier) * atr
    anomaly_limit = float(anomaly_atr_multiplier) * atr
    return bool(bar_range >= doji_limit and bar_range <= anomaly_limit)


def _compute_reference_baseline(
    *,
    bar_index: np.ndarray,
    closes: np.ndarray,
    volumes: np.ndarray,
    reference_bar: int,
    indicator: str,
    sma_period: int,
    allow_partial_sma: bool,
) -> float | None:
    ref_pos = _reference_bar_index_position(bar_index, reference_bar)
    if ref_pos is None:
        return None

    closes_slice = closes[: ref_pos + 1].astype(float)
    if closes_slice.size == 0:
        return None

    indicator_norm = str(indicator or "vwap").strip().lower()
    if indicator_norm in {"sma", "sma20", "ma", "moving_average"}:
        period = max(1, int(sma_period))
        if closes_slice.size < period and not allow_partial_sma:
            return None
        window = closes_slice[-period:] if closes_slice.size >= period else closes_slice
        baseline = float(np.nanmean(window)) if window.size > 0 else np.nan
        return None if np.isnan(baseline) else baseline

    vol_slice = volumes[: ref_pos + 1].astype(float)
    valid = np.isfinite(closes_slice) & np.isfinite(vol_slice) & (vol_slice > 0)
    if valid.any():
        return float(np.average(closes_slice[valid], weights=vol_slice[valid]))

    # Fallback when feed has no reliable volume.
    baseline = float(np.nanmean(closes_slice))
    return None if np.isnan(baseline) else baseline


def _baseline_direction_gates(
    *,
    day_meta: Any,
    bar_index: np.ndarray,
    closes: np.ndarray,
    volumes: np.ndarray,
    reference_bar: int,
    baseline_indicator: str,
    baseline_sma_period: int,
    baseline_allow_partial: bool,
    baseline_price_source: str,
) -> tuple[bool, bool]:
    baseline = _compute_reference_baseline(
        bar_index=bar_index,
        closes=closes,
        volumes=volumes,
        reference_bar=reference_bar,
        indicator=baseline_indicator,
        sma_period=baseline_sma_period,
        allow_partial_sma=baseline_allow_partial,
    )
    if baseline is None:
        return False, False

    reference_price = _meta_value(day_meta, baseline_price_source)
    reference_price_float = pd.to_numeric(reference_price, errors="coerce")
    if pd.isna(reference_price_float):
        ref_pos = _reference_bar_index_position(bar_index, reference_bar)
        if ref_pos is None:
            return False, False
        reference_price_float = float(closes[ref_pos])

    ref_price = float(reference_price_float)
    allow_long = ref_price > float(baseline)
    allow_short = ref_price < float(baseline)
    return bool(allow_long), bool(allow_short)


def _parse_local_time(value: object, fallback: time) -> time:
    if isinstance(value, time):
        return value
    token = str(value or "").strip()
    if not token:
        return fallback
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(token, fmt).time()
        except ValueError:
            continue
    return fallback


def _size_units_from_risk(
    *,
    equity: float,
    risk_percent: float,
    entry_price: float,
    stop_price: float,
) -> float:
    distance = abs(float(entry_price) - float(stop_price))
    if distance <= 0:
        return 0.0
    return (float(equity) * float(risk_percent) / 100.0) / distance


def _timeframe_to_minutes(value: object) -> int | None:
    token = str(value or "").strip().lower()
    if not token:
        return None
    if token.endswith("m") and token[:-1].isdigit():
        parsed = int(token[:-1])
        return parsed if parsed > 0 else None
    if token.endswith("h") and token[:-1].isdigit():
        parsed = int(token[:-1]) * 60
        return parsed if parsed > 0 else None
    if token.isdigit():
        parsed = int(token)
        return parsed if parsed > 0 else None
    return None


def _build_mon_last5_reversal_targets(
    daily_df: pd.DataFrame,
    *,
    min_history_events: int,
) -> dict[tuple[str, str, Any], tuple[float, float]]:
    """
    Build per-day historical targets for Mon Last5 Reversal.

    Targets are computed from prior qualifying events only (no lookahead):
    - setup condition: Monday low <= Friday last 5m bar low
    - avg_move_up_pts = mean(day_high - prev_day_last_bar_high)
    - avg_move_down_from_up_pts = mean(day_high - day_low)
    """
    required = {
        "instrument",
        "timeframe",
        "trade_date",
        "day_high",
        "day_low",
        "prev_day_last_bar_high",
        "prev_day_last_bar_low",
    }
    if daily_df.empty or not required.issubset(set(daily_df.columns)):
        return {}

    min_history = max(1, int(min_history_events))
    ordered = daily_df.sort_values(["instrument", "timeframe", "trade_date"]).copy()
    ordered["trade_date"] = pd.to_datetime(ordered["trade_date"], errors="coerce")

    targets: dict[tuple[str, str, Any], tuple[float, float]] = {}
    for (_, _), chunk in ordered.groupby(["instrument", "timeframe"], sort=False):
        chunk = chunk.sort_values("trade_date").copy()
        if chunk.empty:
            continue

        trade_dates = pd.to_datetime(chunk["trade_date"], errors="coerce")
        weekdays = trade_dates.dt.dayofweek
        day_high = pd.to_numeric(chunk["day_high"], errors="coerce")
        day_low = pd.to_numeric(chunk["day_low"], errors="coerce")
        prev_last_high = pd.to_numeric(chunk["prev_day_last_bar_high"], errors="coerce")
        prev_last_low = pd.to_numeric(chunk["prev_day_last_bar_low"], errors="coerce")

        rule_match = (weekdays == 0) & day_low.notna() & prev_last_low.notna() & (day_low <= prev_last_low)
        move_up = day_high - prev_last_high
        move_down_from_up = day_high - day_low
        valid_event = rule_match & move_up.notna() & move_down_from_up.notna() & (move_up > 0.0) & (move_down_from_up > 0.0)

        hist_count = valid_event.astype(int).shift(1).fillna(0).cumsum()
        hist_avg_up = move_up.where(valid_event).shift(1).expanding().mean()
        hist_avg_down = move_down_from_up.where(valid_event).shift(1).expanding().mean()

        for idx in chunk.index:
            if not bool(valid_event.loc[idx]):
                continue
            if int(hist_count.loc[idx]) < min_history:
                continue
            avg_up = pd.to_numeric(hist_avg_up.loc[idx], errors="coerce")
            avg_down = pd.to_numeric(hist_avg_down.loc[idx], errors="coerce")
            if pd.isna(avg_up) or pd.isna(avg_down):
                continue
            avg_up_f = float(avg_up)
            avg_down_f = float(avg_down)
            if avg_up_f <= 0.0 or avg_down_f <= 0.0:
                continue

            row = chunk.loc[idx]
            key = (str(row["instrument"]), str(row["timeframe"]), row["trade_date"])
            targets[key] = (avg_up_f, avg_down_f)

    return targets


def _simulate_mon_last5_reversal_arrays(
    *,
    bar_index: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    entry_level: float,
    stop_level: float,
    take_profit_level: float,
    start_bar: int,
) -> StrategyResult:
    if bar_index.size == 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False, direction="short")
    if not np.isfinite(entry_level) or not np.isfinite(stop_level) or not np.isfinite(take_profit_level):
        return StrategyResult(False, None, None, None, None, None, None, None, None, False, direction="short")
    if stop_level <= entry_level or take_profit_level >= entry_level:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False, direction="short")

    start_idx = int(np.searchsorted(bar_index, int(start_bar), side="left"))
    if start_idx >= bar_index.size:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False, direction="short")

    trigger_hits = np.flatnonzero(highs[start_idx:] >= float(entry_level))
    if trigger_hits.size == 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False, direction="short")

    entry_idx = start_idx + int(trigger_hits[0])
    risk_pts = float(stop_level) - float(entry_level)
    if risk_pts <= 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False, direction="short")

    exit_price: float | None = None
    stop_hit = False
    max_favorable_pts = 0.0
    max_adverse_pts = 0.0

    for idx in range(entry_idx, bar_index.size):
        bar_high = float(highs[idx])
        bar_low = float(lows[idx])
        favorable_now = max(0.0, float(entry_level) - bar_low)
        adverse_now = max(0.0, bar_high - float(entry_level))
        max_favorable_pts = max(max_favorable_pts, favorable_now)
        max_adverse_pts = max(max_adverse_pts, adverse_now)

        stop_touched = bar_high >= float(stop_level)
        take_profit_touched = bar_low <= float(take_profit_level)

        # Conservative tie-break when both are touched in one OHLC bar.
        if stop_touched and take_profit_touched:
            stop_hit = True
            exit_price = float(stop_level)
            break
        if stop_touched:
            stop_hit = True
            exit_price = float(stop_level)
            break
        if take_profit_touched:
            exit_price = float(take_profit_level)
            break

    if exit_price is None:
        exit_price = float(closes[-1])

    result_pts = float(entry_level) - float(exit_price)
    result_r = float(result_pts / risk_pts) if risk_pts > 0 else np.nan
    return StrategyResult(
        triggered=True,
        entry_price=float(entry_level),
        stop_price=float(stop_level),
        exit_price=float(exit_price),
        risk_pts=float(risk_pts),
        result_pts=float(result_pts),
        result_r=float(result_r) if not np.isnan(result_r) else np.nan,
        max_favorable_pts=float(max_favorable_pts),
        max_adverse_pts=float(max_adverse_pts),
        stop_hit=bool(stop_hit),
        entry_bar_index=int(bar_index[entry_idx]),
        executed_legs=1,
        direction="short",
    )


def _simulate_lunchbreak_reclaim_arrays(
    *,
    bar_index: np.ndarray,
    timestamps: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    observation_start: object,
    observation_end: object,
    trading_start: object,
    trading_end: object,
    entry_offset_pts: float,
    risk_percent: float,
    account_equity: float,
    break_even_trigger_r: float,
    pyramid_trigger_r: float,
    pyramid_same_size: bool,
    timeframe_minutes: int,
) -> StrategyResult:
    if bar_index.size == 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    ts = pd.to_datetime(pd.Series(timestamps), errors="coerce")
    if ts.isna().all():
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    bar_minutes = max(1, int(timeframe_minutes))
    close_ts = ts + pd.Timedelta(minutes=bar_minutes)
    open_time = ts.dt.time
    close_time = close_ts.dt.time

    obs_start_t = _parse_local_time(observation_start, time(12, 0))
    obs_end_t = _parse_local_time(observation_end, time(13, 0))
    trade_start_t = _parse_local_time(trading_start, time(13, 0))
    trade_end_t = _parse_local_time(trading_end, time(14, 25))

    range_mask = (open_time >= obs_start_t) & (close_time <= obs_end_t)
    range_positions = np.flatnonzero(range_mask.to_numpy(dtype=bool))
    if range_positions.size == 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    range_high = float(np.nanmax(highs[range_positions]))
    range_low = float(np.nanmin(lows[range_positions]))
    if not np.isfinite(range_high) or not np.isfinite(range_low) or range_high <= range_low:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    midpoint = float(range_high - ((range_high - range_low) / 2.0))
    long_break = float(range_high + float(entry_offset_pts))
    short_break = float(range_low - float(entry_offset_pts))

    trade_window_mask = (close_time >= trade_start_t) & (close_time <= trade_end_t)
    trade_positions = np.flatnonzero(trade_window_mask.to_numpy(dtype=bool))
    if trade_positions.size == 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    direction: str | None = None
    entry_idx: int | None = None
    entry_price: float | None = None
    for idx in trade_positions:
        close_price = float(closes[idx])
        if close_price > long_break:
            direction = "long"
            entry_idx = int(idx)
            entry_price = close_price
            break
        if close_price < short_break:
            direction = "short"
            entry_idx = int(idx)
            entry_price = close_price
            break

    if direction is None or entry_idx is None or entry_price is None:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    initial_risk = abs(float(entry_price) - float(midpoint))
    if initial_risk <= 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    position_units = _size_units_from_risk(
        equity=float(account_equity),
        risk_percent=float(risk_percent),
        entry_price=float(entry_price),
        stop_price=float(midpoint),
    )
    if not np.isfinite(position_units) or position_units <= 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    eod_candidates = np.flatnonzero((close_time <= trade_end_t).to_numpy(dtype=bool))
    if eod_candidates.size == 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)
    eod_idx = int(eod_candidates[-1])

    stop_level = float(midpoint)
    stop_hit = False
    exit_price: float | None = None
    added = False
    add_entry: float | None = None
    executed_legs = 1
    max_favorable_pts = 0.0
    max_adverse_pts = 0.0

    for idx in range(entry_idx + 1, eod_idx + 1):
        bar_high = float(highs[idx])
        bar_low = float(lows[idx])

        if direction == "long":
            if bar_low <= stop_level:
                stop_hit = True
                exit_price = float(stop_level)
                break
            favorable_now = max(0.0, bar_high - float(entry_price))
            adverse_now = max(0.0, float(entry_price) - bar_low)
            add_trigger_price = float(entry_price) + float(pyramid_trigger_r) * float(initial_risk)
            be_trigger_price = float(entry_price) + float(break_even_trigger_r) * float(initial_risk)
            reached_r_target = bar_high >= max(add_trigger_price, be_trigger_price)
        else:
            if bar_high >= stop_level:
                stop_hit = True
                exit_price = float(stop_level)
                break
            favorable_now = max(0.0, float(entry_price) - bar_low)
            adverse_now = max(0.0, bar_high - float(entry_price))
            add_trigger_price = float(entry_price) - float(pyramid_trigger_r) * float(initial_risk)
            be_trigger_price = float(entry_price) - float(break_even_trigger_r) * float(initial_risk)
            reached_r_target = bar_low <= min(add_trigger_price, be_trigger_price)

        max_favorable_pts = max(max_favorable_pts, favorable_now)
        max_adverse_pts = max(max_adverse_pts, adverse_now)

        if not added and reached_r_target:
            if bool(pyramid_same_size):
                add_entry = float(add_trigger_price)
                executed_legs = 2
                stop_level = float((float(entry_price) + float(add_entry)) / 2.0)
            else:
                stop_level = float(entry_price)
            added = True

    if exit_price is None:
        if entry_idx > eod_idx:
            exit_price = float(entry_price)
        else:
            exit_price = float(closes[eod_idx])

    if direction == "long":
        base_result = float(exit_price) - float(entry_price)
        add_result = (float(exit_price) - float(add_entry)) if add_entry is not None else 0.0
    else:
        base_result = float(entry_price) - float(exit_price)
        add_result = (float(add_entry) - float(exit_price)) if add_entry is not None else 0.0

    result_pts = float(base_result + add_result)
    result_r = float(result_pts / float(initial_risk)) if initial_risk > 0 else np.nan
    return StrategyResult(
        triggered=True,
        entry_price=float(entry_price),
        stop_price=float(midpoint),
        exit_price=float(exit_price),
        risk_pts=float(initial_risk),
        result_pts=float(result_pts),
        result_r=float(result_r) if not np.isnan(result_r) else np.nan,
        max_favorable_pts=float(max_favorable_pts),
        max_adverse_pts=float(max_adverse_pts),
        stop_hit=bool(stop_hit),
        entry_bar_index=int(bar_index[entry_idx]),
        executed_legs=int(executed_legs),
        direction=direction,
    )


def _simulate_breakout(
    day_intraday: pd.DataFrame,
    *,
    entry_level: float,
    stop_level: float,
    direction: str,
    start_bar: int,
    max_trigger_bars: int | None = None,
) -> StrategyResult:
    bars = day_intraday.sort_values("bar_index")
    search_bars = bars[bars["bar_index"] >= start_bar]
    if max_trigger_bars is not None and max_trigger_bars > 0:
        end_bar = int(start_bar) + int(max_trigger_bars) - 1
        search_bars = search_bars[search_bars["bar_index"] <= end_bar]
    if search_bars.empty:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    if direction == "long":
        trigger_hits = search_bars[search_bars["high"] >= entry_level]
    else:
        trigger_hits = search_bars[search_bars["low"] <= entry_level]

    if trigger_hits.empty:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    entry_bar = int(trigger_hits.iloc[0]["bar_index"])
    active_bars = bars[bars["bar_index"] >= entry_bar]
    if active_bars.empty:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    stop_hit = False
    if direction == "long":
        stop_bars = active_bars[active_bars["low"] <= stop_level]
    else:
        stop_bars = active_bars[active_bars["high"] >= stop_level]
    if not stop_bars.empty:
        stop_hit = True

    if stop_hit:
        exit_price = float(stop_level)
    else:
        exit_price = float(active_bars.iloc[-1]["close"])

    risk_pts = abs(float(entry_level) - float(stop_level))
    if risk_pts <= 0:
        risk_pts = np.nan

    if direction == "long":
        result_pts = exit_price - float(entry_level)
        max_favorable_pts = float(active_bars["high"].max() - float(entry_level))
        max_adverse_pts = float(float(entry_level) - active_bars["low"].min())
    else:
        result_pts = float(entry_level) - exit_price
        max_favorable_pts = float(float(entry_level) - active_bars["low"].min())
        max_adverse_pts = float(active_bars["high"].max() - float(entry_level))

    if np.isnan(risk_pts):
        result_r = np.nan
    else:
        result_r = result_pts / risk_pts

    return StrategyResult(
        triggered=True,
        entry_price=float(entry_level),
        stop_price=float(stop_level),
        exit_price=exit_price,
        risk_pts=float(risk_pts) if not np.isnan(risk_pts) else np.nan,
        result_pts=float(result_pts),
        result_r=float(result_r) if not np.isnan(result_r) else np.nan,
        max_favorable_pts=float(max_favorable_pts),
        max_adverse_pts=float(max_adverse_pts),
        stop_hit=stop_hit,
        entry_bar_index=int(entry_bar),
        direction=str(direction),
    )


def _simulate_breakout_arrays(
    *,
    bar_index: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    entry_level: float,
    stop_level: float,
    direction: str,
    start_bar: int,
    max_trigger_bars: int | None = None,
    require_trigger_price_inside_range: tuple[float, float] | None = None,
    trailing_activation_r: float | None = None,
    trailing_giveback_r: float = 0.5,
) -> StrategyResult:
    start_idx = int(np.searchsorted(bar_index, start_bar, side="left"))
    if start_idx >= bar_index.size:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    search_highs = highs[start_idx:]
    search_lows = lows[start_idx:]
    search_bars = bar_index[start_idx:]
    if max_trigger_bars is not None and max_trigger_bars > 0:
        end_bar = int(start_bar) + int(max_trigger_bars) - 1
        within = search_bars <= end_bar
        search_highs = search_highs[within]
        search_lows = search_lows[within]
        search_bars = search_bars[within]
        if search_bars.size == 0:
            return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    if direction == "long":
        trigger_positions = np.flatnonzero(search_highs >= entry_level)
    else:
        trigger_positions = np.flatnonzero(search_lows <= entry_level)
    if trigger_positions.size == 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    entry_idx = start_idx + int(trigger_positions[0])
    if require_trigger_price_inside_range is not None:
        range_low, range_high = require_trigger_price_inside_range
        # "Price at trigger" is the executed entry level for the breakout.
        trigger_price = float(entry_level)
        if trigger_price < float(range_low) or trigger_price > float(range_high):
            return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    active_highs = highs[entry_idx:]
    active_lows = lows[entry_idx:]
    active_closes = closes[entry_idx:]
    if active_closes.size == 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    risk_pts = abs(float(entry_level) - float(stop_level))
    if risk_pts <= 0:
        risk_pts = np.nan

    current_stop = float(stop_level)
    stop_hit = False
    exit_price: float | None = None
    max_favorable_pts = 0.0
    max_adverse_pts = 0.0
    trailing_enabled = (
        trailing_activation_r is not None
        and pd.notna(risk_pts)
        and float(risk_pts) > 0
        and float(trailing_activation_r) > 0
    )

    for idx in range(active_closes.size):
        bar_high = float(active_highs[idx])
        bar_low = float(active_lows[idx])

        if direction == "long":
            if bar_low <= current_stop:
                stop_hit = True
                exit_price = float(current_stop)
                break
            favorable_now = max(0.0, bar_high - float(entry_level))
            adverse_now = max(0.0, float(entry_level) - bar_low)
        else:
            if bar_high >= current_stop:
                stop_hit = True
                exit_price = float(current_stop)
                break
            favorable_now = max(0.0, float(entry_level) - bar_low)
            adverse_now = max(0.0, bar_high - float(entry_level))

        max_favorable_pts = max(max_favorable_pts, favorable_now)
        max_adverse_pts = max(max_adverse_pts, adverse_now)

        if trailing_enabled:
            favorable_r = favorable_now / float(risk_pts)
            if favorable_r >= float(trailing_activation_r):
                locked_favorable = max(0.0, favorable_now - float(trailing_giveback_r) * float(risk_pts))
                if direction == "long":
                    trail_candidate = float(entry_level) + locked_favorable
                    current_stop = max(current_stop, float(trail_candidate))
                else:
                    trail_candidate = float(entry_level) - locked_favorable
                    current_stop = min(current_stop, float(trail_candidate))

    if exit_price is None:
        exit_price = float(active_closes[-1])

    if direction == "long":
        result_pts = exit_price - float(entry_level)
    else:
        result_pts = float(entry_level) - exit_price

    result_r = np.nan if np.isnan(risk_pts) else float(result_pts / risk_pts)
    return StrategyResult(
        triggered=True,
        entry_price=float(entry_level),
        stop_price=float(stop_level),
        exit_price=exit_price,
        risk_pts=float(risk_pts) if not np.isnan(risk_pts) else np.nan,
        result_pts=float(result_pts),
        result_r=float(result_r) if not np.isnan(result_r) else np.nan,
        max_favorable_pts=float(max_favorable_pts),
        max_adverse_pts=float(max_adverse_pts),
        stop_hit=stop_hit,
        entry_bar_index=int(bar_index[entry_idx]),
        executed_legs=1,
        direction=str(direction),
    )


def _simulate_breakout_arrays_tom_live(
    *,
    bar_index: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    entry_level: float,
    stop_level: float,
    direction: str,
    start_bar: int,
    max_trigger_bars: int | None = None,
    strategy_name: str,
    execution_model: str = EXECUTION_MODEL_TOM_LIVE,
    require_trigger_price_inside_range: tuple[float, float] | None = None,
) -> StrategyResult:
    start_idx = int(np.searchsorted(bar_index, start_bar, side="left"))
    if start_idx >= bar_index.size:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    search_highs = highs[start_idx:]
    search_lows = lows[start_idx:]
    search_bars = bar_index[start_idx:]
    if max_trigger_bars is not None and max_trigger_bars > 0:
        end_bar = int(start_bar) + int(max_trigger_bars) - 1
        within = search_bars <= end_bar
        search_highs = search_highs[within]
        search_lows = search_lows[within]
        search_bars = search_bars[within]
        if search_bars.size == 0:
            return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    if direction == "long":
        trigger_positions = np.flatnonzero(search_highs >= entry_level)
    else:
        trigger_positions = np.flatnonzero(search_lows <= entry_level)
    if trigger_positions.size == 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    entry_idx = start_idx + int(trigger_positions[0])
    if entry_idx >= bar_index.size:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)
    if require_trigger_price_inside_range is not None:
        range_low, range_high = require_trigger_price_inside_range
        # "Price at trigger" is the executed entry level for the breakout.
        trigger_price = float(entry_level)
        if trigger_price < float(range_low) or trigger_price > float(range_high):
            return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    mgmt = _management_config_for_model(
        execution_model=execution_model,
        strategy_name=strategy_name,
    )
    adjusted_stop = _apply_stop_cap(
        entry_level=float(entry_level),
        stop_level=float(stop_level),
        direction=direction,
        max_stop_pts=mgmt.max_stop_pts,
    )
    initial_risk = abs(float(entry_level) - float(adjusted_stop))
    if initial_risk <= 0:
        return StrategyResult(False, None, None, None, None, None, None, None, None, False)

    current_stop = float(adjusted_stop)
    break_even_armed = False
    add_entries: list[float] = []
    max_favorable_pts = 0.0
    max_adverse_pts = 0.0
    stop_hit = False
    exit_price: float | None = None

    for idx in range(entry_idx, bar_index.size):
        bar_high = float(highs[idx])
        bar_low = float(lows[idx])
        bar_close = float(closes[idx])

        if direction == "long":
            if bar_low <= current_stop:
                stop_hit = True
                exit_price = float(current_stop)
                max_adverse_pts = max(max_adverse_pts, max(0.0, float(entry_level) - float(current_stop)))
                break
        else:
            if bar_high >= current_stop:
                stop_hit = True
                exit_price = float(current_stop)
                max_adverse_pts = max(max_adverse_pts, max(0.0, float(current_stop) - float(entry_level)))
                break

        if direction == "long":
            favorable_now = max(0.0, bar_high - float(entry_level))
            adverse_now = max(0.0, float(entry_level) - bar_low)
        else:
            favorable_now = max(0.0, float(entry_level) - bar_low)
            adverse_now = max(0.0, bar_high - float(entry_level))

        max_favorable_pts = max(max_favorable_pts, favorable_now)
        max_adverse_pts = max(max_adverse_pts, adverse_now)
        favorable_r = favorable_now / initial_risk

        if not break_even_armed and favorable_r >= mgmt.break_even_trigger_r:
            break_even_armed = True
            if direction == "long":
                current_stop = max(current_stop, float(entry_level))
            else:
                current_stop = min(current_stop, float(entry_level))

        if break_even_armed and len(add_entries) < mgmt.max_add_ons:
            if mgmt.add_on_step_pts is not None and mgmt.add_on_step_pts > 0:
                next_add_level = float(len(add_entries) + 1) * float(mgmt.add_on_step_pts)
            else:
                next_add_level = float(len(add_entries) + 1) * mgmt.add_on_trigger_r * initial_risk
            if favorable_now >= next_add_level:
                add_price = (
                    float(entry_level) + next_add_level
                    if direction == "long"
                    else float(entry_level) - next_add_level
                )
                add_entries.append(add_price)

        if break_even_armed and favorable_r >= mgmt.trail_activation_r:
            locked_favorable = max(0.0, favorable_now - mgmt.trail_giveback_r * initial_risk)
            trail_candidate = (
                float(entry_level) + locked_favorable
                if direction == "long"
                else float(entry_level) - locked_favorable
            )
            if direction == "long":
                current_stop = max(current_stop, float(trail_candidate))
            else:
                current_stop = min(current_stop, float(trail_candidate))

    if exit_price is None:
        exit_price = float(closes[-1])

    if direction == "long":
        base_result_pts = exit_price - float(entry_level)
        add_result_pts = float(sum(exit_price - add_entry for add_entry in add_entries))
    else:
        base_result_pts = float(entry_level) - exit_price
        add_result_pts = float(sum(add_entry - exit_price for add_entry in add_entries))

    result_pts = float(base_result_pts + add_result_pts)
    result_r = float(result_pts / initial_risk) if initial_risk > 0 else np.nan

    return StrategyResult(
        triggered=True,
        entry_price=float(entry_level),
        stop_price=float(adjusted_stop),
        exit_price=float(exit_price),
        risk_pts=float(initial_risk),
        result_pts=result_pts,
        result_r=float(result_r) if not np.isnan(result_r) else np.nan,
        max_favorable_pts=float(max_favorable_pts),
        max_adverse_pts=float(max_adverse_pts),
        stop_hit=stop_hit,
        entry_bar_index=int(bar_index[entry_idx]),
        executed_legs=int(1 + len(add_entries)),
        direction=str(direction),
    )


def compute_strategy_rows(
    intraday_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    execution_model: str = EXECUTION_MODEL_DEFAULT,
) -> pd.DataFrame:
    """Compute strategy outcomes for each instrument/timeframe/day row."""
    if intraday_df.empty or daily_df.empty:
        return pd.DataFrame(columns=["instrument", "timeframe", "trade_date"])

    grouped_indices = intraday_df.groupby(["instrument", "timeframe", "trade_date"], sort=False).indices
    bar_index_values = intraday_df["bar_index"].to_numpy(copy=False)
    timestamp_values = intraday_df["timestamp"].to_numpy(copy=False)
    high_values = intraday_df["high"].to_numpy(copy=False)
    low_values = intraday_df["low"].to_numpy(copy=False)
    close_values = intraday_df["close"].to_numpy(copy=False)
    if "volume" in intraday_df.columns:
        volume_values = pd.to_numeric(intraday_df["volume"], errors="coerce").fillna(0.0).to_numpy(copy=False)
    else:
        volume_values = np.zeros(len(intraday_df), dtype=float)

    rows: list[dict[str, Any]] = []
    normalized_model = _normalize_execution_model(execution_model)
    mon_last5_cfg = STRATEGY_CONFIG.get("Mon Last5 Reversal", {})
    mon_last5_targets = _build_mon_last5_reversal_targets(
        daily_df=daily_df,
        min_history_events=int(mon_last5_cfg.get("min_history_events", 20)),
    )

    for day_meta in daily_df.itertuples(index=False):
        key = (day_meta.instrument, day_meta.timeframe, day_meta.trade_date)
        positions = grouped_indices.get(key)
        if positions is None:
            continue
        position_array = np.asarray(positions)
        if position_array.size == 0:
            continue
        bar_index = bar_index_values[position_array]
        timestamps = timestamp_values[position_array]
        highs = high_values[position_array]
        lows = low_values[position_array]
        closes = close_values[position_array]
        volumes = volume_values[position_array]

        row: dict[str, Any] = {
            "instrument": key[0],
            "timeframe": key[1],
            "trade_date": key[2],
        }

        for strategy_name, config in STRATEGY_CONFIG.items():
            prefix = config["column_prefix"]
            if not _can_run_strategy_tuple(day_meta, config):
                _empty_strategy_values(prefix=prefix, row=row)
                continue

            mode = str(config.get("mode", "single_breakout")).strip().lower()
            overnight_filter_mode = str(config.get("overnight_filter", "")).strip().lower()
            trigger_inside_range: tuple[float, float] | None = None
            max_trigger_bars = config.get("max_trigger_bars")
            if max_trigger_bars is not None:
                try:
                    max_trigger_bars = int(max_trigger_bars)
                except (TypeError, ValueError):
                    max_trigger_bars = None
            strategy_direction = str(config.get("direction", "long")).strip().lower()
            reference_bar = max(1, int(config.get("reference_bar", int(config["start_bar"]) - 1)))
            bar_quality_filter_enabled = bool(config.get("bar_quality_filter_enabled", False))
            baseline_filter_enabled = bool(config.get("baseline_filter_enabled", False))
            baseline_indicator = str(config.get("baseline_indicator", "vwap"))
            baseline_sma_period = int(config.get("baseline_sma_period", 20))
            baseline_allow_partial = bool(config.get("baseline_allow_partial", True))
            baseline_price_source = str(config.get("baseline_price_source", f"bar_{reference_bar}_close"))
            atr_period = int(config.get("atr_period", 14))
            doji_atr_multiplier = float(config.get("doji_atr_multiplier", 0.5))
            anomaly_atr_multiplier = float(config.get("anomaly_atr_multiplier", 2.0))

            trailing_activation_raw = pd.to_numeric(config.get("trailing_activation_r"), errors="coerce")
            trailing_activation_r = float(trailing_activation_raw) if pd.notna(trailing_activation_raw) else None
            trailing_giveback_raw = pd.to_numeric(config.get("trailing_giveback_r"), errors="coerce")
            trailing_giveback_r = float(trailing_giveback_raw) if pd.notna(trailing_giveback_raw) else 0.5

            if bar_quality_filter_enabled:
                if not _passes_reference_bar_quality_filter(
                    bar_index=bar_index,
                    highs=highs,
                    lows=lows,
                    closes=closes,
                    reference_bar=reference_bar,
                    atr_period=atr_period,
                    doji_atr_multiplier=doji_atr_multiplier,
                    anomaly_atr_multiplier=anomaly_atr_multiplier,
                ):
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue

            baseline_allow_long = True
            baseline_allow_short = True
            if baseline_filter_enabled:
                baseline_allow_long, baseline_allow_short = _baseline_direction_gates(
                    day_meta=day_meta,
                    bar_index=bar_index,
                    closes=closes,
                    volumes=volumes,
                    reference_bar=reference_bar,
                    baseline_indicator=baseline_indicator,
                    baseline_sma_period=baseline_sma_period,
                    baseline_allow_partial=baseline_allow_partial,
                    baseline_price_source=baseline_price_source,
                )

            if mode == "lunchbreak_reclaim":
                resolved_tf_minutes = _timeframe_to_minutes(_meta_value(day_meta, "timeframe"))
                if resolved_tf_minutes is None:
                    resolved_tf_minutes = _timeframe_to_minutes(config.get("timeframe_minutes", 15))
                if resolved_tf_minutes is None:
                    resolved_tf_minutes = 15
                result = _simulate_lunchbreak_reclaim_arrays(
                    bar_index=bar_index,
                    timestamps=timestamps,
                    highs=highs,
                    lows=lows,
                    closes=closes,
                    observation_start=config.get("observation_start", "12:00"),
                    observation_end=config.get("observation_end", "13:00"),
                    trading_start=config.get("trading_start", "13:00"),
                    trading_end=config.get("trading_end", "14:25"),
                    entry_offset_pts=float(config.get("entry_offset_pts", 1.0)),
                    risk_percent=float(config.get("risk_percent", 0.5)),
                    account_equity=float(config.get("default_equity", 10_000.0)),
                    break_even_trigger_r=float(config.get("break_even_trigger_r", 1.0)),
                    pyramid_trigger_r=float(config.get("pyramid_trigger_r", 1.0)),
                    pyramid_same_size=bool(config.get("pyramid_same_size", True)),
                    timeframe_minutes=int(resolved_tf_minutes),
                )
                if result.direction in {"long", "short"}:
                    strategy_direction = str(result.direction)
            elif mode == "sa_mon_last5_reversal":
                target = mon_last5_targets.get(key)
                if target is None:
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue
                prev_day_close = pd.to_numeric(_meta_value(day_meta, "prev_day_close"), errors="coerce")
                if pd.isna(prev_day_close):
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue

                ts = pd.to_datetime(pd.Series(timestamps), errors="coerce")
                if ts.isna().all():
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue
                tf_minutes = _timeframe_to_minutes(_meta_value(day_meta, "timeframe"))
                if tf_minutes is None:
                    tf_minutes = 5
                close_ts = ts + pd.Timedelta(minutes=int(tf_minutes))
                eu_open_time = _parse_local_time(config.get("eu_open_time", "09:00"), time(9, 0))
                break_offset_pts = float(config.get("friday_close_break_offset_pts", 0.0))
                trigger_threshold = float(prev_day_close) + float(break_offset_pts)
                trigger_mask = (close_ts.dt.time >= eu_open_time).to_numpy(dtype=bool) & (closes > trigger_threshold)
                trigger_positions = np.flatnonzero(trigger_mask)
                if trigger_positions.size == 0:
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue

                avg_move_up_pts, avg_move_down_from_up_pts = target
                stop_buffer_pct = float(config.get("stop_buffer_pct_above_avg_up", 1.00))
                entry_index = int(trigger_positions[0])
                entry_level = float(closes[entry_index])
                stop_level = float(entry_level) + (float(avg_move_up_pts) * float(stop_buffer_pct))
                take_profit_level = float(entry_level) - float(avg_move_down_from_up_pts)
                strategy_direction = "short"
                result = _simulate_mon_last5_reversal_arrays(
                    bar_index=bar_index,
                    highs=highs,
                    lows=lows,
                    closes=closes,
                    entry_level=float(entry_level),
                    stop_level=float(stop_level),
                    take_profit_level=float(take_profit_level),
                    start_bar=int(bar_index[entry_index]),
                )
            elif mode == "dual_breakout":
                long_entry_level = getattr(day_meta, config["long_entry_source"], np.nan)
                long_stop_level = getattr(day_meta, config["long_stop_source"], np.nan)
                short_entry_level = getattr(day_meta, config["short_entry_source"], np.nan)
                short_stop_level = getattr(day_meta, config["short_stop_source"], np.nan)
                if pd.isna(long_entry_level) or pd.isna(long_stop_level) or pd.isna(short_entry_level) or pd.isna(short_stop_level):
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue

                entry_offset = float(config.get("entry_offset_pts", 0.0) or 0.0)
                long_entry_level = float(long_entry_level) + entry_offset
                short_entry_level = float(short_entry_level) - entry_offset
                long_stop_level = float(long_stop_level)
                short_stop_level = float(short_stop_level)
                allow_long = bool(baseline_allow_long)
                allow_short = bool(baseline_allow_short)

                if overnight_filter_mode:
                    overnight_low, overnight_high = _overnight_bounds(day_meta)
                    if overnight_low is None or overnight_high is None:
                        _empty_strategy_values(prefix=prefix, row=row)
                        continue
                    if overnight_filter_mode == "outside_or_break":
                        if not _passes_overnight_outside_or_break_dual(
                            day_meta=day_meta,
                            long_entry_level=long_entry_level,
                            short_entry_level=short_entry_level,
                        ):
                            _empty_strategy_values(prefix=prefix, row=row)
                            continue
                    elif overnight_filter_mode == "inside_at_trigger":
                        trigger_inside_range = (float(overnight_low), float(overnight_high))
                        allow_long = bool(float(overnight_low) <= float(long_entry_level) <= float(overnight_high))
                        allow_short = bool(float(overnight_low) <= float(short_entry_level) <= float(overnight_high))
                        if not allow_long and not allow_short:
                            _empty_strategy_values(prefix=prefix, row=row)
                            continue

                strategy_direction, _ = _first_dual_trigger(
                    bar_index=bar_index,
                    highs=highs,
                    lows=lows,
                    start_bar=int(config["start_bar"]),
                    long_entry_level=long_entry_level,
                    short_entry_level=short_entry_level,
                    max_trigger_bars=max_trigger_bars,
                    allow_long=allow_long,
                    allow_short=allow_short,
                )
                if strategy_direction not in {"long", "short"}:
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue

                if strategy_direction == "long":
                    entry_level = long_entry_level
                    stop_level = long_stop_level
                else:
                    entry_level = short_entry_level
                    stop_level = short_stop_level
            else:
                entry_level = getattr(day_meta, config["entry_source"], np.nan)
                stop_level = getattr(day_meta, config["stop_source"], np.nan)
                if pd.isna(entry_level) or pd.isna(stop_level):
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue
                if strategy_direction == "long" and not baseline_allow_long:
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue
                if strategy_direction == "short" and not baseline_allow_short:
                    _empty_strategy_values(prefix=prefix, row=row)
                    continue

                if overnight_filter_mode:
                    overnight_low, overnight_high = _overnight_bounds(day_meta)
                    if overnight_low is None or overnight_high is None:
                        _empty_strategy_values(prefix=prefix, row=row)
                        continue
                    if overnight_filter_mode == "outside_or_break":
                        if not _passes_overnight_outside_or_break(
                            day_meta=day_meta,
                            entry_level=float(entry_level),
                            direction=str(strategy_direction),
                        ):
                            _empty_strategy_values(prefix=prefix, row=row)
                            continue
                    elif overnight_filter_mode == "inside_at_trigger":
                        trigger_inside_range = (float(overnight_low), float(overnight_high))

            if mode not in {"lunchbreak_reclaim", "sa_mon_last5_reversal"}:
                if normalized_model in {EXECUTION_MODEL_TOM_LIVE, EXECUTION_MODEL_TOM_AGGRESSIVE}:
                    result = _simulate_breakout_arrays_tom_live(
                        bar_index=bar_index,
                        highs=highs,
                        lows=lows,
                        closes=closes,
                        entry_level=float(entry_level),
                        stop_level=float(stop_level),
                        direction=str(strategy_direction),
                        start_bar=int(config["start_bar"]),
                        max_trigger_bars=max_trigger_bars,
                        strategy_name=strategy_name,
                        execution_model=normalized_model,
                        require_trigger_price_inside_range=trigger_inside_range,
                    )
                else:
                    result = _simulate_breakout_arrays(
                        bar_index=bar_index,
                        highs=highs,
                        lows=lows,
                        closes=closes,
                        entry_level=float(entry_level),
                        stop_level=float(stop_level),
                        direction=str(strategy_direction),
                        start_bar=int(config["start_bar"]),
                        max_trigger_bars=max_trigger_bars,
                        require_trigger_price_inside_range=trigger_inside_range,
                        trailing_activation_r=trailing_activation_r,
                        trailing_giveback_r=trailing_giveback_r,
                    )

            row[f"{prefix}_triggered"] = result.triggered
            resolved_direction = (
                str(result.direction).strip().lower()
                if result.triggered and isinstance(result.direction, str) and str(result.direction).strip()
                else str(strategy_direction).strip().lower()
            )
            row[f"{prefix}_direction"] = resolved_direction if result.triggered else np.nan
            row[f"{prefix}_entry"] = result.entry_price
            row[f"{prefix}_stop"] = result.stop_price
            row[f"{prefix}_exit"] = result.exit_price
            row[f"{prefix}_risk_pts"] = result.risk_pts

            gross_result_pts = pd.to_numeric(result.result_pts, errors="coerce")
            trade_fee = np.nan
            if result.triggered:
                trade_time = None
                entry_bar_idx = result.entry_bar_index
                if entry_bar_idx is not None:
                    matches = np.flatnonzero(bar_index == int(entry_bar_idx))
                    if matches.size > 0 and timestamps.size > int(matches[0]):
                        ts_value = pd.to_datetime(timestamps[int(matches[0])], errors="coerce")
                        if pd.notna(ts_value):
                            trade_time = ts_value.time()
                per_trade_spread_pts = float(FeeModel.get_spread(str(key[0]), trade_time))
                # Fee is charged once per closed trade in metrics layer (no add-to-winners bucket multiplier).
                trade_fee = float(per_trade_spread_pts)

            net_result_pts = gross_result_pts
            if pd.notna(gross_result_pts) and pd.notna(trade_fee):
                net_result_pts = float(gross_result_pts) - float(trade_fee)

            if result.risk_pts is not None and pd.notna(result.risk_pts) and float(result.risk_pts) > 0 and pd.notna(net_result_pts):
                net_result_r = float(net_result_pts) / float(result.risk_pts)
            else:
                net_result_r = result.result_r

            row[f"{prefix}_result_pts_gross"] = float(gross_result_pts) if pd.notna(gross_result_pts) else np.nan
            row[f"{prefix}_trade_fee"] = float(trade_fee) if pd.notna(trade_fee) else np.nan
            row[f"{prefix}_result_pts_net"] = float(net_result_pts) if pd.notna(net_result_pts) else np.nan
            row[f"{prefix}_result_pts"] = float(net_result_pts) if pd.notna(net_result_pts) else np.nan
            row[f"{prefix}_result_r"] = float(net_result_r) if pd.notna(net_result_r) else np.nan
            row[f"{prefix}_mfe_pts"] = result.max_favorable_pts
            row[f"{prefix}_mae_pts"] = result.max_adverse_pts
            row[f"{prefix}_stop_hit"] = result.stop_hit
            row[f"{prefix}_win"] = bool(result.triggered and pd.notna(net_result_pts) and float(net_result_pts) > 0)

        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=["instrument", "timeframe", "trade_date"])

    return pd.DataFrame(rows)


def compute_index_divergence(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute bar2-vs-open divergence.

    Primary signal is vs peer average (same date/timeframe) excluding the instrument itself.
    If only one instrument exists on a given day, fallback to a rolling self-baseline.
    """
    required = {"instrument", "timeframe", "trade_date", "day_open", "bar_2_close"}
    if not required.issubset(daily_df.columns):
        return pd.DataFrame(columns=["instrument", "timeframe", "trade_date", "divergence_points"])

    base = daily_df[["instrument", "timeframe", "trade_date", "day_open", "bar_2_close"]].copy()
    safe_day_open = base["day_open"].replace(0.0, np.nan)
    base["bar2_return_pct"] = (base["bar_2_close"] / safe_day_open - 1.0) * 100.0

    peer_stats = (
        base.groupby(["timeframe", "trade_date"], as_index=False)["bar2_return_pct"]
        .agg(peer_sum="sum", peer_count="count")
    )
    merged = base.merge(peer_stats, on=["timeframe", "trade_date"], how="left")

    merged["peer_mean_ex_self"] = np.where(
        merged["peer_count"] > 1,
        (merged["peer_sum"] - merged["bar2_return_pct"]) / (merged["peer_count"] - 1),
        np.nan,
    )

    merged = merged.sort_values(["instrument", "timeframe", "trade_date"]).reset_index(drop=True)
    merged["self_baseline"] = merged.groupby(["instrument", "timeframe"])["bar2_return_pct"].transform(
        lambda s: s.shift(1).rolling(window=20, min_periods=5).mean()
    )

    merged["divergence_points"] = merged["bar2_return_pct"] - merged["peer_mean_ex_self"]
    fallback_mask = merged["divergence_points"].isna()
    merged.loc[fallback_mask, "divergence_points"] = (
        merged.loc[fallback_mask, "bar2_return_pct"] - merged.loc[fallback_mask, "self_baseline"]
    )
    merged["divergence_points"] = (
        merged["divergence_points"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    )

    merged["divergence_state"] = np.select(
        [merged["divergence_points"] > 0.10, merged["divergence_points"] < -0.10],
        ["Bullish Divergence", "Bearish Divergence"],
        default="Neutral",
    )
    return merged[["instrument", "timeframe", "trade_date", "divergence_points", "divergence_state"]]


def attach_strategies(
    intraday_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    execution_model: str = EXECUTION_MODEL_DEFAULT,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Attach strategy and divergence columns to intraday and daily dataframes."""
    strategy_rows = compute_strategy_rows(
        intraday_df=intraday_df,
        daily_df=daily_df,
        execution_model=execution_model,
    )
    divergence_rows = compute_index_divergence(daily_df=daily_df)

    enriched_daily = daily_df.copy()
    enriched_daily = enriched_daily.merge(
        strategy_rows,
        on=["instrument", "timeframe", "trade_date"],
        how="left",
    )
    enriched_daily = enriched_daily.merge(
        divergence_rows,
        on=["instrument", "timeframe", "trade_date"],
        how="left",
    )
    enriched_daily["divergence_state"] = enriched_daily["divergence_state"].fillna("Neutral")
    enriched_daily["divergence_points"] = enriched_daily["divergence_points"].fillna(0.0)
    enriched_daily["day_result_pts"] = enriched_daily["day_close"] - enriched_daily["day_open"]
    enriched_daily["day_range_pts"] = enriched_daily["day_high"] - enriched_daily["day_low"]

    enriched_intraday = intraday_df.merge(
        enriched_daily[
            [
                "instrument",
                "timeframe",
                "trade_date",
                "divergence_points",
                "divergence_state",
                "day_result_pts",
                "day_range_pts",
            ]
        ],
        on=["instrument", "timeframe", "trade_date"],
        how="left",
    )
    return enriched_intraday, enriched_daily


def strategy_prefix_for_overlay(overlay: str) -> str | None:
    # Runtime-only overlays that are intentionally decoupled from STRATEGY_CONFIG.
    runtime_prefix_map: dict[str, str] = {
        "Custom": "custom",
        "Custom Futures": "custom_futures",
    }
    mapped = runtime_prefix_map.get(str(overlay).strip())
    if mapped:
        return mapped

    config = STRATEGY_CONFIG.get(overlay)
    if config is None:
        return None
    return str(config["column_prefix"])


def normalize_overlay_selection(overlay: str | tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    """Normalize overlay selection while keeping order and removing invalid values."""
    if overlay is None:
        return ("None",)

    raw_values: list[str]
    if isinstance(overlay, str):
        raw_values = [overlay]
    else:
        raw_values = [str(value) for value in overlay]

    cleaned: list[str] = []
    for value in raw_values:
        token = value.strip()
        if not token:
            continue
        if token not in cleaned:
            cleaned.append(token)

    if not cleaned:
        return ("None",)

    if "None" in cleaned and len(cleaned) > 1:
        cleaned = [value for value in cleaned if value != "None"]

    normalized = [value for value in cleaned if value == "None" or strategy_prefix_for_overlay(value)]
    if not normalized:
        return ("None",)
    return tuple(normalized)


def _build_overlay_result_series(
    matched_days: pd.DataFrame, overlays: tuple[str, ...]
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    """Compose triggered/result series across multiple overlays (first-trigger priority)."""
    triggered_mask = pd.Series(False, index=matched_days.index, dtype=bool)
    pts_series = pd.Series(np.nan, index=matched_days.index, dtype=float)
    r_series = pd.Series(np.nan, index=matched_days.index, dtype=float)
    win_series = pd.Series(False, index=matched_days.index, dtype=bool)

    for overlay_name in overlays:
        prefix = strategy_prefix_for_overlay(overlay_name)
        if not prefix:
            continue

        triggered_col = f"{prefix}_triggered"
        if triggered_col not in matched_days.columns:
            continue

        triggered = matched_days[triggered_col].fillna(False).astype(bool)
        choose = triggered & ~triggered_mask
        if choose.any():
            result_pts_col = f"{prefix}_result_pts"
            if result_pts_col in matched_days.columns:
                pts_series.loc[choose] = pd.to_numeric(matched_days[result_pts_col], errors="coerce").loc[choose]

            result_r_col = f"{prefix}_result_r"
            if result_r_col in matched_days.columns:
                r_series.loc[choose] = pd.to_numeric(matched_days[result_r_col], errors="coerce").loc[choose]

            win_col = f"{prefix}_win"
            if win_col in matched_days.columns:
                win_series.loc[choose] = matched_days[win_col].fillna(False).astype(bool).loc[choose]
            elif result_pts_col in matched_days.columns:
                inferred_wins = pd.to_numeric(matched_days[result_pts_col], errors="coerce") > 0
                win_series.loc[choose] = inferred_wins.loc[choose]

        triggered_mask = triggered_mask | triggered

    return triggered_mask, pts_series, r_series, win_series


def build_strategy_performance(
    matched_days: pd.DataFrame, overlay: str | tuple[str, ...] | list[str] | None
) -> dict[str, float]:
    """Compute aggregate metrics for selected days and optional strategy overlay."""
    if matched_days.empty:
        return {
            "sessions": 0.0,
            "triggered": 0.0,
            "win_rate": 0.0,
            "avg_pts": 0.0,
            "avg_r": 0.0,
            "expectancy_pts": 0.0,
        }

    overlays = normalize_overlay_selection(overlay)
    if overlays == ("None",):
        pts = matched_days["day_result_pts"].dropna()
        sessions = float(len(matched_days))
        win_rate = float((pts > 0).mean() * 100.0) if not pts.empty else 0.0
        avg_pts = float(pts.mean()) if not pts.empty else 0.0
        return {
            "sessions": sessions,
            "triggered": sessions,
            "win_rate": win_rate,
            "avg_pts": avg_pts,
            "avg_r": 0.0,
            "expectancy_pts": avg_pts,
        }

    triggered_mask, combined_pts, combined_r, combined_wins = _build_overlay_result_series(matched_days, overlays)
    triggered_days = matched_days[triggered_mask]

    pts = combined_pts[triggered_mask].dropna()
    r_values = combined_r[triggered_mask].dropna()
    wins = combined_wins[triggered_mask]
    if wins.empty and not pts.empty:
        wins = (pts > 0)

    sessions = float(len(matched_days))
    triggered = float(len(triggered_days))
    win_rate = float(wins.mean() * 100.0) if not wins.empty else 0.0
    avg_pts = float(pts.mean()) if not pts.empty else 0.0
    avg_r = float(r_values.mean()) if not r_values.empty else 0.0

    return {
        "sessions": sessions,
        "triggered": triggered,
        "win_rate": win_rate,
        "avg_pts": avg_pts,
        "avg_r": avg_r,
        "expectancy_pts": avg_pts,
    }
