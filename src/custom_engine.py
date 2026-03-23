"""School Run-only runtime engine used by the `Custom` overlay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .custom_types import CustomStrategyConfig, coerce_custom_strategy_config
from .strategies import (
    EXECUTION_MODEL_SIMPLIFIED,
    EXECUTION_MODEL_TOM_AGGRESSIVE,
    EXECUTION_MODEL_TOM_LIVE,
    _first_dual_trigger,
    _simulate_breakout_arrays,
    _simulate_breakout_arrays_tom_live,
)
from .trading.fee_model import FeeModel


CUSTOM_PREFIX = "custom"
CUSTOM_RESULT_COLUMNS = [
    "instrument",
    "timeframe",
    "trade_date",
    f"{CUSTOM_PREFIX}_triggered",
    f"{CUSTOM_PREFIX}_direction",
    f"{CUSTOM_PREFIX}_entry",
    f"{CUSTOM_PREFIX}_stop",
    f"{CUSTOM_PREFIX}_exit",
    f"{CUSTOM_PREFIX}_risk_pts",
    f"{CUSTOM_PREFIX}_result_pts_gross",
    f"{CUSTOM_PREFIX}_trade_fee",
    f"{CUSTOM_PREFIX}_trade_fee_usd",
    f"{CUSTOM_PREFIX}_contracts_fill",
    f"{CUSTOM_PREFIX}_contract_symbol",
    f"{CUSTOM_PREFIX}_point_value",
    f"{CUSTOM_PREFIX}_result_pts_net",
    f"{CUSTOM_PREFIX}_result_pts",
    f"{CUSTOM_PREFIX}_result_r",
    f"{CUSTOM_PREFIX}_mfe_pts",
    f"{CUSTOM_PREFIX}_mae_pts",
    f"{CUSTOM_PREFIX}_stop_hit",
    f"{CUSTOM_PREFIX}_win",
    f"{CUSTOM_PREFIX}_executed_legs",
    f"{CUSTOM_PREFIX}_position_units",
    f"{CUSTOM_PREFIX}_result_capital",
]

TARGET_TIMEFRAME = "15m"
TARGET_MINUTES = 15
APP_SESSION_TIMEZONE = ZoneInfo("Europe/Copenhagen")
US_MARKET_TIMEZONE = ZoneInfo("America/New_York")
SCHOOL_RUN_US_OPEN_AUTO = "US Open (auto)"
_SCHOOL_RUN_US_OPEN_AUTO_TOKENS = {
    str(SCHOOL_RUN_US_OPEN_AUTO).strip().lower(),
    "us_open_auto",
    "us cash open",
    "us_cash_open",
    "market_open_auto",
}
MODEL_LABEL_TO_ID = {
    "Systematisk": EXECUTION_MODEL_SIMPLIFIED,
    "Dynamisk": EXECUTION_MODEL_TOM_LIVE,
    "Aggressiv": EXECUTION_MODEL_TOM_AGGRESSIVE,
}


@dataclass(frozen=True)
class _SchoolRunSetup:
    start_bar: int
    long_entry: float
    long_stop: float
    short_entry: float
    short_stop: float


@dataclass(frozen=True)
class SchoolRunSessionClock:
    trade_date: str
    mode: str
    bar1_start_dk: time
    bar2_start_dk: time
    trigger_start_dk: time


def _parse_local_time(value: object, fallback: time) -> time:
    if isinstance(value, time):
        return value
    token = str(value or "").strip()
    if token:
        for fmt in ("%H:%M", "%H:%M:%S"):
            try:
                return datetime.strptime(token, fmt).time()
            except ValueError:
                continue
    return fallback


def is_school_run_auto_bar1_start(value: object) -> bool:
    token = str(value or "").strip().lower()
    return token in _SCHOOL_RUN_US_OPEN_AUTO_TOKENS


def _coerce_school_run_trade_date(value: Any | None) -> date:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return datetime.now(tz=APP_SESSION_TIMEZONE).date()
    if getattr(parsed, "tzinfo", None) is not None:
        parsed = parsed.tz_convert(APP_SESSION_TIMEZONE).tz_localize(None)
    return parsed.date()


def resolve_school_run_session_clock_dk(
    bar1_start: object,
    *,
    trade_date: Any | None = None,
) -> SchoolRunSessionClock:
    trade_day = _coerce_school_run_trade_date(trade_date)
    if is_school_run_auto_bar1_start(bar1_start):
        us_open = datetime.combine(trade_day, time(9, 30), tzinfo=US_MARKET_TIMEZONE)
        bar1_dt_dk = us_open.astimezone(APP_SESSION_TIMEZONE)
        mode = "us_open_auto"
    else:
        start_clock = _parse_local_time(bar1_start, fallback=time(9, 0))
        bar1_dt_dk = datetime.combine(trade_day, start_clock, tzinfo=APP_SESSION_TIMEZONE)
        mode = "manual_dk"

    bar2_dt_dk = bar1_dt_dk + timedelta(minutes=15)
    trigger_dt_dk = bar1_dt_dk + timedelta(minutes=30)
    return SchoolRunSessionClock(
        trade_date=trade_day.isoformat(),
        mode=mode,
        bar1_start_dk=time(bar1_dt_dk.hour, bar1_dt_dk.minute, bar1_dt_dk.second),
        bar2_start_dk=time(bar2_dt_dk.hour, bar2_dt_dk.minute, bar2_dt_dk.second),
        trigger_start_dk=time(trigger_dt_dk.hour, trigger_dt_dk.minute, trigger_dt_dk.second),
    )


def _normalize_trade_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def _timestamp_dk(frame: pd.DataFrame) -> pd.Series:
    source = frame["timestamp_dk"] if "timestamp_dk" in frame.columns else frame["timestamp"]
    ts = pd.to_datetime(source, errors="coerce")
    if ts.dt.tz is not None:
        return ts.dt.tz_convert("Europe/Copenhagen").dt.tz_localize(None)
    return ts


def _execution_model_id(label: str) -> str:
    token = str(label or "").strip()
    return MODEL_LABEL_TO_ID.get(token, EXECUTION_MODEL_TOM_AGGRESSIVE)


def _prepare_daily_base(daily_df: pd.DataFrame, cfg: CustomStrategyConfig) -> pd.DataFrame:
    base = daily_df[daily_df["instrument"].astype(str) == str(cfg.instrument)].copy()
    if base.empty:
        return base
    base["trade_date"] = _normalize_trade_date(base["trade_date"])
    base = base.dropna(subset=["trade_date"]).copy()
    if base.empty:
        return base
    base = base.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last")
    return base.tail(max(1, int(cfg.lookback_sessions))).copy()


def _build_15m_bars(intraday_df: pd.DataFrame, cfg: CustomStrategyConfig, trade_dates: set[pd.Timestamp]) -> pd.DataFrame:
    bars = intraday_df.copy()
    bars["trade_date"] = _normalize_trade_date(bars["trade_date"])
    bars = bars[
        (bars["instrument"].astype(str) == str(cfg.instrument))
        & (bars["trade_date"].isin(trade_dates))
    ].copy()
    if bars.empty:
        return bars

    bars["timestamp_dk"] = _timestamp_dk(bars)
    bars = bars.dropna(subset=["timestamp_dk", "open", "high", "low", "close"]).copy()
    if bars.empty:
        return bars

    if (bars["timeframe"].astype(str) == TARGET_TIMEFRAME).any():
        out = bars[bars["timeframe"].astype(str) == TARGET_TIMEFRAME].copy()
        out = out.sort_values(["trade_date", "timestamp_dk"]).reset_index(drop=True)
        out["bar_index"] = out.groupby("trade_date").cumcount().add(1)
        return out

    source_tf = None
    for token in ("5m", "10m"):
        if (bars["timeframe"].astype(str) == token).any():
            source_tf = token
            break
    if source_tf is None:
        return bars.iloc[0:0].copy()

    source = bars[bars["timeframe"].astype(str) == source_tf].copy()
    rows: list[pd.DataFrame] = []
    for trade_date, chunk in source.groupby("trade_date", sort=False):
        chunk = chunk.sort_values("timestamp_dk")
        if chunk.empty:
            continue
        resampled = (
            chunk.set_index("timestamp_dk")
            .resample(f"{TARGET_MINUTES}min", label="left", closed="left")
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
            )
            .dropna(subset=["open", "high", "low", "close"])
            .reset_index()
        )
        if resampled.empty:
            continue
        resampled["instrument"] = str(cfg.instrument)
        resampled["timeframe"] = TARGET_TIMEFRAME
        resampled["trade_date"] = pd.Timestamp(trade_date).normalize()
        resampled["bar_index"] = np.arange(1, len(resampled) + 1, dtype=int)
        rows.append(resampled)
    if not rows:
        return source.iloc[0:0].copy()
    return pd.concat(rows, ignore_index=True)


def _school_run_setup(day_bars: pd.DataFrame, cfg: CustomStrategyConfig) -> _SchoolRunSetup | None:
    ts_dk = day_bars["timestamp_dk"] if "timestamp_dk" in day_bars.columns else _timestamp_dk(day_bars)
    if ts_dk.isna().all():
        return None
    trade_date = None
    if "trade_date" in day_bars.columns:
        trade_date = pd.to_datetime(day_bars["trade_date"], errors="coerce").dropna().max()
    if pd.isna(pd.to_datetime(trade_date, errors="coerce")):
        trade_date = pd.to_datetime(ts_dk, errors="coerce").dropna().max()
    session_clock = resolve_school_run_session_clock_dk(cfg.bar1_start, trade_date=trade_date)
    start_clock = session_clock.bar1_start_dk

    minute_of_day = ts_dk.dt.hour.to_numpy(dtype=int) * 60 + ts_dk.dt.minute.to_numpy(dtype=int)
    if minute_of_day.size == 0:
        return None
    start_minutes = int(start_clock.hour) * 60 + int(start_clock.minute)
    first_minutes = int(minute_of_day[0])
    last_minutes = int(minute_of_day[-1])

    if start_minutes <= first_minutes:
        start_pos = 0
    elif start_minutes > last_minutes:
        return None
    else:
        candidate_pos = np.flatnonzero(minute_of_day >= start_minutes)
        if candidate_pos.size == 0:
            return None
        start_pos = int(candidate_pos[0])

    bar1_pos = int(start_pos)
    bar2_pos = bar1_pos + 1
    if bar2_pos >= len(day_bars):
        return None

    highs = pd.to_numeric(day_bars["high"], errors="coerce").to_numpy(dtype=float)
    lows = pd.to_numeric(day_bars["low"], errors="coerce").to_numpy(dtype=float)
    bar_index = pd.to_numeric(day_bars["bar_index"], errors="coerce").fillna(0).astype(int).to_numpy(dtype=int)

    bar2_high = float(highs[bar2_pos])
    bar2_low = float(lows[bar2_pos])
    if not np.isfinite(bar2_high) or not np.isfinite(bar2_low) or bar2_high <= bar2_low:
        return None
    start_bar = int(bar_index[bar2_pos]) + 1
    offset = float(cfg.entry_offset_pts)
    return _SchoolRunSetup(
        start_bar=start_bar,
        long_entry=float(bar2_high + offset),
        long_stop=float(bar2_low),
        short_entry=float(bar2_low - offset),
        short_stop=float(bar2_high),
    )


def _empty_row(instrument: str, timeframe: str, trade_date: Any) -> dict[str, Any]:
    return {
        "instrument": instrument,
        "timeframe": timeframe,
        "trade_date": trade_date,
        f"{CUSTOM_PREFIX}_triggered": False,
        f"{CUSTOM_PREFIX}_direction": np.nan,
        f"{CUSTOM_PREFIX}_entry": np.nan,
        f"{CUSTOM_PREFIX}_stop": np.nan,
        f"{CUSTOM_PREFIX}_exit": np.nan,
        f"{CUSTOM_PREFIX}_risk_pts": np.nan,
        f"{CUSTOM_PREFIX}_result_pts_gross": np.nan,
        f"{CUSTOM_PREFIX}_trade_fee": np.nan,
        f"{CUSTOM_PREFIX}_trade_fee_usd": np.nan,
        f"{CUSTOM_PREFIX}_contracts_fill": np.nan,
        f"{CUSTOM_PREFIX}_contract_symbol": np.nan,
        f"{CUSTOM_PREFIX}_point_value": np.nan,
        f"{CUSTOM_PREFIX}_result_pts_net": np.nan,
        f"{CUSTOM_PREFIX}_result_pts": np.nan,
        f"{CUSTOM_PREFIX}_result_r": np.nan,
        f"{CUSTOM_PREFIX}_mfe_pts": np.nan,
        f"{CUSTOM_PREFIX}_mae_pts": np.nan,
        f"{CUSTOM_PREFIX}_stop_hit": False,
        f"{CUSTOM_PREFIX}_win": False,
        f"{CUSTOM_PREFIX}_executed_legs": 0,
        f"{CUSTOM_PREFIX}_position_units": 1.0,
        f"{CUSTOM_PREFIX}_result_capital": 0.0,
    }


def _simulate_school_run_day(day_bars: pd.DataFrame, cfg: CustomStrategyConfig) -> dict[str, Any] | None:
    setup = _school_run_setup(day_bars=day_bars, cfg=cfg)
    if setup is None:
        return None

    bar_index = pd.to_numeric(day_bars["bar_index"], errors="coerce").fillna(0).astype(int).to_numpy(dtype=int)
    highs = pd.to_numeric(day_bars["high"], errors="coerce").to_numpy(dtype=float)
    lows = pd.to_numeric(day_bars["low"], errors="coerce").to_numpy(dtype=float)
    closes = pd.to_numeric(day_bars["close"], errors="coerce").to_numpy(dtype=float)

    direction, _ = _first_dual_trigger(
        bar_index=bar_index,
        highs=highs,
        lows=lows,
        start_bar=int(setup.start_bar),
        long_entry_level=float(setup.long_entry),
        short_entry_level=float(setup.short_entry),
        max_trigger_bars=int(cfg.max_trigger_bars),
        allow_long=True,
        allow_short=True,
    )
    if direction not in {"long", "short"}:
        return None

    if direction == "long":
        entry = float(setup.long_entry)
        stop = float(setup.long_stop)
    else:
        entry = float(setup.short_entry)
        stop = float(setup.short_stop)

    model_id = _execution_model_id(cfg.execution_model)
    if model_id in {EXECUTION_MODEL_TOM_LIVE, EXECUTION_MODEL_TOM_AGGRESSIVE}:
        result = _simulate_breakout_arrays_tom_live(
            bar_index=bar_index,
            highs=highs,
            lows=lows,
            closes=closes,
            entry_level=float(entry),
            stop_level=float(stop),
            direction=str(direction),
            start_bar=int(setup.start_bar),
            max_trigger_bars=int(cfg.max_trigger_bars),
            strategy_name="School Run",
            execution_model=model_id,
        )
    else:
        result = _simulate_breakout_arrays(
            bar_index=bar_index,
            highs=highs,
            lows=lows,
            closes=closes,
            entry_level=float(entry),
            stop_level=float(stop),
            direction=str(direction),
            start_bar=int(setup.start_bar),
            max_trigger_bars=int(cfg.max_trigger_bars),
        )
    if not result.triggered:
        return None

    entry_time: time | None = None
    if result.entry_bar_index is not None:
        hit = day_bars[
            pd.to_numeric(day_bars["bar_index"], errors="coerce").fillna(0).astype(int)
            == int(result.entry_bar_index)
        ]
        if not hit.empty:
            ts = hit.iloc[0]["timestamp_dk"]
            if pd.notna(ts):
                entry_time = pd.Timestamp(ts).to_pydatetime().time()

    executed_legs = max(1, int(result.executed_legs or 1))
    contract_symbol = str(getattr(cfg, "contract_symbol", "") or "").strip().upper()
    if not contract_symbol:
        inferred = FeeModel.contract_for_instrument(str(cfg.instrument))
        contract_symbol = str(inferred or "").strip().upper()
    point_value = FeeModel.point_value_for_contract(contract_symbol)
    contracts_fill = 1.0
    contract_sides = float(contracts_fill) * 2.0

    trade_fee_usd = np.nan
    fee_per_side_usd = FeeModel.fee_per_side_usd_for_contract(contract_symbol)
    if fee_per_side_usd is not None and point_value is not None and point_value > 0:
        # Futures contract-fee path (Apex/Tradovate-style): fee side -> points.
        trade_fee_usd = float(fee_per_side_usd * contract_sides)
        trade_fee = float(FeeModel.fees_pts_from_contract_sides(contract_symbol, contract_sides))
    else:
        # Fallback for non-futures/custom instruments: spread model in points.
        per_trade_spread_pts = float(FeeModel.get_spread(str(cfg.instrument), entry_time))
        trade_fee = float(per_trade_spread_pts)
        if point_value is not None and point_value > 0:
            trade_fee_usd = float(per_trade_spread_pts * float(point_value) * contracts_fill)
    gross_pts = float(result.result_pts) if pd.notna(result.result_pts) else np.nan
    net_pts = float(gross_pts - trade_fee) if pd.notna(gross_pts) else np.nan
    risk_pts = float(result.risk_pts) if pd.notna(result.risk_pts) else np.nan
    result_r = float(net_pts / risk_pts) if pd.notna(net_pts) and pd.notna(risk_pts) and risk_pts > 0 else np.nan

    return {
        f"{CUSTOM_PREFIX}_triggered": True,
        f"{CUSTOM_PREFIX}_direction": str(direction),
        f"{CUSTOM_PREFIX}_entry": float(result.entry_price) if result.entry_price is not None else np.nan,
        f"{CUSTOM_PREFIX}_stop": float(result.stop_price) if result.stop_price is not None else np.nan,
        f"{CUSTOM_PREFIX}_exit": float(result.exit_price) if result.exit_price is not None else np.nan,
        f"{CUSTOM_PREFIX}_risk_pts": risk_pts,
        f"{CUSTOM_PREFIX}_result_pts_gross": gross_pts,
        f"{CUSTOM_PREFIX}_trade_fee": float(trade_fee),
        f"{CUSTOM_PREFIX}_trade_fee_usd": float(trade_fee_usd) if pd.notna(trade_fee_usd) else np.nan,
        f"{CUSTOM_PREFIX}_contracts_fill": float(contracts_fill),
        f"{CUSTOM_PREFIX}_contract_symbol": contract_symbol if contract_symbol else np.nan,
        f"{CUSTOM_PREFIX}_point_value": float(point_value) if point_value is not None else np.nan,
        f"{CUSTOM_PREFIX}_result_pts_net": net_pts,
        f"{CUSTOM_PREFIX}_result_pts": net_pts,
        f"{CUSTOM_PREFIX}_result_r": result_r,
        f"{CUSTOM_PREFIX}_mfe_pts": float(result.max_favorable_pts) if pd.notna(result.max_favorable_pts) else np.nan,
        f"{CUSTOM_PREFIX}_mae_pts": float(result.max_adverse_pts) if pd.notna(result.max_adverse_pts) else np.nan,
        f"{CUSTOM_PREFIX}_stop_hit": bool(result.stop_hit),
        f"{CUSTOM_PREFIX}_win": bool(pd.notna(net_pts) and float(net_pts) > 0.0),
        f"{CUSTOM_PREFIX}_executed_legs": int(executed_legs),
        f"{CUSTOM_PREFIX}_position_units": 1.0,
        f"{CUSTOM_PREFIX}_result_capital": float(net_pts) if pd.notna(net_pts) else 0.0,
    }


def compute_custom_strategy_rows(
    *,
    intraday_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    config: CustomStrategyConfig | dict[str, Any] | None,
) -> pd.DataFrame:
    """Compute runtime `custom_*` results using original School Run defaults."""
    cfg = coerce_custom_strategy_config(config)
    required_intraday = {"instrument", "trade_date", "timestamp", "open", "high", "low", "close"}
    required_daily = {"instrument", "trade_date"}
    if intraday_df.empty or daily_df.empty:
        return pd.DataFrame(columns=CUSTOM_RESULT_COLUMNS)
    if not required_intraday.issubset(set(intraday_df.columns)):
        return pd.DataFrame(columns=CUSTOM_RESULT_COLUMNS)
    if not required_daily.issubset(set(daily_df.columns)):
        return pd.DataFrame(columns=CUSTOM_RESULT_COLUMNS)

    daily_base = _prepare_daily_base(daily_df=daily_df, cfg=cfg)
    if daily_base.empty:
        return pd.DataFrame(columns=CUSTOM_RESULT_COLUMNS)
    selected_dates = set(_normalize_trade_date(daily_base["trade_date"]).dropna().tolist())
    intraday_15m = _build_15m_bars(intraday_df=intraday_df, cfg=cfg, trade_dates=selected_dates)
    if intraday_15m.empty:
        return pd.DataFrame(columns=CUSTOM_RESULT_COLUMNS)
    intraday_15m["trade_date"] = _normalize_trade_date(intraday_15m["trade_date"])
    grouped = intraday_15m.groupby("trade_date", sort=False)

    rows: list[dict[str, Any]] = []
    for day_meta in daily_base.itertuples(index=False):
        trade_date = pd.Timestamp(getattr(day_meta, "trade_date")).normalize()
        row = _empty_row(instrument=str(cfg.instrument), timeframe=TARGET_TIMEFRAME, trade_date=trade_date)
        if trade_date in grouped.groups:
            day_bars = grouped.get_group(trade_date).copy().sort_values("timestamp_dk")
            simulated = _simulate_school_run_day(day_bars=day_bars, cfg=cfg)
            if isinstance(simulated, dict):
                row.update(simulated)
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=CUSTOM_RESULT_COLUMNS)
    return pd.DataFrame(rows, columns=CUSTOM_RESULT_COLUMNS)
