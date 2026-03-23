"""Pure data-coercion helpers for the Custom Human trading engine.

This module owns every function that transforms raw/untyped input into a
typed, validated Python structure.  All functions here are pure (no I/O,
no shared-state mutation, no threading).

Extracted from engine_core.py as Phase 3 of the engine_core split.

globals() note
──────────────
The ``_*_startup`` variants use ``globals().get("<full_name>")`` to delegate
to the full coercion function when it is already defined.  Because both the
startup and full versions now live in the *same* module, ``globals()`` at
call time always finds the full version — the startup fallback path is only
a safety net and is effectively dead code in normal operation.

The one exception is ``_coerce_custom_human_diagnostics_events_startup``,
which delegates to ``_coerce_custom_human_diagnostics_events`` imported from
``diagnostics_manager``.  That import is at module top-level so ``globals()``
finds it too.
"""
from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from src.trading.diagnostics_manager import _coerce_custom_human_diagnostics_events

APP_TIMEZONE = ZoneInfo("Europe/Copenhagen")
CUSTOM_HUMAN_DIAGNOSTIC_EVENT_LIMIT = 50


# ---------------------------------------------------------------------------
# Primitive float helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _safe_float_startup(value: Any) -> float | None:
    """Startup-safe float coercion for cold-start paths before later helpers exist."""
    safe_float = globals().get("_safe_float")
    if callable(safe_float):
        return safe_float(value)
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _coerce_optional_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Live state
# ---------------------------------------------------------------------------

def _default_custom_human_live_state(trade_date: str = "") -> dict[str, Any]:
    return {
        "trade_date": str(trade_date or ""),
        "phase": "waiting_for_setup",
        "direction": "",
        "entry_price": None,
        "stop_price": None,
        "active_stop": None,
        "risk_pts": None,
        "entry_bar_index": None,
        "last_processed_bar_index": None,
        "break_even_armed": False,
        "add_count_sent": 0,
        "position_open": False,
        "market_timestamp": "",
        "last_note": "",
        "max_favorable_pts": 0.0,
        "max_adverse_pts": 0.0,
        "start_bar": None,
        "max_trigger_bar": None,
        "pending_signal_id": "",
        "pending_event": "",
        "pending_candidate": None,
        "reconcile_required": False,
        "broker_position_qty": None,
        "broker_account_value": "",
        "last_broker_snapshot": None,
        "last_reconciled_at": "",
        "exit_signal_seq": 0,
    }


def _coerce_custom_human_live_state(raw: dict[str, Any] | None) -> dict[str, Any]:
    state = _default_custom_human_live_state()
    payload = raw if isinstance(raw, dict) else {}
    for key in state:
        if key in payload:
            state[key] = payload.get(key)
    state["trade_date"] = str(state.get("trade_date", "") or "")
    state["phase"] = str(state.get("phase", "waiting_for_setup") or "waiting_for_setup")
    state["direction"] = str(state.get("direction", "") or "")
    state["break_even_armed"] = bool(state.get("break_even_armed", False))
    state["add_count_sent"] = int(max(0, int(state.get("add_count_sent", 0) or 0)))
    state["position_open"] = bool(state.get("position_open", False))
    state["market_timestamp"] = str(state.get("market_timestamp", "") or "")
    state["last_note"] = str(state.get("last_note", "") or "")
    state["max_favorable_pts"] = float(_coerce_optional_float(state.get("max_favorable_pts")) or 0.0)
    state["max_adverse_pts"] = float(_coerce_optional_float(state.get("max_adverse_pts")) or 0.0)
    state["pending_signal_id"] = str(state.get("pending_signal_id", "") or "")
    state["pending_event"] = str(state.get("pending_event", "") or "")
    state["pending_candidate"] = dict(state.get("pending_candidate")) if isinstance(state.get("pending_candidate"), dict) else None
    state["reconcile_required"] = bool(state.get("reconcile_required", False))
    state["broker_position_qty"] = _coerce_optional_float(state.get("broker_position_qty"))
    state["broker_account_value"] = str(state.get("broker_account_value", "") or "")
    state["last_broker_snapshot"] = (
        dict(state.get("last_broker_snapshot")) if isinstance(state.get("last_broker_snapshot"), dict) else None
    )
    state["last_reconciled_at"] = str(state.get("last_reconciled_at", "") or "")
    state["exit_signal_seq"] = int(max(0, int(state.get("exit_signal_seq", 0) or 0)))
    for numeric_key in (
        "entry_price",
        "stop_price",
        "active_stop",
        "risk_pts",
    ):
        state[numeric_key] = _coerce_optional_float(state.get(numeric_key))
    for int_key in ("entry_bar_index", "last_processed_bar_index", "start_bar", "max_trigger_bar"):
        try:
            state[int_key] = int(state[int_key]) if state.get(int_key) is not None else None
        except Exception:
            state[int_key] = None
    return state


# ---------------------------------------------------------------------------
# Tradovate snapshot
# ---------------------------------------------------------------------------

def _coerce_custom_human_tradovate_snapshot(snapshot_raw: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(snapshot_raw, dict):
        return None
    snapshot = dict(snapshot_raw)
    for key in ("position_qty", "last_price", "bid_price", "ask_price", "spread"):
        snapshot[key] = _safe_float(snapshot.get(key))
    for key in ("connected", "account_ok", "instrument_visible", "position_open", "quote_ready"):
        snapshot[key] = bool(snapshot.get(key, False))
    snapshot["account_value"] = str(snapshot.get("account_value", "") or "")
    snapshot["instrument_match"] = str(snapshot.get("instrument_match", "") or "")
    snapshot["order_quantity_value"] = str(snapshot.get("order_quantity_value", "") or "")
    snapshot["page_title"] = str(snapshot.get("page_title", "") or "")
    snapshot["page_url"] = str(snapshot.get("page_url", "") or "")
    snapshot["market_clock_text"] = str(snapshot.get("market_clock_text", "") or "")
    snapshot["observed_at"] = str(snapshot.get("observed_at", "") or "")
    return snapshot


def _coerce_broker_snapshot_qty(snapshot: dict[str, Any] | None) -> float:
    if not isinstance(snapshot, dict):
        return 0.0
    qty = _safe_float(snapshot.get("position_qty"))
    return abs(float(qty or 0.0))


def _normalize_custom_human_confirmation_snapshot(
    snapshot_raw: dict[str, Any] | None,
) -> dict[str, Any] | None:
    snapshot = _coerce_custom_human_tradovate_snapshot(snapshot_raw)
    if not isinstance(snapshot, dict):
        return None
    position_qty = _coerce_broker_snapshot_qty(snapshot)
    return {
        "connected": bool(snapshot.get("connected", False)),
        "account_ok": bool(snapshot.get("account_ok", True)),
        "account_value": str(snapshot.get("account_value", "") or ""),
        "instrument_visible": bool(snapshot.get("instrument_visible", False)),
        "instrument_match": str(snapshot.get("instrument_match", "") or ""),
        "position_open": bool(snapshot.get("position_open", False)),
        "position_qty": float(position_qty),
        "position_side": str(snapshot.get("position_side", "") or ""),
    }


def _normalize_custom_human_confirmation_snapshot_startup(
    snapshot_raw: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Startup-safe snapshot normalizer before later helpers are defined."""
    normalize_snapshot = globals().get("_normalize_custom_human_confirmation_snapshot")
    if callable(normalize_snapshot):
        return normalize_snapshot(snapshot_raw)
    if not isinstance(snapshot_raw, dict):
        return None
    position_qty = _coerce_optional_float(snapshot_raw.get("position_qty"))
    return {
        "connected": bool(snapshot_raw.get("connected", False)),
        "account_ok": bool(snapshot_raw.get("account_ok", False)),
        "account_value": str(snapshot_raw.get("account_value", "") or ""),
        "instrument_visible": bool(snapshot_raw.get("instrument_visible", False)),
        "instrument_match": str(snapshot_raw.get("instrument_match", "") or ""),
        "position_open": bool(snapshot_raw.get("position_open", False)),
        "position_qty": abs(float(position_qty or 0.0)),
        "position_side": str(snapshot_raw.get("position_side", "") or ""),
    }


# ---------------------------------------------------------------------------
# Inflight orders
# ---------------------------------------------------------------------------

def _coerce_custom_human_inflight_orders(raw: Any) -> dict[str, dict[str, Any]]:
    payload = raw if isinstance(raw, dict) else {}
    coerced: dict[str, dict[str, Any]] = {}
    for signal_id, item in payload.items():
        token = str(signal_id or "").strip()
        if not token or not isinstance(item, dict):
            continue
        recovery_snapshot_raw = item.get("recovery_last_confirmation_snapshot")
        coerced[token] = {
            "signal_id": token,
            "signal": str(item.get("signal", "") or "").strip().upper(),
            "event": str(item.get("event", "") or "").strip().lower(),
            "action": str(item.get("action", "") or "").strip().lower(),
            "position_key": str(item.get("position_key", "") or "").strip(),
            "instrument": str(item.get("instrument", "") or "").strip().upper(),
            "quantity": float(_coerce_optional_float(item.get("quantity")) or 0.0),
            "status": str(item.get("status", "reserved") or "reserved").strip().lower(),
            "reserved_at": str(item.get("reserved_at", "") or ""),
            "queued_at": str(item.get("queued_at", "") or ""),
            "clicked_at": str(item.get("clicked_at", "") or ""),
            "confirmed_at": str(item.get("confirmed_at", "") or ""),
            "last_message": str(item.get("last_message", "") or ""),
            "candidate": dict(item.get("candidate")) if isinstance(item.get("candidate"), dict) else None,
            "position_snapshot": (
                dict(item.get("position_snapshot")) if isinstance(item.get("position_snapshot"), dict) else None
            ),
            "recovery_confirmation_streak": int(max(0, int(item.get("recovery_confirmation_streak", 0) or 0))),
            "recovery_last_confirmation_snapshot": _normalize_custom_human_confirmation_snapshot_startup(
                recovery_snapshot_raw
            )
            if isinstance(recovery_snapshot_raw, dict)
            else None,
        }
    return coerced


# ---------------------------------------------------------------------------
# Price samples and bars
# ---------------------------------------------------------------------------

def _coerce_custom_human_tradovate_price_samples(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    samples: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        observed_at = str(item.get("observed_at", "") or "")
        timestamp = pd.to_datetime(observed_at, errors="coerce")
        if pd.isna(timestamp):
            continue
        ts = pd.Timestamp(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize(APP_TIMEZONE)
        else:
            ts = ts.tz_convert(APP_TIMEZONE)
        price = _safe_float(item.get("price"))
        if price is None:
            continue
        samples.append(
            {
                "observed_at": ts.isoformat(),
                "price": float(price),
                "instrument_match": str(item.get("instrument_match", "") or ""),
                "account_value": str(item.get("account_value", "") or ""),
                "price_source": str(item.get("price_source", "") or ""),
            }
        )
    return samples


def _coerce_custom_human_tradovate_bars(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    bars: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        ts = pd.to_datetime(item.get("timestamp_dk"), errors="coerce")
        trade_date = pd.to_datetime(item.get("trade_date"), errors="coerce")
        if pd.isna(ts) or pd.isna(trade_date):
            continue
        ts_norm = pd.Timestamp(ts)
        if ts_norm.tzinfo is None:
            ts_norm = ts_norm.tz_localize(APP_TIMEZONE)
        else:
            ts_norm = ts_norm.tz_convert(APP_TIMEZONE)
        trade_date_norm = pd.Timestamp(trade_date).normalize()
        bars.append(
            {
                "trade_date": trade_date_norm.strftime("%Y-%m-%d"),
                "timestamp_dk": ts_norm.isoformat(),
                "bar_index": int(item.get("bar_index", 0) or 0),
                "open": float(item.get("open", 0.0) or 0.0),
                "high": float(item.get("high", 0.0) or 0.0),
                "low": float(item.get("low", 0.0) or 0.0),
                "close": float(item.get("close", 0.0) or 0.0),
                "sample_count": int(item.get("sample_count", 0) or 0),
                "instrument_match": str(item.get("instrument_match", "") or ""),
            }
        )
    return bars


# ---------------------------------------------------------------------------
# Execution config
# ---------------------------------------------------------------------------

def _coerce_live_execution_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    cfg = {
        "contract_symbol": "MYM",
        "point_value_usd": 0.5,
        "fee_per_side_usd": 0.52,
        "default_quantity": 1,
        "max_positions_per_strategy": 1,
        "max_daily_loss_r": 3.0,
        "max_consecutive_losses": 4,
        "webhook_rate_limit_per_min": 60,
        "webhook_rate_limit_per_hour": 500,
        "webhook_timeout_sec": 10.0,
        "webhook_max_retries": 3,
        "webhook_backoff_base_sec": 0.5,
        "webhook_url": "",
    }
    if isinstance(raw, dict):
        for key, value in raw.items():
            if key in cfg:
                cfg[key] = value

    def _to_int(name: str, fallback: int, minimum: int) -> int:
        try:
            token = int(cfg.get(name, fallback))
        except (TypeError, ValueError):
            token = fallback
        return max(minimum, token)

    def _to_float(name: str, fallback: float, minimum: float) -> float:
        try:
            token = float(cfg.get(name, fallback))
        except (TypeError, ValueError):
            token = fallback
        return max(minimum, token)

    cfg["contract_symbol"] = str(cfg.get("contract_symbol", "MYM")).strip().upper() or "MYM"
    cfg["webhook_url"] = str(cfg.get("webhook_url", "")).strip()
    cfg["default_quantity"] = _to_int("default_quantity", 1, 1)
    cfg["max_positions_per_strategy"] = _to_int("max_positions_per_strategy", 1, 1)
    cfg["max_consecutive_losses"] = _to_int("max_consecutive_losses", 4, 1)
    cfg["webhook_rate_limit_per_min"] = _to_int("webhook_rate_limit_per_min", 60, 1)
    cfg["webhook_rate_limit_per_hour"] = _to_int(
        "webhook_rate_limit_per_hour",
        max(int(cfg["webhook_rate_limit_per_min"]), 500),
        int(cfg["webhook_rate_limit_per_min"]),
    )
    cfg["point_value_usd"] = _to_float("point_value_usd", 0.5, 0.0001)
    cfg["fee_per_side_usd"] = _to_float("fee_per_side_usd", 0.52, 0.0)
    cfg["max_daily_loss_r"] = _to_float("max_daily_loss_r", 3.0, 0.1)
    cfg["webhook_timeout_sec"] = _to_float("webhook_timeout_sec", 10.0, 1.0)
    cfg["webhook_max_retries"] = _to_int("webhook_max_retries", 3, 0)
    cfg["webhook_backoff_base_sec"] = _to_float("webhook_backoff_base_sec", 0.5, 0.05)
    return cfg


# ---------------------------------------------------------------------------
# Startup variants (delegate to full versions via globals())
# ---------------------------------------------------------------------------

def _coerce_custom_human_tradovate_snapshot_startup(snapshot_raw: dict[str, Any] | None) -> dict[str, Any] | None:
    """Startup-safe snapshot coercion for code paths that can run before later helpers are defined."""
    snapshot_coercer = globals().get("_coerce_custom_human_tradovate_snapshot")
    if callable(snapshot_coercer):
        return snapshot_coercer(snapshot_raw)
    if not isinstance(snapshot_raw, dict):
        return None
    snapshot = dict(snapshot_raw)
    for key in ("position_qty", "last_price", "bid_price", "ask_price", "spread"):
        snapshot[key] = _safe_float_startup(snapshot.get(key))
    for key in ("connected", "account_ok", "instrument_visible", "position_open", "quote_ready"):
        snapshot[key] = bool(snapshot.get(key, False))
    for key in (
        "account_value",
        "instrument_match",
        "order_quantity_value",
        "page_title",
        "page_url",
        "market_clock_text",
        "observed_at",
    ):
        snapshot[key] = str(snapshot.get(key, "") or "")
    return snapshot


def _coerce_custom_human_diagnostics_events_startup(raw: Any) -> list[dict[str, str]]:
    """Startup-safe diagnostics coercion before later helpers are defined."""
    diagnostics_coercer = globals().get("_coerce_custom_human_diagnostics_events")
    if callable(diagnostics_coercer):
        return diagnostics_coercer(raw)
    items = raw if isinstance(raw, list) else []
    events: list[dict[str, str]] = []
    for item in items[-CUSTOM_HUMAN_DIAGNOSTIC_EVENT_LIMIT:]:
        if not isinstance(item, dict):
            continue
        events.append(
            {
                "ts": str(item.get("ts", "") or ""),
                "kind": str(item.get("kind", "") or ""),
                "tone": str(item.get("tone", "") or "info"),
                "headline": str(item.get("headline", "") or ""),
                "detail": str(item.get("detail", "") or ""),
            }
        )
    return events[-CUSTOM_HUMAN_DIAGNOSTIC_EVENT_LIMIT:]


def _coerce_custom_human_inflight_orders_startup(raw: Any) -> dict[str, dict[str, Any]]:
    inflight_coercer = globals().get("_coerce_custom_human_inflight_orders")
    if callable(inflight_coercer):
        return inflight_coercer(raw)
    payload = raw if isinstance(raw, dict) else {}
    coerced: dict[str, dict[str, Any]] = {}
    for signal_id, item in payload.items():
        token = str(signal_id or "").strip()
        if not token or not isinstance(item, dict):
            continue
        recovery_snapshot_raw = item.get("recovery_last_confirmation_snapshot")
        coerced[token] = {
            "signal_id": token,
            "signal": str(item.get("signal", "") or "").strip().upper(),
            "event": str(item.get("event", "") or "").strip().lower(),
            "action": str(item.get("action", "") or "").strip().lower(),
            "position_key": str(item.get("position_key", "") or "").strip(),
            "instrument": str(item.get("instrument", "") or "").strip().upper(),
            "quantity": float(_safe_float_startup(item.get("quantity")) or 0.0),
            "status": str(item.get("status", "reserved") or "reserved").strip().lower(),
            "reserved_at": str(item.get("reserved_at", "") or ""),
            "queued_at": str(item.get("queued_at", "") or ""),
            "clicked_at": str(item.get("clicked_at", "") or ""),
            "confirmed_at": str(item.get("confirmed_at", "") or ""),
            "last_message": str(item.get("last_message", "") or ""),
            "candidate": dict(item.get("candidate")) if isinstance(item.get("candidate"), dict) else None,
            "position_snapshot": (
                dict(item.get("position_snapshot")) if isinstance(item.get("position_snapshot"), dict) else None
            ),
            "recovery_confirmation_streak": int(max(0, int(item.get("recovery_confirmation_streak", 0) or 0))),
            "recovery_last_confirmation_snapshot": _normalize_custom_human_confirmation_snapshot_startup(
                recovery_snapshot_raw if isinstance(recovery_snapshot_raw, dict) else None
            ),
        }
    return coerced


def _coerce_custom_human_tradovate_price_samples_startup(raw: Any) -> list[dict[str, Any]]:
    """Startup-safe price sample coercion before later helpers are defined."""
    sample_coercer = globals().get("_coerce_custom_human_tradovate_price_samples")
    if callable(sample_coercer):
        return sample_coercer(raw)
    items = raw if isinstance(raw, list) else []
    samples: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        observed_at = str(item.get("observed_at", "") or "")
        timestamp = pd.to_datetime(observed_at, errors="coerce")
        if pd.isna(timestamp):
            continue
        ts = pd.Timestamp(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize(APP_TIMEZONE)
        else:
            ts = ts.tz_convert(APP_TIMEZONE)
        price = _safe_float_startup(item.get("price"))
        if price is None:
            continue
        samples.append(
            {
                "observed_at": ts.isoformat(),
                "price": float(price),
                "instrument_match": str(item.get("instrument_match", "") or ""),
                "account_value": str(item.get("account_value", "") or ""),
                "price_source": str(item.get("price_source", "") or ""),
            }
        )
    return samples


def _coerce_custom_human_tradovate_bars_startup(raw: Any) -> list[dict[str, Any]]:
    """Startup-safe Tradovate bar coercion before later helpers are defined."""
    bars_coercer = globals().get("_coerce_custom_human_tradovate_bars")
    if callable(bars_coercer):
        return bars_coercer(raw)
    items = raw if isinstance(raw, list) else []
    bars: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ts = pd.to_datetime(item.get("timestamp_dk"), errors="coerce")
        trade_date = pd.to_datetime(item.get("trade_date"), errors="coerce")
        if pd.isna(ts) or pd.isna(trade_date):
            continue
        ts_norm = pd.Timestamp(ts)
        if ts_norm.tzinfo is None:
            ts_norm = ts_norm.tz_localize(APP_TIMEZONE)
        else:
            ts_norm = ts_norm.tz_convert(APP_TIMEZONE)
        trade_date_norm = pd.Timestamp(trade_date).normalize()
        bars.append(
            {
                "trade_date": trade_date_norm.strftime("%Y-%m-%d"),
                "timestamp_dk": ts_norm.isoformat(),
                "bar_index": int(item.get("bar_index", 0) or 0),
                "open": float(_safe_float_startup(item.get("open")) or 0.0),
                "high": float(_safe_float_startup(item.get("high")) or 0.0),
                "low": float(_safe_float_startup(item.get("low")) or 0.0),
                "close": float(_safe_float_startup(item.get("close")) or 0.0),
                "sample_count": int(item.get("sample_count", 0) or 0),
                "instrument_match": str(item.get("instrument_match", "") or ""),
            }
        )
    return bars
