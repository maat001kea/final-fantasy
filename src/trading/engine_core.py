from __future__ import annotations

import asyncio
import builtins
import hashlib
import json
import logging
import math
import os
import queue
import random
import tempfile
import time as time_module
import threading
from datetime import datetime, time
from pathlib import Path
from typing import Any, Awaitable, Callable, MutableMapping
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yaml

from src.custom_engine import _build_15m_bars, _school_run_setup, resolve_school_run_session_clock_dk
from src.custom_types import CustomStrategyConfig
from src.live_dukascopy import DUKASCOPY_SYMBOL_MAP, load_dukascopy_cached_intraday_data, load_dukascopy_intraday_data
from src.live_yahoo import YahooApiError, load_yahoo_intraday_data
from src.preprocessing import prepare_intraday_data
from src.strategies import (
    EXECUTION_MODEL_SIMPLIFIED,
    EXECUTION_MODEL_TOM_AGGRESSIVE,
    EXECUTION_MODEL_TOM_LIVE,
    TomLiveManagementConfig,
    _apply_stop_cap,
    _management_config_for_model,
)
from src.traderspost_payloads import build_aggressive_action_payload
from src.trading.audit_db import AuditDB
from src.trading.broker_adapter_base import BrokerAdapter, OrderRequest, OrderResult, OrderSide, OrderStatus, OrderType
from src.trading.cdp_adapter import CDPConnectionError, CDPHumanAdapter
from src.trading.pitbull_reconnection import run_with_reconnect
from src.trading.execution_pipeline import ExecutionPipeline, PipelineConfig
from src.trading.position_sizer import load_sizing_config
from src.trading.risk_gate import GateDecision
from src.trading.timing_manager import (
    CUSTOM_HUMAN_BIO_POLL_AMPLITUDE,
    CUSTOM_HUMAN_BIO_POLL_PERIOD_SECONDS,
    CUSTOM_HUMAN_IDLE_HEALTH_CHECK_AFTER_SECONDS,
    CUSTOM_HUMAN_IDLE_SCROLL_NOISE_MAX_SECONDS,
    CUSTOM_HUMAN_IDLE_SCROLL_NOISE_MIN_SECONDS,
    CUSTOM_HUMAN_JITTER_MAX_FACTOR,
    CUSTOM_HUMAN_JITTER_MIN_FACTOR,
    CUSTOM_HUMAN_LIVE_ARMED_POLL_SECONDS,
    CUSTOM_HUMAN_LIVE_CONFIRMATION_POLL_SECONDS,
    CUSTOM_HUMAN_LIVE_DEGRADED_POLL_SECONDS,
    CUSTOM_HUMAN_LIVE_POLL_SECONDS,
    CUSTOM_HUMAN_LIVE_POSITION_POLL_SECONDS,
    CUSTOM_HUMAN_RECOVERY_DUPLICATE_WINDOW_SECONDS,
    CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_ACTIVE_POLL_SECONDS,
    CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_ARMED_POLL_SECONDS,
    CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_POLL_SECONDS,
    _apply_custom_human_bio_modulation,
    _classify_custom_human_restart_suppression,
    _custom_human_bio_polling_eligible,
    _custom_human_duplicate_retry_signal_id,
    _custom_human_is_recent_recovery,
    _custom_human_jittered_interval,
    _custom_human_live_observer_poll_seconds,
    _custom_human_should_bypass_live_feed_cache,
    _custom_human_should_retry_router_duplicate,
    _custom_human_snapshot_poll_seconds,
    _get_bio_modulation,
    _mark_custom_human_recent_recovery,
    _mark_custom_human_worker_interaction,
    _maybe_schedule_custom_human_idle_health_check,
    _maybe_schedule_custom_human_idle_scroll_noise,
    _seed_custom_human_bio_polling_profile,
    _should_abort_custom_human_ui_reset,
    _wait_for_custom_human_post_flat_cooldown,
)
from src.trading.snapshot_manager import (
    CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_STALE_SECONDS,
    _custom_human_diagnostics_snapshot,
    _custom_human_post_entry_health_snapshot,
    _custom_human_preflight_snapshot,
    _custom_human_tradovate_snapshot_status,
    _custom_human_watchdog_blocks_candidate,
    _custom_human_watchdog_snapshot,
    _format_custom_human_diag_age,
    _format_custom_human_snapshot_pair_for_log,
)
from src.trading.persistence_manager import (
    CUSTOM_HUMAN_RUNTIME_STATE_FILE,
    _apply_custom_human_startup_inflight_guard,
    _capture_custom_human_runtime_state,
    _custom_human_inflight_timestamp_value,
    _custom_human_runtime_profile_is_valid,
    _custom_human_store_key,
    _load_custom_human_runtime_state,
    _normalize_custom_human_auto_requested,
    _persist_custom_human_runtime_state,
    _reset_stale_custom_human_reconcile_on_start,
    _restore_custom_human_runtime_state_into_shared,
    _save_custom_human_runtime_state,
)
from src.trading.coercion_manager import (
    _coerce_broker_snapshot_qty,
    _coerce_custom_human_inflight_orders,
    _coerce_custom_human_inflight_orders_startup,
    _coerce_custom_human_live_state,
    _coerce_custom_human_tradovate_bars,
    _coerce_custom_human_tradovate_bars_startup,
    _coerce_custom_human_tradovate_price_samples,
    _coerce_custom_human_tradovate_price_samples_startup,
    _coerce_custom_human_tradovate_snapshot,
    _coerce_custom_human_tradovate_snapshot_startup,
    _coerce_custom_human_diagnostics_events_startup,
    _coerce_live_execution_config,
    _coerce_optional_float,
    _default_custom_human_live_state,
    _normalize_custom_human_confirmation_snapshot,
    _normalize_custom_human_confirmation_snapshot_startup,
    _safe_float,
    _safe_float_startup,
)
from src.trading.diagnostics_manager import (
    _append_custom_human_diagnostic_event,
    _coerce_custom_human_diagnostics_events,
    _log_custom_human_runtime_event,
    _record_custom_human_snapshot_diagnostic,
    _record_custom_human_watchdog_diagnostic,
)
from src.trading.risk_gate_manager import (
    _clear_custom_human_riskgate_cache,
    _custom_human_gate_state_snapshot,
    _serialize_custom_human_risk_gate_state,
)
from src.trading.state_buffer import (
    StateVerificationBuffer,
    STATE_VERIFICATION_REQUIRED_CONFIRMATIONS,
    _SENTINEL as _STATE_VERIFICATION_SENTINEL,
)
from src.trading_signal_router import TradingSignalRouter


_APP_LOGGER = logging.getLogger("final_fantasy.engine_core")
if not _APP_LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    _APP_LOGGER.addHandler(_handler)
    _APP_LOGGER.setLevel(logging.INFO)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


# StateVerificationBuffer is imported from src.trading.state_buffer above.
# _STATE_VERIFICATION_SENTINEL and STATE_VERIFICATION_REQUIRED_CONFIRMATIONS
# are re-exported as module-level names so existing code and tests that
# reference ``engine_core.StateVerificationBuffer`` / ``engine_core._STATE_VERIFICATION_SENTINEL``
# continue to work without changes.

_PATCH_NAMESPACE: MutableMapping[str, Any] | None = None
_PATCHABLE_DEFAULTS: dict[str, Callable[..., Any]] = {}


def register_patch_namespace(namespace: MutableMapping[str, Any] | None) -> None:
    global _PATCH_NAMESPACE
    _PATCH_NAMESPACE = namespace


def _external_value(name: str, default: Any) -> Any:
    namespace = _PATCH_NAMESPACE
    if isinstance(namespace, MutableMapping) and name in namespace:
        value = namespace[name]
        if not getattr(value, '_engine_shim', False):
            return value
    return default


class _PatchSafeProxy:
    def __init__(self, name: str, default: Callable[..., Any]) -> None:
        self._name = name
        self._default = default
        self.__name__ = getattr(default, '__name__', name)
        self.__doc__ = getattr(default, '__doc__', None)
        self.__wrapped__ = default

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        namespace = _PATCH_NAMESPACE
        if isinstance(namespace, MutableMapping):
            override = namespace.get(self._name)
            if callable(override) and not getattr(override, '_engine_shim', False):
                return override(*args, **kwargs)
        return self._default(*args, **kwargs)

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._default, attr)


def _install_patch_proxy(name: str) -> None:
    value = globals().get(name)
    if not callable(value) or isinstance(value, _PatchSafeProxy):
        return
    _PATCHABLE_DEFAULTS[name] = value
    globals()[name] = _PatchSafeProxy(name, value)


APP_TIMEZONE = ZoneInfo("Europe/Copenhagen")

def _custom_human_preconfigured_auto_targets(
    session_state: Any | None = None,
) -> dict[str, Any]:
    """Resolve full-auto button targets from the selected Custom Human UI mode."""
    state = session_state if session_state is not None else {}
    selector_mode = str(state.get("sa_custom_human_selector_mode", "Auto (platform selectors)")).strip()

    if selector_mode == "CSS Selector":
        buy_selector = str(state.get("sa_custom_human_buy_selector", "")).strip()
        sell_selector = str(state.get("sa_custom_human_sell_selector", "")).strip()
        flatten_selector = str(state.get("sa_custom_human_flatten_selector", "")).strip()
        buy_info = {"selector": buy_selector} if buy_selector else None
        sell_info = {"selector": sell_selector} if sell_selector else None
        if not buy_info and not sell_info:
            return {
                "mode": "css",
                "buy": None,
                "sell": None,
                "flat": None,
                "error": "Ingen Buy/Sell selectors sat. Angiv mindst én CSS selector før full-auto start.",
            }
        flat_info = {"selector": flatten_selector} if flatten_selector else None
        return {"mode": "css", "buy": buy_info, "sell": sell_info, "flat": flat_info, "error": None}

    if selector_mode == "Manuelle koordinater":
        return {
            "mode": "manual",
            "buy": None,
            "sell": None,
            "flat": None,
            "error": "Fuldauto understøtter ikke `Manuelle koordinater` endnu. Brug `CSS Selector` eller `Auto`.",
        }

    return {"mode": "auto", "buy": None, "sell": None, "flat": None, "error": None}


def _prepare_custom_human_auto_targets(
    adapter: "CDPHumanAdapter",
    shared: dict[str, Any],
    *,
    session_state: Any | None = None,
) -> dict[str, Any]:
    """Resolve and validate auto-trading targets without starting runtime."""
    state = session_state if session_state is not None else {}
    target_cfg = _custom_human_preconfigured_auto_targets(state)
    target_error = target_cfg.get("error")
    if target_error is not None and str(target_error).strip():
        shared["last_dom_preflight"] = {
            "success": False,
            "error_msg": str(target_error).strip(),
            "account_match": False,
            "instrument_match": False,
            "module_found": False,
            "quantity_found": False,
            "quantity_value": "",
            "contract": None,
        }
        return {
            "ok": False,
            "status": "invalid_target_config",
            "message": str(target_error).strip(),
            "buy": None,
            "sell": None,
            "flat": None,
        }

    if str(target_cfg.get("mode")) == "css":
        validated = _validate_custom_human_auto_targets(adapter, target_cfg)
        buy_info = validated.get("buy")
        sell_info = validated.get("sell")
        flat_info = validated.get("flat")
    else:
        discovered = _run_cdp_adapter_task(
            adapter,
            adapter.auto_discover_trading_buttons,
        )
        buy_info = discovered.get("buy") if discovered else None
        sell_info = discovered.get("sell") if discovered else None
        flat_info = None

        # --- Hardkodet CSS-fallback (Tradovate known-good selectors) ---
        # auto_discover_trading_buttons bruger tekst-matching og kan misse
        # knapperne hvis Tradovate's module-container har skiftet klasse.
        # Prøv de tre velkendte CSS-selectors direkte på hele dokumentet.
        _TRADOVATE_DIRECT_CSS: dict[str, str] = {
            "buy":  "div.btn.btn-success",
            "sell": "div.btn.btn-danger:not(.panic-button)",
            "flat": "button.btn.btn-default:not(.dropdown-toggle)",
        }
        if not buy_info:
            try:
                _center = _run_cdp_adapter_task(
                    adapter,
                    lambda s=_TRADOVATE_DIRECT_CSS["buy"]: adapter.get_element_center(s),
                )
                if _center:
                    buy_info = {
                        "selector": _TRADOVATE_DIRECT_CSS["buy"],
                        "x": float(_center[0]),
                        "y": float(_center[1]),
                        "source": "css_direct_fallback",
                    }
            except Exception as exc:
                _APP_LOGGER.debug("CSS direct-fallback (buy) fejlede: %s", exc)
        if not sell_info:
            try:
                _center = _run_cdp_adapter_task(
                    adapter,
                    lambda s=_TRADOVATE_DIRECT_CSS["sell"]: adapter.get_element_center(s),
                )
                if _center:
                    sell_info = {
                        "selector": _TRADOVATE_DIRECT_CSS["sell"],
                        "x": float(_center[0]),
                        "y": float(_center[1]),
                        "source": "css_direct_fallback",
                    }
            except Exception as exc:
                _APP_LOGGER.debug("CSS direct-fallback (sell) fejlede: %s", exc)
        if not flat_info:
            try:
                _center = _run_cdp_adapter_task(
                    adapter,
                    lambda s=_TRADOVATE_DIRECT_CSS["flat"]: adapter.get_element_center(s),
                )
                if _center:
                    flat_info = {
                        "selector": _TRADOVATE_DIRECT_CSS["flat"],
                        "x": float(_center[0]),
                        "y": float(_center[1]),
                        "source": "css_direct_fallback",
                    }
            except Exception as exc:
                _APP_LOGGER.debug("CSS direct-fallback (flat) fejlede: %s", exc)
        # ----------------------------------------------------------------

    if not buy_info and not sell_info:
        shared["last_dom_preflight"] = {
            "success": False,
            "error_msg": "Kunne hverken finde Buy eller Sell target i den aktive Tradovate-module.",
            "account_match": False,
            "instrument_match": False,
            "module_found": False,
            "quantity_found": False,
            "quantity_value": "",
            "contract": None,
        }
        _log_custom_human_runtime_event(
            shared,
            headline="AUTO TARGETS MANGLER",
            detail="Kunne hverken finde Buy eller Sell target i den aktive Tradovate-module.",
            tone="warning",
            kind="warning",
            level=logging.WARNING,
        )
        return {
            "ok": False,
            "status": "missing_trade_buttons",
            "message": (
                "❌ Kunne hverken finde Køb- eller Salg-knap. "
                "Kontrollér dine selectors eller skift til `Auto` og prøv igen."
            ),
            "buy": None,
            "sell": None,
            "flat": None,
        }

    runtime_profile = state.get(_custom_human_store_key("runtime_profile"), {}) or {}
    contract_preflight = _run_custom_human_dom_contract_preflight(
        adapter,
        {
            "buy": buy_info,
            "sell": sell_info,
            "flat": flat_info,
            "ticker": str(
                runtime_profile.get("ticker")
                or runtime_profile.get("contract_symbol")
                or runtime_profile.get("instrument")
                or "MYM"
            ).strip().upper() or "MYM",
        },
    )
    tradovate_snapshot = _coerce_custom_human_tradovate_snapshot(shared.get("tradovate_snapshot"))
    contract_payload = (
        dict(contract_preflight.get("contract"))
        if isinstance(contract_preflight.get("contract"), dict)
        else {}
    )
    active_module = (
        dict(contract_payload.get("active_module"))
        if isinstance(contract_payload.get("active_module"), dict)
        else {}
    )
    quantity_payload = (
        dict(contract_payload.get("quantity"))
        if isinstance(contract_payload.get("quantity"), dict)
        else {}
    )
    shared["last_dom_preflight"] = {
        "success": bool(contract_preflight.get("ok", False)),
        "error_msg": str(contract_preflight.get("message", "") or "").strip(),
        "account_match": bool((tradovate_snapshot or {}).get("account_ok", False)),
        "instrument_match": bool((tradovate_snapshot or {}).get("instrument_visible", False)),
        "module_found": bool(active_module),
        "quantity_found": bool(quantity_payload.get("found", False)),
        "quantity_value": str(quantity_payload.get("value", "") or "").strip(),
        "entry_safe": bool((contract_payload.get("entry_integrity") or {}).get("ok", contract_preflight.get("ok", False))),
        "exit_safe": bool((contract_payload.get("exit_integrity") or {}).get("ok", contract_preflight.get("ok", False))),
        "entry_status": str((contract_payload.get("entry_integrity") or {}).get("status", "unknown") or "unknown"),
        "exit_status": str((contract_payload.get("exit_integrity") or {}).get("status", "unknown") or "unknown"),
        "error_code": str(contract_preflight.get("error_code", "") or ""),
        "contract": contract_payload,
    }
    if not bool(contract_preflight.get("ok", False)):
        contract_message = str(
            contract_preflight.get("message", "Tradovate UI contract-check fejlede.")
        ).strip() or "Tradovate UI contract-check fejlede."
        _log_custom_human_runtime_event(
            shared,
            headline="AUTO CONTRACT PREFLIGHT FEJLEDE",
            detail=contract_message,
            tone="warning",
            kind="warning",
            level=logging.WARNING,
        )
        return {
            "ok": False,
            "status": str(contract_preflight.get("status", "contract_failed") or "contract_failed"),
            "message": contract_message,
            "buy": buy_info,
            "sell": sell_info,
            "flat": flat_info,
            "contract": contract_preflight.get("contract"),
        }

    shared["buy_info"] = buy_info
    shared["sell_info"] = sell_info
    shared["flat_info"] = flat_info
    shared["ui_contract"] = contract_preflight.get("contract")
    _log_custom_human_runtime_event(
        shared,
        headline="AUTO TARGETS KLAR",
        detail=(
            f"Mode={str(target_cfg.get('mode', 'auto') or 'auto')} | "
            f"Buy={str((buy_info or {}).get('selector', '')) or 'N/A'} | "
            f"Sell={str((sell_info or {}).get('selector', '')) or 'N/A'} | "
            f"Flat={str((flat_info or {}).get('selector', '')) or 'N/A'}"
        ),
        tone="active",
        kind="system",
        level=logging.INFO,
    )
    return {
        "ok": True,
        "status": "ok",
        "message": str(contract_preflight.get("message", "") or "").strip(),
        "buy": buy_info,
        "sell": sell_info,
        "flat": flat_info,
        "contract": contract_preflight.get("contract"),
        "mode": str(target_cfg.get("mode", "auto") or "auto"),
    }

def _capture_startup_snapshot(
    label: str,
    exc: BaseException | None = None,
    *,
    shared: dict[str, Any] | None = None,
) -> None:
    lines: list[str] = [f"[STARTUP SNAPSHOT] {label}"]
    if exc is not None:
        lines.append(f"exception={type(exc).__name__}: {exc}")
    if isinstance(shared, dict):
        lines.append(
            "shared_keys="
            + ",".join(
                sorted(
                    key
                    for key in shared.keys()
                    if key
                    not in {
                        "adapter",
                        "lock",
                        "signal_queue",
                        "stop_event",
                        "supervisor_stop_event",
                        "live_observer_cfg",
                    }
                )
            )
        )
        for key in (
            "running",
            "auto_requested",
            "connected",
            "runtime_active",
            "runtime_state_loaded",
            "last_stop_reason",
        ):
            lines.append(f"{key}={shared.get(key)!r}")
    _APP_LOGGER.error(" | ".join(lines))


CUSTOM_FUTURES_MYMPV = 0.5  # $ per index point for MYM


CUSTOM_FUTURES_YAHOO_SYMBOLS = ("MYM=F", "YM=F")


CUSTOM_FUTURES_RANGE_KEY = "1000d"


CUSTOM_HUMAN_MAX_ADDS = 10


CUSTOM_HUMAN_BROKER_CONFIRMATION_SNAPSHOTS = 2


CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS = 10.0


CUSTOM_HUMAN_EXIT_CONFIRMATION_TIMEOUT_SECONDS = 5.0


CUSTOM_HUMAN_LIVE_RANGE_KEY = "5d"


CUSTOM_HUMAN_LIVE_FETCH_MIN_INTERVAL_SECONDS = 12.0


CUSTOM_HUMAN_LIVE_DUKASCOPY_REFRESH_AFTER_SECONDS = 6 * 60


CUSTOM_HUMAN_LIVE_STALE_SECONDS = 20 * 60





DUKASCOPY_AUTO_BARS: int | None = None


def _load_live_yahoo_data(
    *,
    symbol: str,
    instrument: str,
    range_key: str,
    include_5m: bool,
    include_15m: bool,
) -> pd.DataFrame:
    timeframes: list[str] = []
    if include_5m:
        timeframes.append("5m")
    if include_15m:
        timeframes.append("15m")
    if not timeframes:
        timeframes = ["15m"]

    return load_yahoo_intraday_data(
        symbol=symbol.strip(),
        instrument_label=instrument,
        range_key=range_key,
        timeframes=timeframes,
    )


def _load_custom_futures_mym_intraday(
    *,
    range_key: str = CUSTOM_FUTURES_RANGE_KEY,
    include_5m: bool = True,
    include_15m: bool = True,
) -> tuple[pd.DataFrame, str]:
    errors: list[str] = []
    for symbol in CUSTOM_FUTURES_YAHOO_SYMBOLS:
        try:
            data = _load_live_yahoo_data(
                symbol=symbol,
                instrument="DOW",
                range_key=range_key,
                include_5m=include_5m,
                include_15m=include_15m,
            )
            if isinstance(data, pd.DataFrame) and not data.empty:
                return data, symbol
            errors.append(f"{symbol}: empty dataset")
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
    detail = " | ".join(errors) if errors else "unknown error"
    raise YahooApiError(f"Failed to load MYM futures data. {detail}")


def _filter_instrument_market_data(
    intraday_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    *,
    instrument: str = "DOW",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    intraday = intraday_df.copy()
    daily = daily_df.copy()
    if "instrument" in intraday.columns:
        intraday = intraday[intraday["instrument"].astype(str) == str(instrument)].copy()
    if "instrument" in daily.columns:
        daily = daily[daily["instrument"].astype(str) == str(instrument)].copy()
    return intraday, daily


def _load_live_dukascopy_data(
    *,
    symbol: str,
    instrument: str,
    range_key: str,
    include_5m: bool,
    include_15m: bool,
) -> pd.DataFrame:
    timeframes: list[str] = []
    if include_5m:
        timeframes.append("5m")
    if include_15m:
        timeframes.append("15m")
    if not timeframes:
        timeframes = ["15m"]

    return load_dukascopy_intraday_data(
        symbol=symbol.strip().lower(),
        instrument_label=instrument,
        range_key=range_key,
        timeframes=timeframes,
        max_bars_per_timeframe=DUKASCOPY_AUTO_BARS,
    )


def _load_live_dukascopy_cached_data(
    *,
    symbol: str,
    instrument: str,
    range_key: str,
    include_5m: bool,
    include_15m: bool,
) -> pd.DataFrame:
    timeframes: list[str] = []
    if include_5m:
        timeframes.append("5m")
    if include_15m:
        timeframes.append("15m")
    if not timeframes:
        timeframes = ["15m"]

    return load_dukascopy_cached_intraday_data(
        symbol=symbol.strip().lower(),
        instrument_label=instrument,
        range_key=range_key,
        timeframes=timeframes,
        max_bars_per_timeframe=DUKASCOPY_AUTO_BARS,
    )


def _dukascopy_symbol_for_instrument(instrument: str) -> str:
    return str(DUKASCOPY_SYMBOL_MAP.get(str(instrument).strip(), "")).strip().lower()



# Fix 2: Module-level lock for thread-safe signal-queue initialisation.
# _build_cdp_auto_trade_shared_state() pre-creates the queue, but
# _ensure_cdp_signal_queue is also callable independently and two concurrent
# callers could race to replace it with different Queue() instances, silently
# dropping any signals queued to the "lost" object.
_CDP_SIGNAL_QUEUE_LOCK: threading.Lock = threading.Lock()


def _ensure_cdp_signal_queue() -> queue.Queue[dict[str, Any]]:
    signal_queue = _cdp_auto_trade_shared.get("signal_queue")
    if isinstance(signal_queue, queue.Queue):
        return signal_queue
    with _CDP_SIGNAL_QUEUE_LOCK:
        # Double-checked locking — re-read after acquiring.
        signal_queue = _cdp_auto_trade_shared.get("signal_queue")
        if not isinstance(signal_queue, queue.Queue):
            signal_queue = queue.Queue()
            _cdp_auto_trade_shared["signal_queue"] = signal_queue
    return signal_queue


def _reset_cdp_signal_queue() -> queue.Queue[dict[str, Any]]:
    signal_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    _cdp_auto_trade_shared["signal_queue"] = signal_queue
    return signal_queue


def _queue_cdp_signal_from_custom_human(signal: str | dict[str, Any]) -> tuple[str, str]:
    signal_payload: dict[str, Any]
    if isinstance(signal, dict):
        signal_payload = dict(signal)
        action_token = str(signal_payload.get("action", "")).strip().lower()
        token = str(signal_payload.get("signal", "")).strip().upper()
    else:
        token = str(signal).strip().upper()
        signal_payload = {"signal": token}
        action_token = ""

    if action_token == "exit":
        token = "FLAT"

    if token not in {"BUY", "SELL", "FLAT"}:
        return "ignored", f"Unsupported signal `{token}`."

    signal_payload["signal"] = token
    if _cdp_auto_trade_shared.get("running"):
        signal_queue = _ensure_cdp_signal_queue()
        signal_queue.put(signal_payload)
        return "queued_to_cdp", f"{token} queued to CDP worker (fifo depth {signal_queue.qsize()})."

    return "cdp_not_running", "CDP auto loop not running (start auto first)."


def _custom_human_signal_strategy_name(signal_item: dict[str, Any]) -> str:
    base_name = str(signal_item.get("strategy_name", "Custom Human")).strip() or "Custom Human"
    event = str(signal_item.get("event", "entry")).strip().lower()
    if event == "add":
        return f"{base_name}::add_{int(signal_item.get('add_index', 0))}"
    if event == "exit":
        return f"{base_name}::exit"
    return f"{base_name}::entry"


def _build_custom_human_router_payload(
    *,
    signal_item: dict[str, Any],
    runtime_profile: dict[str, Any],
) -> dict[str, Any]:
    action = str(signal_item.get("action", "")).strip().lower()
    if action not in {"buy", "sell", "add", "exit"}:
        raise ValueError(f"Unsupported Custom Human action: {action}")

    ticker = str(signal_item.get("instrument") or runtime_profile.get("ticker") or "MYM").strip().upper() or "MYM"
    signal_price = _safe_float(signal_item.get("signal_price"))
    if signal_price is None or signal_price <= 0:
        raise ValueError("Custom Human signal_price must be positive")

    extras = {
        "source": "custom_human_cdp",
        "strategy": str(signal_item.get("strategy_name", "Custom Human")),
        "tradeDate": str(signal_item.get("trade_date", "")),
        "eventType": str(signal_item.get("event", action)),
        "propFirm": str(runtime_profile.get("prop_firm", "")),
        "accountSize": str(runtime_profile.get("account_size", "")),
        "riskProfile": str(runtime_profile.get("risk_profile", "")),
        "riskUsd": _safe_float(runtime_profile.get("risk_usd")),
        "signalToken": str(signal_item.get("signal", "")),
    }
    if signal_item.get("add_index") is not None:
        extras["addIndex"] = int(signal_item.get("add_index", 0))
    if signal_item.get("add_trigger_r") is not None:
        extras["addTriggerR"] = float(signal_item.get("add_trigger_r"))

    stop_loss_amount = _safe_float(signal_item.get("stop_loss_amount"))
    quantity = _safe_float(signal_item.get("quantity"))
    return build_aggressive_action_payload(
        action=action,
        ticker=ticker,
        signal_price=float(signal_price),
        quantity=quantity if quantity is not None else 1.0,
        quantity_type="fixed_quantity",
        stop_loss_amount=stop_loss_amount,
        signal_id=str(signal_item.get("signal_id", "")),
        position_key=str(signal_item.get("position_key", "")),
        interval=str(signal_item.get("interval", "15")),
        source="Final Fantasy School Run",
        extras=extras,
    )


def _build_custom_human_trade_intent(
    *,
    signal_item: dict[str, Any],
    runtime_profile: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    strategy_name = _custom_human_signal_strategy_name(signal_item)
    instrument = str(signal_item.get("instrument") or runtime_profile.get("ticker") or "MYM").strip().upper() or "MYM"
    direction = str(signal_item.get("direction", "long")).strip().lower() or "long"
    trade_date = str(signal_item.get("trade_date", now.date().isoformat())).strip() or now.date().isoformat()
    signal_price = _safe_float(signal_item.get("signal_price"))
    entry_price = _safe_float(signal_item.get("entry_price"))
    stop_price = _safe_float(signal_item.get("stop_price"))
    risk_pts = _safe_float(signal_item.get("risk_pts")) or 0.0
    quantity = float(_safe_float(signal_item.get("quantity")) or 1.0)
    tick_value = float(_safe_float(signal_item.get("tick_value")) or CUSTOM_FUTURES_MYMPV)
    # Embed event type as suffix so Add orders get a distinct idempotency key from Entry orders.
    # Also embed bar_index (when present) so two valid entry signals on *different* bars of the
    # same trading day produce distinct keys and are not blocked as duplicates by the risk gate.
    _event_raw = str(signal_item.get("event", "entry") or "entry").strip().lower()
    _add_index = int(signal_item.get("add_index", 0) or 0)
    _intent_suffix = f"add_{_add_index}" if _event_raw == "add" else _event_raw
    _bar_idx_raw = signal_item.get("entry_bar_index")
    _bar_idx = int(_bar_idx_raw) if _bar_idx_raw is not None else None
    _idem_raw = f"{strategy_name}|{instrument}|{direction}|{trade_date}|{_intent_suffix}"
    if _bar_idx is not None:
        _idem_raw = f"{_idem_raw}|{_bar_idx}"
    idempotency_key = hashlib.sha256(_idem_raw.encode("utf-8")).hexdigest()
    router_payload = _build_custom_human_router_payload(signal_item=signal_item, runtime_profile=runtime_profile)
    return {
        "strategy_name": strategy_name,
        "instrument": instrument,
        "direction": direction,
        "entry_price": signal_price if signal_price is not None else entry_price,
        "stop_price": stop_price,
        "risk_pts": float(risk_pts),
        "quantity": float(quantity),
        "risk_amount": float(quantity) * float(risk_pts) * float(tick_value),
        "risk_pct": None,
        "tick_value": float(tick_value),
        "idempotency_key": idempotency_key,
        "order_type": _intent_suffix,
        "entry_bar_index": _bar_idx,
        "trade_date": trade_date,
        "created_at": now.isoformat(),
        "context_snapshot": {
            "signal_id": str(signal_item.get("signal_id", "")),
            "signal": str(signal_item.get("signal", "")),
            "event": str(signal_item.get("event", "")),
            "position_key": str(signal_item.get("position_key", "")),
            "entry_bar_index": _bar_idx,
            "prop_firm": str(runtime_profile.get("prop_firm", "")),
            "account_size": str(runtime_profile.get("account_size", "")),
            "risk_profile": str(runtime_profile.get("risk_profile", "")),
        },
        "metadata": {
            "router_payload": router_payload,
            "cdp_signal": dict(signal_item),
        },
    }


def _custom_human_dispatch_row(
    *,
    signal_item: dict[str, Any],
    status: str,
    message: str,
    retries: int = 0,
) -> dict[str, Any]:
    return {
        "action": str(signal_item.get("action", signal_item.get("signal", ""))).lower(),
        "status": str(status),
        "http": None,
        "qty": _safe_float(signal_item.get("quantity")),
        "retries": int(retries),
        "signal_id": str(signal_item.get("signal_id", ""))[:12],
        "position_key": str(signal_item.get("position_key", ""))[:40],
        "message": str(message),
    }


class _CustomHumanQueueBrokerAdapter(BrokerAdapter):
    def __init__(
        self,
        *,
        router: TradingSignalRouter,
        kill_switch: bool,
        max_positions_per_strategy: int,
    ) -> None:
        self.router = router
        self.kill_switch = bool(kill_switch)
        self.max_positions_per_strategy = max(1, int(max_positions_per_strategy))

    async def connect(self) -> None:
        return None

    async def place_order(self, request: OrderRequest) -> OrderResult:
        metadata = dict(request.metadata) if isinstance(request.metadata, dict) else {}
        router_payload = metadata.get("router_payload")
        cdp_signal = metadata.get("cdp_signal")
        if not isinstance(router_payload, dict) or not isinstance(cdp_signal, dict):
            return OrderResult(
                order_id=request.idempotency_key,
                status=OrderStatus.ERROR,
                instrument=request.instrument,
                side=request.side,
                quantity=float(request.quantity),
                fill_price=None,
                error_message="Custom Human metadata missing router_payload/cdp_signal.",
                raw_response={"dispatch_status": "invalid_metadata"},
            )

        if not bool(_cdp_auto_trade_shared.get("running")):
            return OrderResult(
                order_id=request.idempotency_key,
                status=OrderStatus.ERROR,
                instrument=request.instrument,
                side=request.side,
                quantity=float(request.quantity),
                fill_price=None,
                error_message="CDP auto loop not running.",
                raw_response={"dispatch_status": "cdp_not_running"},
            )

        dispatch_result = self.router.dispatch_local(
            router_payload,
            kill_switch=self.kill_switch,
            max_positions_per_strategy=self.max_positions_per_strategy,
        )
        recovery_retry_used = False
        recovery_retry_signal_id = ""
        if (
            dispatch_result.status == "duplicate"
            and _custom_human_should_retry_router_duplicate(_cdp_auto_trade_shared, cdp_signal)
        ):
            recovery_retry_used = True
            recovery_retry_signal_id = _custom_human_duplicate_retry_signal_id(
                str(cdp_signal.get("signal_id", "") or dispatch_result.signal_id)
            )
            retry_signal = dict(cdp_signal)
            retry_signal["signal_id"] = recovery_retry_signal_id
            retry_payload = dict(router_payload)
            retry_extras = dict(retry_payload.get("extras", {})) if isinstance(retry_payload.get("extras"), dict) else {}
            retry_extras["signalId"] = recovery_retry_signal_id
            retry_payload["extras"] = retry_extras
            dispatch_result = self.router.dispatch_local(
                retry_payload,
                kill_switch=self.kill_switch,
                max_positions_per_strategy=self.max_positions_per_strategy,
            )
            if dispatch_result.status == "queued_local_cdp":
                cdp_signal = retry_signal
        raw_response = {
            "dispatch_status": str(dispatch_result.status),
            "dispatch_message": str(dispatch_result.message),
            "position_key": str(dispatch_result.position_key or ""),
        }
        if recovery_retry_used:
            raw_response["recovery_retry_signal_id"] = recovery_retry_signal_id
        if dispatch_result.status != "queued_local_cdp":
            return OrderResult(
                order_id=dispatch_result.signal_id or request.idempotency_key,
                status=OrderStatus.REJECTED,
                instrument=request.instrument,
                side=request.side,
                quantity=float(request.quantity),
                fill_price=None,
                error_message=str(dispatch_result.message),
                raw_response=raw_response,
            )

        queue_status, queue_message = _queue_cdp_signal_from_custom_human(cdp_signal)
        raw_response["queue_status"] = str(queue_status)
        raw_response["queue_message"] = str(queue_message)
        if queue_status != "queued_to_cdp":
            return OrderResult(
                order_id=dispatch_result.signal_id or request.idempotency_key,
                status=OrderStatus.ERROR,
                instrument=request.instrument,
                side=request.side,
                quantity=float(request.quantity),
                fill_price=None,
                error_message=str(queue_message),
                raw_response=raw_response,
            )

        return OrderResult(
            order_id=dispatch_result.signal_id or request.idempotency_key,
            status=OrderStatus.PENDING,
            instrument=request.instrument,
            side=request.side,
            quantity=float(request.quantity),
            fill_price=None,
            error_message=None,
            raw_response=raw_response,
        )

    async def get_position(self, instrument: str) -> dict[str, Any] | None:
        return None

    async def get_account_balance(self) -> float:
        return 0.0

    async def cancel_order(self, order_id: str) -> bool:
        return False

    async def disconnect(self) -> None:
        return None


def _process_custom_human_signal_via_engine(
    *,
    signal_item: dict[str, Any],
    runtime_profile: dict[str, Any],
    pipeline: ExecutionPipeline,
    router: TradingSignalRouter,
    live_cfg: dict[str, Any],
    kill_switch: bool,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    action = str(signal_item.get("action", "")).strip().lower()
    if action not in {"buy", "sell", "add", "exit"}:
        return _custom_human_dispatch_row(
            signal_item=signal_item,
            status="ignored",
            message=f"Unsupported Custom Human action `{action}`.",
        ), None

    if not bool(_cdp_auto_trade_shared.get("running")):
        return _custom_human_dispatch_row(
            signal_item=signal_item,
            status="cdp_not_running",
            message="CDP auto loop not running (start auto first).",
        ), None

    adapter = _CustomHumanQueueBrokerAdapter(
        router=router,
        kill_switch=bool(kill_switch),
        max_positions_per_strategy=int(live_cfg.get("max_positions_per_strategy", 1)),
    )
    intent = _build_custom_human_trade_intent(
        signal_item=signal_item,
        runtime_profile=runtime_profile,
        now=now,
    )

    if action in {"buy", "sell", "add"} and bool(kill_switch):
        decision = GateDecision(
            approved=False,
            reason="UI kill switch active.",
            checks_passed=[],
            checks_failed=["kill_switch"],
            state_snapshot=_custom_human_gate_state_snapshot(pipeline),
        )
        pipeline.audit_db.log_intent(intent)
        pipeline.audit_db.log_gate_decision(intent["idempotency_key"], decision)
        return _custom_human_dispatch_row(
            signal_item=signal_item,
            status="blocked_kill_switch",
            message=decision.reason,
        ), {
            "intent": intent,
            "decision": decision,
            "order_result": None,
        }

    max_consecutive_losses = int(max(0, int(live_cfg.get("max_consecutive_losses", 0))))
    if action in {"buy", "sell", "add"} and max_consecutive_losses > 0:
        gate_state = pipeline.risk_gate.state
        if int(gate_state.consecutive_losses) >= max_consecutive_losses:
            decision = GateDecision(
                approved=False,
                reason=(
                    "Max consecutive losses reached: "
                    f"{int(gate_state.consecutive_losses)} >= {max_consecutive_losses}"
                ),
                checks_passed=[],
                checks_failed=["max_consecutive_losses"],
                state_snapshot=_custom_human_gate_state_snapshot(pipeline),
            )
            pipeline.audit_db.log_intent(intent)
            pipeline.audit_db.log_gate_decision(intent["idempotency_key"], decision)
            return _custom_human_dispatch_row(
                signal_item=signal_item,
                status="blocked_risk_gate",
                message=decision.reason,
            ), {
                "intent": intent,
                "decision": decision,
                "order_result": None,
            }

    if action == "exit":
        decision = GateDecision(
            approved=True,
            reason="Exit signal routed via local broker adapter to flatten safely.",
            checks_passed=["exit_passthrough"],
            checks_failed=[],
            state_snapshot=_custom_human_gate_state_snapshot(pipeline),
        )
        order_side = OrderSide.SELL if str(signal_item.get("signal", "")).upper() == "SELL" else OrderSide.BUY
        order_request = OrderRequest(
            instrument=str(intent.get("instrument", "")),
            side=order_side,
            order_type=OrderType.MARKET,
            quantity=float(intent.get("quantity", 0.0)),
            entry_price=intent.get("entry_price"),
            stop_price=intent.get("stop_price"),
            idempotency_key=str(intent.get("idempotency_key", "")),
            strategy_name=str(intent.get("strategy_name", "")),
            metadata=dict(intent.get("metadata", {})),
        )
        pipeline.audit_db.log_intent(intent)
        pipeline.audit_db.log_gate_decision(intent["idempotency_key"], decision)
        order_result = _run_async_task(lambda: adapter.place_order(order_request))
        pipeline.audit_db.log_broker_order(intent["idempotency_key"], order_result, broker=type(adapter).__name__)
        raw_response = getattr(order_result, "raw_response", {}) or {}
        order_status = getattr(order_result, "status", None)
        row_status = "queued_to_cdp" if order_status == OrderStatus.PENDING else str(raw_response.get("dispatch_status", "broker_error"))
        row_message = str(raw_response.get("queue_message") or raw_response.get("dispatch_message") or getattr(order_result, "error_message", "") or row_status)
        return _custom_human_dispatch_row(
            signal_item=signal_item,
            status=row_status,
            message=row_message,
        ), {
            "intent": intent,
            "decision": decision,
            "order_result": order_result,
        }

    original_broker = pipeline.broker
    try:
        pipeline.broker = adapter
        result = pipeline.process_intent(intent, now=now)
    finally:
        pipeline.broker = original_broker

    decision = result.get("decision")
    order_result = result.get("order_result")
    if decision is None:
        row_status = "blocked_risk_gate"
        row_message = "Gate decision missing."
    elif not bool(getattr(decision, "approved", False)):
        row_status = "blocked_risk_gate"
        row_message = str(getattr(decision, "reason", "Risk gate rejected signal."))
    elif order_result is None:
        row_status = "broker_missing"
        row_message = "Custom Human broker adapter not available."
    else:
        raw_response = getattr(order_result, "raw_response", {}) or {}
        order_status = getattr(order_result, "status", None)
        if order_status == OrderStatus.PENDING and str(raw_response.get("queue_status", "")) == "queued_to_cdp":
            row_status = "queued_to_cdp"
        else:
            row_status = str(raw_response.get("dispatch_status") or raw_response.get("queue_status") or order_status or "broker_error")
        row_message = str(
            raw_response.get("queue_message")
            or raw_response.get("dispatch_message")
            or getattr(order_result, "error_message", "")
            or row_status
        )

    if (
        action in {"buy", "sell", "add"}
        and decision is not None
        and bool(getattr(decision, "approved", False))
        and row_status != "queued_to_cdp"
    ):
        risk_gate = getattr(pipeline, "risk_gate", None)
        if risk_gate is not None and hasattr(risk_gate, "release_idempotency_key"):
            try:
                _release_bar_index = signal_item.get("entry_bar_index")
                risk_gate.release_idempotency_key(
                    strategy=str(intent.get("strategy_name", "")),
                    instrument=str(intent.get("instrument", "")),
                    direction=str(intent.get("direction", "")),
                    trade_date=str(intent.get("trade_date", "")),
                    suffix=str(intent.get("order_type", "entry") or "entry"),
                    bar_index=int(_release_bar_index) if _release_bar_index is not None else None,
                )
            except Exception as exc:
                _APP_LOGGER.warning("Kunne ikke release idempotency key (non-critical): %s", exc)

    row_status, row_message = _classify_custom_human_restart_suppression(
        _cdp_auto_trade_shared,
        row_status=row_status,
        row_message=row_message,
    )

    return _custom_human_dispatch_row(
        signal_item=signal_item,
        status=row_status,
        message=row_message,
    ), result


def _custom_human_session_close_dk(instrument: str, trade_date: Any | None = None) -> time:
    token = str(instrument or "").strip().upper()
    if token in {"DOW", "MYM", "MES", "MNQ", "M2K", "MGC", "MCL"}:
        trade_ts = pd.to_datetime(trade_date, errors="coerce")
        if pd.isna(trade_ts):
            trade_day = datetime.now(tz=APP_TIMEZONE).date()
        else:
            trade_day = pd.Timestamp(trade_ts).date()
        chicago_tz = ZoneInfo("America/Chicago")
        close_dt_chicago = datetime.combine(trade_day, time(15, 0), tzinfo=chicago_tz)
        close_dt_dk = close_dt_chicago.astimezone(APP_TIMEZONE)
        return close_dt_dk.timetz().replace(tzinfo=None)
    return time(16, 30)


def _custom_human_execution_model_id(label: str) -> str:
    token = str(label or "").strip().lower()
    if token in {"aggressiv", "aggressive", "tom_aggressive"}:
        return EXECUTION_MODEL_TOM_AGGRESSIVE
    if token in {"dynamisk", "live", "tom_live"}:
        return EXECUTION_MODEL_TOM_LIVE
    return EXECUTION_MODEL_SIMPLIFIED


def _custom_human_live_feed_priority(source: str) -> int:
    token = str(source or "").strip().lower()
    if token == "tradovate observer":
        return 4
    if token == "yahoo finance":
        return 3
    if token == "dukascopy live":
        return 2
    if token == "dukascopy cache":
        return 1
    return 0


def _custom_human_live_feed_sort_key(candidate: tuple[pd.DataFrame, dict[str, Any]]) -> tuple[pd.Timestamp, int]:
    _bars, meta = candidate
    ts = pd.to_datetime((meta or {}).get("latest_source_timestamp"), errors="coerce")
    if pd.isna(ts):
        ts = pd.Timestamp("1970-01-01", tz=APP_TIMEZONE)
    elif ts.tzinfo is None:
        ts = ts.tz_localize(APP_TIMEZONE)
    else:
        ts = ts.tz_convert(APP_TIMEZONE)
    return ts, _custom_human_live_feed_priority(str((meta or {}).get("source", "")))


def _custom_human_live_feed_age_seconds(meta: dict[str, Any], observed_at: datetime) -> float | None:
    latest_source_ts = pd.to_datetime((meta or {}).get("latest_source_timestamp"), errors="coerce")
    if pd.isna(latest_source_ts):
        return None
    market_ts = pd.Timestamp(latest_source_ts)
    if market_ts.tzinfo is None:
        market_ts = market_ts.tz_localize(APP_TIMEZONE)
    else:
        market_ts = market_ts.tz_convert(APP_TIMEZONE)
    return max(0.0, (observed_at - market_ts.to_pydatetime()).total_seconds())


def _build_signal_router_runtime(
    *,
    repo_root: Path,
    live_cfg: dict[str, Any],
    webhook_url: str,
) -> TradingSignalRouter:
    return TradingSignalRouter(
        db_path=repo_root / "output" / "signal_router.sqlite3",
        webhook_url=webhook_url,
        rate_limit_per_min=int(live_cfg.get("webhook_rate_limit_per_min", 60)),
        rate_limit_per_hour=int(live_cfg.get("webhook_rate_limit_per_hour", 500)),
        request_timeout_sec=float(live_cfg.get("webhook_timeout_sec", 10.0)),
        max_retries=int(live_cfg.get("webhook_max_retries", 3)),
        backoff_base_sec=float(live_cfg.get("webhook_backoff_base_sec", 0.5)),
    )


def _prepare_custom_human_live_candidate(
    *,
    raw: pd.DataFrame,
    cfg: CustomStrategyConfig,
    timezone_name: str,
    overnight_start_dk: time,
    overnight_end_dk: time,
    source_label: str,
    symbol_label: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    intraday_prepared, daily_prepared = prepare_intraday_data(
        raw,
        timezone_name=timezone_name,
        overnight_start_dk=overnight_start_dk,
        overnight_end_dk=overnight_end_dk,
    )
    intraday_prepared, daily_prepared = _filter_instrument_market_data(
        intraday_prepared,
        daily_prepared,
        instrument="DOW",
    )
    if intraday_prepared.empty or daily_prepared.empty:
        raise ValueError(f"{source_label}: live dataset er tomt efter preprocessing.")

    intraday_source = intraday_prepared.copy()
    if (intraday_source["timeframe"].astype(str) == "5m").any():
        intraday_source = intraday_source[intraday_source["timeframe"].astype(str) == "5m"].copy()
    intraday_source["trade_date"] = pd.to_datetime(intraday_source["trade_date"], errors="coerce").dt.normalize()
    latest_trade_date = pd.to_datetime(intraday_source["trade_date"], errors="coerce").dropna().max()
    if pd.isna(latest_trade_date):
        raise ValueError(f"{source_label}: kunne ikke udlede live trade_date.")

    day_bars = _build_15m_bars(
        intraday_df=intraday_source,
        cfg=cfg,
        trade_dates={pd.Timestamp(latest_trade_date).normalize()},
    )
    if day_bars.empty:
        raise ValueError(f"{source_label}: kunne ikke bygge 15m live-bars.")
    day_bars = day_bars[
        pd.to_datetime(day_bars["trade_date"], errors="coerce").dt.normalize()
        == pd.Timestamp(latest_trade_date).normalize()
    ].copy()
    day_bars = day_bars.sort_values("timestamp_dk").reset_index(drop=True)
    if day_bars.empty:
        raise ValueError(f"{source_label}: live 15m bars blev tomme efter day-filter.")

    latest_source_ts = pd.to_datetime(intraday_source["timestamp_dk"], errors="coerce").dropna().max()
    latest_bar_ts = pd.to_datetime(day_bars["timestamp_dk"], errors="coerce").dropna().max()
    return day_bars, {
        "source": str(source_label),
        "symbol": str(symbol_label),
        "trade_date": pd.Timestamp(latest_trade_date).strftime("%Y-%m-%d"),
        "latest_source_timestamp": latest_source_ts.isoformat() if pd.notna(latest_source_ts) else "",
        "latest_bar_timestamp": latest_bar_ts.isoformat() if pd.notna(latest_bar_ts) else "",
        "bar_count": int(len(day_bars)),
    }


def _load_custom_human_live_candidate_from_yahoo(
    *,
    cfg: CustomStrategyConfig,
    timezone_name: str,
    overnight_start_dk: time,
    overnight_end_dk: time,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    raw, used_symbol = _load_custom_futures_mym_intraday(
        range_key=CUSTOM_HUMAN_LIVE_RANGE_KEY,
        include_5m=True,
        include_15m=True,
    )
    return _prepare_custom_human_live_candidate(
        raw=raw,
        cfg=cfg,
        timezone_name=timezone_name,
        overnight_start_dk=overnight_start_dk,
        overnight_end_dk=overnight_end_dk,
        source_label="Yahoo Finance",
        symbol_label=used_symbol,
    )


def _load_custom_human_live_candidate_from_dukascopy(
    *,
    cfg: CustomStrategyConfig,
    timezone_name: str,
    overnight_start_dk: time,
    overnight_end_dk: time,
    cached_only: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    symbol = _dukascopy_symbol_for_instrument("DOW")
    if not symbol:
        raise ValueError("Ingen Dukascopy symbol mapping for DOW.")
    loader = _load_live_dukascopy_cached_data if cached_only else _load_live_dukascopy_data
    raw = loader(
        symbol=symbol,
        instrument="DOW",
        range_key=CUSTOM_HUMAN_LIVE_RANGE_KEY,
        include_5m=True,
        include_15m=True,
    )
    return _prepare_custom_human_live_candidate(
        raw=raw,
        cfg=cfg,
        timezone_name=timezone_name,
        overnight_start_dk=overnight_start_dk,
        overnight_end_dk=overnight_end_dk,
        source_label="Dukascopy Cache" if cached_only else "Dukascopy Live",
        symbol_label="MYM (DOW proxy)",
    )


def _load_custom_human_live_candidate_from_tradovate(
    *,
    shared: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    bars = _coerce_custom_human_tradovate_bars(shared.get("tradovate_15m_bars"))
    if not bars:
        raise ValueError("Ingen Tradovate 15m bars endnu.")

    frame = pd.DataFrame(bars)
    frame["timestamp_dk"] = pd.to_datetime(frame["timestamp_dk"], errors="coerce")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.normalize()
    frame = frame.dropna(subset=["timestamp_dk", "trade_date"]).sort_values("timestamp_dk").reset_index(drop=True)
    if frame.empty:
        raise ValueError("Tradovate 15m bars kunne ikke normaliseres.")

    latest_trade_date = pd.to_datetime(frame["trade_date"], errors="coerce").dropna().max()
    if pd.isna(latest_trade_date):
        raise ValueError("Tradovate 15m bars mangler gyldig trade_date.")
    frame = frame[
        pd.to_datetime(frame["trade_date"], errors="coerce").dt.normalize()
        == pd.Timestamp(latest_trade_date).normalize()
    ].copy()
    frame = frame.sort_values("timestamp_dk").reset_index(drop=True)
    if frame.empty:
        raise ValueError("Tradovate 15m bars blev tomme efter day-filter.")

    frame["instrument"] = "DOW"
    frame["timeframe"] = "15m"
    frame["timestamp"] = frame["timestamp_dk"]

    samples = _coerce_custom_human_tradovate_price_samples(shared.get("tradovate_price_samples"))
    latest_source_ts = None
    if samples:
        latest_source_ts = pd.to_datetime(samples[-1].get("observed_at"), errors="coerce")
    if pd.isna(latest_source_ts):
        snapshot = _coerce_custom_human_tradovate_snapshot(shared.get("tradovate_snapshot"))
        latest_source_ts = pd.to_datetime((snapshot or {}).get("observed_at"), errors="coerce")
    latest_bar_ts = pd.to_datetime(frame["timestamp_dk"], errors="coerce").dropna().max()
    instrument_match = str(frame["instrument_match"].iloc[-1] if "instrument_match" in frame.columns else "") or "MYM"

    return frame, {
        "source": "Tradovate Observer",
        "symbol": instrument_match,
        "trade_date": pd.Timestamp(latest_trade_date).strftime("%Y-%m-%d"),
        "latest_source_timestamp": latest_source_ts.isoformat() if pd.notna(latest_source_ts) else "",
        "latest_bar_timestamp": latest_bar_ts.isoformat() if pd.notna(latest_bar_ts) else "",
        "bar_count": int(len(frame)),
    }


def _load_custom_human_live_day_bars(
    *,
    cfg: CustomStrategyConfig,
    timezone_name: str,
    overnight_start_dk: time,
    overnight_end_dk: time,
    observed_at: datetime | None = None,
    shared: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    observed = observed_at or datetime.now(tz=APP_TIMEZONE)
    if shared is not None:
        cached = shared.get("live_feed_cache")
        fetched_at_raw = str(shared.get("live_feed_cache_fetched_at", "") or "")
        if isinstance(cached, dict):
            cached_bars = cached.get("day_bars")
            cached_meta = cached.get("meta")
            fetched_at = pd.to_datetime(fetched_at_raw, errors="coerce")
            if (
                isinstance(cached_bars, pd.DataFrame)
                and isinstance(cached_meta, dict)
                and pd.notna(fetched_at)
            ):
                fetched_ts = pd.Timestamp(fetched_at)
                if fetched_ts.tzinfo is None:
                    fetched_ts = fetched_ts.tz_localize(APP_TIMEZONE)
                else:
                    fetched_ts = fetched_ts.tz_convert(APP_TIMEZONE)
                cache_age = max(0.0, (observed - fetched_ts.to_pydatetime()).total_seconds())
                if (
                    cache_age < float(CUSTOM_HUMAN_LIVE_FETCH_MIN_INTERVAL_SECONDS)
                    and not _custom_human_should_bypass_live_feed_cache(shared, cached_meta)
                ):
                    cached_meta = dict(cached_meta)
                    cached_meta["cache_hit"] = True
                    return cached_bars.copy(), cached_meta

    if shared is not None:
        try:
            tradovate_bars, tradovate_meta = _load_custom_human_live_candidate_from_tradovate(shared=shared)
            tradovate_age = _custom_human_live_feed_age_seconds(tradovate_meta, observed)
            if tradovate_age is not None and tradovate_age < float(CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_STALE_SECONDS):
                tradovate_meta = dict(tradovate_meta)
                tradovate_meta["cache_hit"] = False
                shared["live_feed_cache"] = {"day_bars": tradovate_bars.copy(), "meta": dict(tradovate_meta)}
                shared["live_feed_cache_fetched_at"] = observed.isoformat()
                return tradovate_bars, tradovate_meta
        except Exception as exc:
            _APP_LOGGER.warning("Tradovate live feed fast-path fejlede: %s", exc)

    candidates: list[tuple[pd.DataFrame, dict[str, Any]]] = []
    errors: list[str] = []
    if shared is not None:
        try:
            candidates.append(_load_custom_human_live_candidate_from_tradovate(shared=shared))
        except Exception as exc:
            errors.append(f"Tradovate Observer: {exc}")
    for label, loader in (
        ("Yahoo Finance", lambda: _load_custom_human_live_candidate_from_yahoo(
            cfg=cfg,
            timezone_name=timezone_name,
            overnight_start_dk=overnight_start_dk,
            overnight_end_dk=overnight_end_dk,
        )),
        ("Dukascopy Cache", lambda: _load_custom_human_live_candidate_from_dukascopy(
            cfg=cfg,
            timezone_name=timezone_name,
            overnight_start_dk=overnight_start_dk,
            overnight_end_dk=overnight_end_dk,
            cached_only=True,
        )),
    ):
        try:
            candidates.append(loader())
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    best_candidate = max(candidates, key=_custom_human_live_feed_sort_key) if candidates else None
    best_meta = best_candidate[1] if best_candidate is not None else {}
    best_age = _custom_human_live_feed_age_seconds(best_meta, observed) if isinstance(best_meta, dict) else None
    should_try_dukascopy_live = (
        best_candidate is None
        or best_age is None
        or best_age >= float(CUSTOM_HUMAN_LIVE_DUKASCOPY_REFRESH_AFTER_SECONDS)
    )
    if should_try_dukascopy_live:
        try:
            candidates.append(
                _load_custom_human_live_candidate_from_dukascopy(
                    cfg=cfg,
                    timezone_name=timezone_name,
                    overnight_start_dk=overnight_start_dk,
                    overnight_end_dk=overnight_end_dk,
                    cached_only=False,
                )
            )
        except Exception as exc:
            errors.append(f"Dukascopy Live: {exc}")

    if not candidates:
        raise ValueError(" | ".join(errors) if errors else "Ingen live feed kilder gav data.")

    day_bars, meta = max(candidates, key=_custom_human_live_feed_sort_key)
    meta = dict(meta)
    meta["cache_hit"] = False
    if errors:
        meta["warnings"] = list(errors)

    if shared is not None:
        shared["live_feed_cache"] = {"day_bars": day_bars.copy(), "meta": dict(meta)}
        shared["live_feed_cache_fetched_at"] = observed.isoformat()

    return day_bars, meta


def _mark_custom_human_live_candidate_pending(
    *,
    state_raw: dict[str, Any] | None,
    candidate: dict[str, Any],
    signal_id: str,
) -> dict[str, Any]:
    state = _coerce_custom_human_live_state(state_raw)
    event = str(candidate.get("event", "")).strip().lower() or "signal"
    state["pending_signal_id"] = str(signal_id or "").strip()
    state["pending_event"] = event
    state["pending_candidate"] = dict(candidate)
    state["reconcile_required"] = False
    state["last_note"] = f"{event.upper()} queued – afventer broker-bekræftelse."
    return state


def _clear_custom_human_live_pending(state: dict[str, Any]) -> dict[str, Any]:
    state["pending_signal_id"] = ""
    state["pending_event"] = ""
    state["pending_candidate"] = None
    return state


def _post_flat_hard_reset(state: dict[str, Any]) -> dict[str, Any]:
    """Hard-reset all in-trade metadata after a confirmed flat.

    Called once per confirmed_flat cycle.  Clears every field that belongs to
    the *closed* trade so the observer starts the next bar-evaluation pass with
    a completely clean slate.  Must only be called when position_open is already
    False (the caller is responsible for that guard).

    Fields preserved intentionally:
    - ``strategy_name``, ``instrument``, ``trade_date`` — needed for audit /
      next-trade idempotency key construction.
    - ``start_bar``, ``max_trigger_bar`` — belong to the *session setup*, not
      to the individual trade; the observer recomputes them on the next bar anyway.
    - ``last_note`` — left as-is so the UI can display the exit message.
    """
    # ── In-trade price / risk metadata ─────────────────────────────────────────
    state["entry_price"] = None
    state["stop_price"] = None
    state["active_stop"] = None
    state["risk_pts"] = None
    state["entry_bar_index"] = None
    # ── Trade tracking counters ─────────────────────────────────────────────────
    state["add_count_sent"] = 0
    state["max_favorable_pts"] = 0.0
    state["max_adverse_pts"] = 0.0
    state["break_even_armed"] = False
    state["exit_signal_seq"] = 0
    # ── Direction — reset so stale reconcile can't re-use it ───────────────────
    state["direction"] = None
    # ── Pending / inflight ─────────────────────────────────────────────────────
    state["pending_signal_id"] = ""
    state["pending_event"] = ""
    state["pending_candidate"] = None
    state["reconcile_required"] = False
    # ── Phase transition ────────────────────────────────────────────────────────
    # Move to waiting_for_setup so the observer evaluates a fresh bar sequence.
    # The signal loop will advance to pre_entry / entry_pending as normal once
    # the next setup is confirmed.
    state["phase"] = "waiting_for_setup"
    return state


def _custom_human_router_position_key(
    shared: dict[str, Any] | None,
    *,
    fallback_position_key: str = "",
) -> str:
    token = str(fallback_position_key or "").strip()
    if token:
        return token
    if not isinstance(shared, dict):
        return ""

    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    observer_cfg = shared.get("live_observer_cfg")
    runtime_profile = dict(observer_cfg.get("runtime_profile", {})) if isinstance(observer_cfg, dict) else {}
    instrument = str(
        runtime_profile.get("contract_symbol", "")
        or runtime_profile.get("ticker", "")
        or ""
    ).strip().upper()
    strategy_name = str(runtime_profile.get("strategy_name", "") or "").strip()
    trade_date = str(live_state.get("trade_date", "") or "").strip()
    direction = str(live_state.get("direction", "") or "").strip().lower()
    if not instrument or not strategy_name or not trade_date or direction not in {"long", "short"}:
        return ""
    return f"{instrument}:{trade_date}:{strategy_name}:{direction}"


def _release_custom_human_router_cycle_for_flat_position(
    shared: dict[str, Any] | None,
    *,
    router: Any | None = None,
    position_key: str = "",
    reset_context: str = "confirmed_flat",
) -> dict[str, int]:
    token = _custom_human_router_position_key(shared, fallback_position_key=position_key)
    if not token:
        return {"signals_deleted": 0, "locks_released": 0}

    router_obj = router
    if router_obj is None and isinstance(shared, dict):
        observer_cfg = shared.get("live_observer_cfg")
        if isinstance(observer_cfg, dict):
            router_obj = observer_cfg.get("router")
    if router_obj is None or not hasattr(router_obj, "release_local_signals_for_flat_position"):
        return {"signals_deleted": 0, "locks_released": 0}

    try:
        cleanup = router_obj.release_local_signals_for_flat_position(position_key=token)
    except Exception as exc:
        _APP_LOGGER.warning(
            "Router-cleanup efter flat position fejlede for %s (%s): %s",
            token,
            reset_context,
            exc,
        )
        return {"signals_deleted": 0, "locks_released": 0}

    signals_deleted = int((cleanup or {}).get("signals_deleted", 0) or 0)
    locks_released = int((cleanup or {}).get("locks_released", 0) or 0)
    if signals_deleted or locks_released:
        if isinstance(shared, dict):
            _append_custom_human_diagnostic_event(
                shared,
                kind="observer",
                headline="ROUTER-RYDNING",
                detail=(
                    f"Broker er flat ({reset_context}) – renset {signals_deleted} lokal(e) signal(er) "
                    f"og {locks_released} position-lock(s) for {token}."
                ),
                tone="active",
            )
        _APP_LOGGER.info(
            "[GHOST] flat router-cleanup (%s) for %s: signals=%d locks=%d",
            reset_context,
            token,
            signals_deleted,
            locks_released,
        )
    return {
        "signals_deleted": signals_deleted,
        "locks_released": locks_released,
    }


def _build_custom_human_inflight_order(
    *,
    signal_item: dict[str, Any],
    status: str,
    observed_at: datetime,
    message: str = "",
    candidate: dict[str, Any] | None = None,
    position_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    token = str(signal_item.get("signal_id", "") or "").strip()
    now_iso = observed_at.isoformat()
    return {
        "signal_id": token,
        "signal": str(signal_item.get("signal", "") or "").strip().upper(),
        "event": str(signal_item.get("event", "") or "").strip().lower(),
        "action": str(signal_item.get("action", "") or "").strip().lower(),
        "position_key": str(signal_item.get("position_key", "") or "").strip(),
        "instrument": str(signal_item.get("instrument", "") or "").strip().upper(),
        "quantity": float(_safe_float(signal_item.get("quantity")) or 0.0),
        "status": str(status or "reserved").strip().lower(),
        "reserved_at": now_iso,
        "queued_at": now_iso if str(status).strip().lower() == "queued" else "",
        "clicked_at": now_iso if str(status).strip().lower() == "clicked" else "",
        "confirmed_at": now_iso if str(status).strip().lower() in {"confirmed", "failed"} else "",
        "last_message": str(message or ""),
        "candidate": dict(candidate) if isinstance(candidate, dict) else None,
        "position_snapshot": dict(position_snapshot) if isinstance(position_snapshot, dict) else None,
    }


def _upsert_custom_human_inflight_order(
    shared: dict[str, Any],
    signal_item: dict[str, Any],
    *,
    status: str,
    observed_at: datetime | None = None,
    message: str = "",
    candidate: dict[str, Any] | None = None,
    position_snapshot: dict[str, Any] | None = None,
    persist: bool = False,
) -> dict[str, Any]:
    timestamp = observed_at or datetime.now(tz=APP_TIMEZONE)
    signal_id = str(signal_item.get("signal_id", "") or "").strip()
    if not signal_id:
        return {}
    lock = _custom_human_shared_lock(shared)
    inflight_entry: dict[str, Any] | None = None
    if lock is not None:
        with lock:
            inflight_orders = _coerce_custom_human_inflight_orders(shared.get("inflight_orders"))
            current = inflight_orders.get(signal_id) or _build_custom_human_inflight_order(
                signal_item=signal_item,
                status="reserved",
                observed_at=timestamp,
                candidate=candidate,
            )
            current["signal"] = str(signal_item.get("signal", current.get("signal", "")) or "").strip().upper()
            current["event"] = str(signal_item.get("event", current.get("event", "")) or "").strip().lower()
            current["action"] = str(signal_item.get("action", current.get("action", "")) or "").strip().lower()
            current["position_key"] = str(signal_item.get("position_key", current.get("position_key", "")) or "").strip()
            current["instrument"] = str(signal_item.get("instrument", current.get("instrument", "")) or "").strip().upper()
            current["quantity"] = float(_safe_float(signal_item.get("quantity")) or current.get("quantity") or 0.0)
            current["status"] = str(status or current.get("status", "reserved")).strip().lower()
            current["last_message"] = str(message or current.get("last_message", "") or "")
            if not str(current.get("reserved_at", "") or "").strip():
                current["reserved_at"] = timestamp.isoformat()
            if isinstance(candidate, dict):
                current["candidate"] = dict(candidate)
            if current["status"] == "queued":
                current["queued_at"] = timestamp.isoformat()
            elif current["status"] == "clicked":
                current["clicked_at"] = timestamp.isoformat()
            elif current["status"] in {"confirmed", "failed"}:
                current["confirmed_at"] = timestamp.isoformat()
            if isinstance(position_snapshot, dict):
                current["position_snapshot"] = dict(position_snapshot)
            inflight_orders[signal_id] = current
            shared["inflight_orders"] = inflight_orders
            inflight_entry = dict(current)
    else:
        inflight_orders = _coerce_custom_human_inflight_orders(shared.get("inflight_orders"))
        current = inflight_orders.get(signal_id) or _build_custom_human_inflight_order(
            signal_item=signal_item,
            status="reserved",
            observed_at=timestamp,
            candidate=candidate,
        )
        current["signal"] = str(signal_item.get("signal", current.get("signal", "")) or "").strip().upper()
        current["event"] = str(signal_item.get("event", current.get("event", "")) or "").strip().lower()
        current["action"] = str(signal_item.get("action", current.get("action", "")) or "").strip().lower()
        current["position_key"] = str(signal_item.get("position_key", current.get("position_key", "")) or "").strip()
        current["instrument"] = str(signal_item.get("instrument", current.get("instrument", "")) or "").strip().upper()
        current["quantity"] = float(_safe_float(signal_item.get("quantity")) or current.get("quantity") or 0.0)
        current["status"] = str(status or current.get("status", "reserved")).strip().lower()
        current["last_message"] = str(message or current.get("last_message", "") or "")
        if not str(current.get("reserved_at", "") or "").strip():
            current["reserved_at"] = timestamp.isoformat()
        if isinstance(candidate, dict):
            current["candidate"] = dict(candidate)
        if current["status"] == "queued":
            current["queued_at"] = timestamp.isoformat()
        elif current["status"] == "clicked":
            current["clicked_at"] = timestamp.isoformat()
        elif current["status"] in {"confirmed", "failed"}:
            current["confirmed_at"] = timestamp.isoformat()
        if isinstance(position_snapshot, dict):
            current["position_snapshot"] = dict(position_snapshot)
        inflight_orders[signal_id] = current
        shared["inflight_orders"] = inflight_orders
        inflight_entry = dict(current)
    if persist:
        _persist_custom_human_runtime_state(shared)
    return inflight_entry or {}


def _clear_custom_human_inflight_order(
    shared: dict[str, Any],
    signal_id: str,
    *,
    persist: bool = False,
) -> dict[str, Any] | None:
    token = str(signal_id or "").strip()
    if not token:
        return None
    removed: dict[str, Any] | None = None
    lock = _custom_human_shared_lock(shared)
    if lock is not None:
        with lock:
            inflight_orders = _coerce_custom_human_inflight_orders(shared.get("inflight_orders"))
            removed_raw = inflight_orders.pop(token, None)
            shared["inflight_orders"] = inflight_orders
            removed = dict(removed_raw) if isinstance(removed_raw, dict) else None
    else:
        inflight_orders = _coerce_custom_human_inflight_orders(shared.get("inflight_orders"))
        removed_raw = inflight_orders.pop(token, None)
        shared["inflight_orders"] = inflight_orders
        removed = dict(removed_raw) if isinstance(removed_raw, dict) else None
    if persist:
        _persist_custom_human_runtime_state(shared)
    return removed


def _custom_human_has_nonterminal_inflight(shared: dict[str, Any] | None) -> bool:
    inflight_orders = _coerce_custom_human_inflight_orders((shared or {}).get("inflight_orders"))
    for entry in inflight_orders.values():
        status = str((entry or {}).get("status", "") or "").strip().lower()
        if status in {"reserved", "queued", "clicked"}:
            return True
    return False


def _consume_custom_human_terminal_inflight_confirmation(
    shared: dict[str, Any],
    signal_id: str,
) -> dict[str, Any] | None:
    token = str(signal_id or "").strip()
    if not token:
        return None
    inflight_orders = _coerce_custom_human_inflight_orders(shared.get("inflight_orders"))
    entry = inflight_orders.get(token)
    if not isinstance(entry, dict):
        return None
    status = str(entry.get("status", "") or "").strip().lower()
    if status not in {"confirmed", "failed"}:
        return None
    event = str(entry.get("event", "") or "").strip().lower()
    action = str(entry.get("action", "") or "").strip().lower()
    confirmation_status = "click_failed"
    if status == "confirmed":
        if event == "exit" or action == "exit" or str(entry.get("signal", "")).strip().upper() == "FLAT":
            confirmation_status = "confirmed_flat"
        elif action == "add" or event == "add":
            confirmation_status = "confirmed_add"
        else:
            confirmation_status = "confirmed_open"
    return {
        "signal_id": token,
        "signal": str(entry.get("signal", "") or "").strip().upper(),
        "event": event,
        "instrument": str(entry.get("instrument", "") or "").strip().upper(),
        "confirmed": status == "confirmed",
        "status": confirmation_status,
        "message": str(entry.get("last_message", "") or ""),
        "position_snapshot": (
            dict(entry.get("position_snapshot")) if isinstance(entry.get("position_snapshot"), dict) else None
        ),
        "confirmed_at": str(entry.get("confirmed_at", "") or ""),
    }



def _synthesize_custom_human_inflight_confirmation_from_snapshot(
    shared: dict[str, Any],
    state_raw: dict[str, Any] | None,
) -> dict[str, Any] | None:
    state = _coerce_custom_human_live_state(state_raw)
    pending_signal_id = str(state.get("pending_signal_id", "") or "").strip()
    if not pending_signal_id:
        return None
    inflight_orders = _coerce_custom_human_inflight_orders(shared.get("inflight_orders"))
    entry = inflight_orders.get(pending_signal_id)
    if not isinstance(entry, dict):
        return None
    inflight_status = str(entry.get("status", "") or "").strip().lower()
    if inflight_status not in {"reserved", "queued", "clicked", "confirmed"}:
        return None
    snapshot = _coerce_custom_human_tradovate_snapshot(shared.get("tradovate_snapshot"))
    if not isinstance(snapshot, dict):
        return None

    required_consecutive = _broker_snapshot_confirmation_requirement()
    now_ts = datetime.now(tz=APP_TIMEZONE)
    started_at_raw = (
        str(entry.get("clicked_at", "") or "").strip()
        or str(entry.get("queued_at", "") or "").strip()
        or str(entry.get("reserved_at", "") or "").strip()
    )
    started_at = pd.to_datetime(started_at_raw, errors="coerce")
    if pd.isna(started_at):
        started_at = pd.Timestamp(now_ts)
    if getattr(started_at, "tzinfo", None) is None:
        started_at = started_at.tz_localize(APP_TIMEZONE)
    else:
        started_at = started_at.tz_convert(APP_TIMEZONE)
    elapsed_s = max(0.0, float((pd.Timestamp(now_ts) - pd.Timestamp(started_at)).total_seconds()))

    pending_event = str(state.get("pending_event", "") or entry.get("event", "") or "").strip().lower()
    signal = str(entry.get("signal", "") or "").strip().upper()
    expected_flat = pending_event == "exit" or signal == "FLAT"
    position_open = bool(snapshot.get("position_open", False))
    account_ok = bool(snapshot.get("account_ok", True))
    instrument_ok = bool(snapshot.get("instrument_visible", False))
    normalized_current_snapshot = _normalize_custom_human_confirmation_snapshot(snapshot)
    previous_snapshot = entry.get("recovery_last_confirmation_snapshot")
    previous_snapshot_payload = (
        dict(previous_snapshot) if isinstance(previous_snapshot, dict) else None
    )
    previous_streak = int(max(0, int(entry.get("recovery_confirmation_streak", 0) or 0)))

    def _store_recovery_snapshot(streak: int, payload: dict[str, Any] | None) -> None:
        updated = dict(entry)
        updated["recovery_confirmation_streak"] = int(max(0, streak))
        updated["recovery_last_confirmation_snapshot"] = (
            dict(payload) if isinstance(payload, dict) else None
        )
        inflight_orders[pending_signal_id] = updated
        shared["inflight_orders"] = inflight_orders

    if expected_flat:
        if position_open:
            _store_recovery_snapshot(0, None)
            if elapsed_s >= float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS):
                return {
                    "signal_id": pending_signal_id,
                    "signal": signal or "FLAT",
                    "event": "exit",
                    "instrument": str(entry.get("instrument", "") or "").strip().upper(),
                    "confirmed": False,
                    "status": "click_failed",
                    "message": (
                        f"Kunne ikke bekræfte FLAT med {required_consecutive} ens snapshots inden for "
                        f"{float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS):.0f}s efter restart/recovery."
                    ),
                    "position_snapshot": snapshot,
                    "confirmed_at": now_ts.isoformat(),
                }
            return None
        if previous_streak > 0 and previous_snapshot_payload != normalized_current_snapshot:
            _log_custom_human_runtime_event(
                shared,
                headline="BROKER SNAPSHOT FLIMREDE",
                detail=(
                    f"Recovery FLAT snapshots differed for {signal or 'FLAT'}. "
                    + _format_custom_human_snapshot_pair_for_log(previous_snapshot_payload, normalized_current_snapshot)
                ),
                tone="warning",
                kind="confirmation",
                level=logging.WARNING,
            )
        current_streak = (
            previous_streak + 1
            if previous_streak > 0 and previous_snapshot_payload == normalized_current_snapshot
            else 1
        )
        _store_recovery_snapshot(current_streak, normalized_current_snapshot)
        if current_streak < required_consecutive:
            if elapsed_s >= float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS):
                return {
                    "signal_id": pending_signal_id,
                    "signal": signal or "FLAT",
                    "event": "exit",
                    "instrument": str(entry.get("instrument", "") or "").strip().upper(),
                    "confirmed": False,
                    "status": "click_failed",
                    "message": (
                        f"Kunne ikke bekræfte FLAT med {required_consecutive} ens snapshots inden for "
                        f"{float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS):.0f}s efter restart/recovery."
                    ),
                    "position_snapshot": snapshot,
                    "confirmed_at": now_ts.isoformat(),
                }
            return None
        return {
            "signal_id": pending_signal_id,
            "signal": signal or "FLAT",
            "event": "exit",
            "instrument": str(entry.get("instrument", "") or "").strip().upper(),
            "confirmed": True,
            "status": "confirmed_flat",
            "message": "Broker-state gendannet fra snapshot efter restart/recovery (flat bekræftet).",
            "position_snapshot": snapshot,
            "confirmed_at": now_ts.isoformat(),
        }

    if not (position_open and account_ok and instrument_ok):
        _store_recovery_snapshot(0, None)
        if elapsed_s >= float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS):
            return {
                "signal_id": pending_signal_id,
                "signal": signal or ("BUY" if str(state.get("direction", "")).strip().lower() != "short" else "SELL"),
                "event": pending_event or "entry",
                "instrument": str(entry.get("instrument", "") or "").strip().upper(),
                "confirmed": False,
                "status": "click_failed",
                "message": (
                    f"Kunne ikke bekræfte åben position med {required_consecutive} ens snapshots inden for "
                    f"{float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS):.0f}s efter restart/recovery."
                ),
                "position_snapshot": snapshot,
                "confirmed_at": now_ts.isoformat(),
            }
        return None
    if previous_streak > 0 and previous_snapshot_payload != normalized_current_snapshot:
        _log_custom_human_runtime_event(
            shared,
            headline="BROKER SNAPSHOT FLIMREDE",
            detail=(
                f"Recovery OPEN snapshots differed for {signal or pending_event or 'ENTRY'}. "
                + _format_custom_human_snapshot_pair_for_log(previous_snapshot_payload, normalized_current_snapshot)
            ),
            tone="warning",
            kind="confirmation",
            level=logging.WARNING,
        )
    current_streak = (
        previous_streak + 1
        if previous_streak > 0 and previous_snapshot_payload == normalized_current_snapshot
        else 1
    )
    _store_recovery_snapshot(current_streak, normalized_current_snapshot)
    if current_streak < required_consecutive:
        if elapsed_s >= float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS):
            return {
                "signal_id": pending_signal_id,
                "signal": signal or ("BUY" if str(state.get("direction", "")).strip().lower() != "short" else "SELL"),
                "event": pending_event or "entry",
                "instrument": str(entry.get("instrument", "") or "").strip().upper(),
                "confirmed": False,
                "status": "click_failed",
                "message": (
                    f"Kunne ikke bekræfte åben position med {required_consecutive} ens snapshots inden for "
                    f"{float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS):.0f}s efter restart/recovery."
                ),
                "position_snapshot": snapshot,
                "confirmed_at": now_ts.isoformat(),
            }
        return None
    confirmation_status = "confirmed_add" if pending_event == "add" else "confirmed_open"
    return {
        "signal_id": pending_signal_id,
        "signal": signal or ("BUY" if str(state.get("direction", "")).strip().lower() != "short" else "SELL"),
        "event": pending_event or "entry",
        "instrument": str(entry.get("instrument", "") or "").strip().upper(),
        "confirmed": True,
        "status": confirmation_status,
        "message": "Broker-position gendannet fra snapshot efter restart/recovery.",
        "position_snapshot": snapshot,
        "confirmed_at": now_ts.isoformat(),
    }


def _reserve_custom_human_pending_dispatch(
    *,
    shared: dict[str, Any],
    state_raw: dict[str, Any] | None,
    candidate: dict[str, Any],
    signal_item: dict[str, Any],
    observed_at: datetime,
) -> dict[str, Any]:
    lock = _custom_human_shared_lock(shared)
    state = _mark_custom_human_live_candidate_pending(
        state_raw=state_raw,
        candidate=candidate,
        signal_id=str(signal_item.get("signal_id", "")),
    )
    state["last_note"] = (
        f"{str(candidate.get('event', '')).upper()} reserveret – dispatch til broker i gang."
    ).strip()
    if lock is not None:
        with lock:
            shared["live_state"] = state
            _upsert_custom_human_inflight_order(
                shared,
                signal_item,
                status="reserved",
                observed_at=observed_at,
                message="Signal reserveret før broker-dispatch.",
                candidate=candidate,
                persist=False,
            )
            _persist_custom_human_runtime_state(shared)
    else:
        shared["live_state"] = state
        _upsert_custom_human_inflight_order(
            shared,
            signal_item,
            status="reserved",
            observed_at=observed_at,
            message="Signal reserveret før broker-dispatch.",
            candidate=candidate,
            persist=False,
        )
        _persist_custom_human_runtime_state(shared)
    return state


def _rollback_custom_human_pending_dispatch(
    *,
    shared: dict[str, Any],
    state_raw: dict[str, Any] | None,
    signal_item: dict[str, Any],
    note: str,
) -> dict[str, Any]:
    state = _coerce_custom_human_live_state(state_raw)
    signal_id = str(signal_item.get("signal_id", "") or "").strip()
    if str(state.get("pending_signal_id", "") or "").strip() == signal_id:
        state = _clear_custom_human_live_pending(state)
    state["last_note"] = str(note or state.get("last_note", "") or "").strip()
    lock = _custom_human_shared_lock(shared)
    if lock is not None:
        with lock:
            shared["live_state"] = state
            _clear_custom_human_inflight_order(shared, signal_id, persist=False)
            _persist_custom_human_runtime_state(shared)
    else:
        shared["live_state"] = state
        _clear_custom_human_inflight_order(shared, signal_id, persist=False)
        _persist_custom_human_runtime_state(shared)
    return state


def _apply_custom_human_broker_snapshot(
    state: dict[str, Any],
    snapshot_raw: dict[str, Any] | None,
    *,
    confirmed_at: str = "",
) -> dict[str, Any]:
    snapshot = dict(snapshot_raw) if isinstance(snapshot_raw, dict) else None
    if snapshot is None:
        return state
    state["last_broker_snapshot"] = snapshot
    state["broker_position_qty"] = _safe_float(snapshot.get("position_qty"))
    state["broker_account_value"] = str(snapshot.get("account_value", "") or "")
    state["last_reconciled_at"] = str(confirmed_at or "")
    return state


def _reconcile_custom_human_live_state_with_broker_snapshot(
    *,
    state_raw: dict[str, Any] | None,
    snapshot_raw: dict[str, Any] | None,
    runtime_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = _coerce_custom_human_live_state(state_raw)
    snapshot = dict(snapshot_raw) if isinstance(snapshot_raw, dict) else None
    profile = runtime_profile if isinstance(runtime_profile, dict) else {}
    state = _apply_custom_human_broker_snapshot(state, snapshot)
    if snapshot is None:
        return state

    position_open = bool(snapshot.get("position_open", False))
    position_qty = _safe_float_startup(snapshot.get("position_qty")) or 0.0
    position_side = str(snapshot.get("position_side", "") or "").strip().lower()

    if not position_open or abs(position_qty) <= 0.0:
        return state

    state["position_open"] = True
    if position_side in {"long", "short"}:
        state["direction"] = position_side

    has_trade_metadata = all(
        state.get(key) is not None
        for key in ("entry_price", "stop_price", "active_stop", "risk_pts", "entry_bar_index")
    )
    if has_trade_metadata:
        base_qty = max(1.0, float(int(max(1, int(round(float(profile.get("fixed_contracts", 1) or 1)))))))
        recovered_legs = max(1, int(round(abs(position_qty) / base_qty))) if abs(position_qty) > 0 else 1
        max_adds_allowed = max(
            0,
            min(
                int(CUSTOM_HUMAN_MAX_ADDS),
                int(profile.get("max_add_to_winners", CUSTOM_HUMAN_MAX_ADDS) or CUSTOM_HUMAN_MAX_ADDS),
            ),
        )
        state["add_count_sent"] = min(max_adds_allowed, max(0, recovered_legs - 1))
        if str(state.get("phase", "")).strip().lower() not in {"manual_reconcile", "entry_pending", "exit_pending"}:
            state["phase"] = "in_position"
        if not str(state.get("last_note", "")).strip():
            state["last_note"] = (
                f"Broker-position gendannet ({state.get('direction', position_side) or 'ukendt'}) "
                f"qty {abs(position_qty):g}."
            )
        return state

    state["phase"] = "manual_reconcile"
    state["reconcile_required"] = True
    state["last_note"] = (
        f"Broker-position gendannet efter reconnect ({position_side or 'ukendt'}) qty {abs(position_qty):g}, "
        "men trade-metadata mangler. Nye entries er pauset."
    )
    return state



def _clear_stale_custom_human_reconcile_from_snapshot(
    shared: dict[str, Any] | None,
    *,
    reset_context: str = "frisk snapshot",
) -> bool:
    if not isinstance(shared, dict):
        return False
    current_state = _coerce_custom_human_live_state(shared.get("live_state"))
    trade_date = str(current_state.get("trade_date", "") or "")
    reset_state, stale_reconcile_reset = _reset_stale_custom_human_reconcile_on_start(
        state_raw=current_state,
        snapshot_raw=shared.get("tradovate_snapshot"),
        trade_date=trade_date,
        reset_context=reset_context,
    )
    if not stale_reconcile_reset:
        return False
    position_key = _custom_human_router_position_key(shared)
    shared["live_state"] = reset_state
    shared["live_observer_status"] = (
        str(reset_state.get("last_note", "")).strip() or "Klar – venter på signal..."
    )
    shared["live_last_confirmation"] = None
    shared["last_result"] = "Klar – venter på signal..."
    _release_custom_human_router_cycle_for_flat_position(
        shared,
        position_key=position_key,
        reset_context=reset_context,
    )
    return True


def _sanitize_custom_human_runtime_after_stop(shared: dict[str, Any], *, reason: str) -> bool:
    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    snapshot = _coerce_custom_human_tradovate_snapshot_startup(shared.get("tradovate_snapshot"))
    snapshot_position_open = bool(snapshot.get("position_open", False)) if isinstance(snapshot, dict) else False
    snapshot_position_qty = abs(_safe_float_startup((snapshot or {}).get("position_qty")) or 0.0)
    has_nonterminal_inflight = _custom_human_has_nonterminal_inflight(shared)
    if snapshot_position_open or snapshot_position_qty > 0.0 or has_nonterminal_inflight:
        return False

    trade_date = str(live_state.get("trade_date", "") or "")
    reset_state = _default_custom_human_live_state(trade_date=trade_date)
    if isinstance(snapshot, dict):
        reset_state["broker_position_qty"] = 0.0
        reset_state["broker_account_value"] = str(snapshot.get("account_value", "") or "")
        reset_state["last_broker_snapshot"] = dict(snapshot)
        reset_state["last_reconciled_at"] = str(snapshot.get("observed_at", "") or "")
    reset_state["last_note"] = str(reason or "Auto stoppet.")
    shared["live_state"] = reset_state
    shared["inflight_orders"] = {}
    shared["live_last_dispatch"] = None
    shared["live_last_confirmation"] = None
    return True


def _apply_custom_human_live_confirmation(
    *,
    state_raw: dict[str, Any] | None,
    confirmation: dict[str, Any],
) -> dict[str, Any]:
    state = _coerce_custom_human_live_state(state_raw)
    pending_signal_id = str(state.get("pending_signal_id", "")).strip()
    confirmation_signal_id = str(confirmation.get("signal_id", "")).strip()
    if not pending_signal_id or confirmation_signal_id != pending_signal_id:
        return state

    pending_candidate = state.get("pending_candidate")
    pending_event = str(state.get("pending_event", "")).strip().lower() or str(
        confirmation.get("event", "signal")
    ).strip().lower() or "signal"
    confirmation_message = str(confirmation.get("message", "")).strip()
    confirmed = bool(confirmation.get("confirmed", False))
    position_snapshot = confirmation.get("position_snapshot")
    confirmed_at = str(confirmation.get("confirmed_at", "") or "")

    if confirmed and isinstance(pending_candidate, dict):
        state = _apply_custom_human_live_candidate(state_raw=state, candidate=pending_candidate)
        state = _apply_custom_human_broker_snapshot(
            state,
            position_snapshot,
            confirmed_at=confirmed_at,
        )
        state["reconcile_required"] = False
        if confirmation_message:
            state["last_note"] = f"{pending_event.upper()} bekræftet · {confirmation_message}"
        return _clear_custom_human_live_pending(state)

    state = _apply_custom_human_broker_snapshot(
        state,
        position_snapshot,
        confirmed_at=confirmed_at,
    )
    state["reconcile_required"] = True
    state["phase"] = "manual_reconcile"
    state["last_note"] = (
        f"{pending_event.upper()} klik udført men broker-state kunne ikke bekræftes."
        + (f" {confirmation_message}" if confirmation_message else "")
    ).strip()
    return _clear_custom_human_live_pending(state)


def _rescue_failed_add_confirmation_from_snapshot(
    shared: dict[str, Any],
    current_state: dict[str, Any],
    confirmation: dict[str, Any],
    runtime_profile: dict[str, Any],
) -> dict[str, Any]:
    """Fix 1: Snapshot-rescue for failed add verifications.

    When ``_verify_cdp_signal_broker_state`` times out for an ADD signal the
    most likely cause is Tradovate DOM lag — the add *did* execute but the
    position qty didn't appear in 2 consecutive snapshots within 10 seconds.
    Before we accept ``confirmed=False`` (which triggers ``reconcile_required``)
    we do one final check of the live snapshot.  If it already shows the
    expected position qty we synthesise a confirmed result and trading
    continues normally.
    """
    if bool(confirmation.get("confirmed", False)):
        return confirmation
    event = str(confirmation.get("event", "") or "").strip().lower()
    if event != "add":
        return confirmation

    snapshot = _coerce_custom_human_tradovate_snapshot(shared.get("tradovate_snapshot"))
    if not isinstance(snapshot, dict):
        return confirmation
    if not bool(snapshot.get("position_open", False)):
        return confirmation

    position_qty = abs(_safe_float(snapshot.get("position_qty")) or 0.0)
    if position_qty <= 0.0:
        return confirmation

    base_qty = max(1.0, float(int(max(1, int(round(float(runtime_profile.get("fixed_contracts", 1) or 1)))))))
    add_count_sent = int(max(0, int(current_state.get("add_count_sent", 0) or 0)))
    # add_count_sent was already incremented when the signal was *reserved*
    # so add_count_sent=1 means "1 add has been sent" → we expect base_qty * 2 contracts
    expected_qty = base_qty * (add_count_sent + 1)

    if position_qty >= expected_qty:
        rescued = dict(confirmation)
        rescued["confirmed"] = True
        rescued["status"] = "confirmed_add_snapshot_rescue"
        rescued["message"] = (
            f"Add bekræftet via snapshot-rescue "
            f"(snapshot qty {position_qty:g} ≥ forventet {expected_qty:g}). "
            f"Original broker-verifikation fejlede pga. Tradovate DOM-lag."
        )
        return rescued

    return confirmation


def _custom_human_first_dual_trigger_details(
    *,
    day_bars: pd.DataFrame,
    start_bar: int,
    max_trigger_bars: int,
    long_entry_level: float,
    short_entry_level: float,
) -> dict[str, Any] | None:
    bar_index = pd.to_numeric(day_bars["bar_index"], errors="coerce").fillna(0).astype(int).to_numpy(dtype=int)
    highs = pd.to_numeric(day_bars["high"], errors="coerce").to_numpy(dtype=float)
    lows = pd.to_numeric(day_bars["low"], errors="coerce").to_numpy(dtype=float)
    if bar_index.size == 0:
        return None

    start_idx = int(np.searchsorted(bar_index, int(start_bar), side="left"))
    if start_idx >= bar_index.size:
        return None

    search_highs = highs[start_idx:]
    search_lows = lows[start_idx:]
    search_bars = bar_index[start_idx:]
    end_bar = int(start_bar) + int(max_trigger_bars) - 1
    within = search_bars <= end_bar
    search_highs = search_highs[within]
    search_lows = search_lows[within]
    search_bars = search_bars[within]
    if search_bars.size == 0:
        return None

    long_pos = np.flatnonzero(search_highs >= float(long_entry_level))
    short_pos = np.flatnonzero(search_lows <= float(short_entry_level))
    if long_pos.size == 0 and short_pos.size == 0:
        return None

    long_idx = int(long_pos[0]) if long_pos.size > 0 else None
    short_idx = int(short_pos[0]) if short_pos.size > 0 else None
    if long_idx is not None and short_idx is not None:
        if long_idx == short_idx:
            return None
        if long_idx < short_idx:
            return {"direction": "long", "entry_price": float(long_entry_level), "entry_bar_index": int(search_bars[long_idx])}
        return {"direction": "short", "entry_price": float(short_entry_level), "entry_bar_index": int(search_bars[short_idx])}
    if long_idx is not None:
        return {"direction": "long", "entry_price": float(long_entry_level), "entry_bar_index": int(search_bars[long_idx])}
    return {"direction": "short", "entry_price": float(short_entry_level), "entry_bar_index": int(search_bars[short_idx])}


def _evaluate_custom_human_live_state(
    *,
    day_bars: pd.DataFrame,
    cfg: CustomStrategyConfig,
    state_raw: dict[str, Any] | None,
    observed_at: datetime,
    max_add_to_winners: int,
    session_close_dk: time,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    state = _coerce_custom_human_live_state(state_raw)
    if bool(state.get("reconcile_required", False)):
        state["last_note"] = str(state.get("last_note", "") or "Manual reconcile krævet før nye signaler.")
        return state, None
    if str(state.get("pending_signal_id", "")).strip():
        state["last_note"] = str(state.get("last_note", "") or "Afventer broker-bekræftelse.")
        return state, None
    if day_bars.empty:
        state["phase"] = "waiting_for_setup"
        state["last_note"] = "Ingen live-bars endnu."
        return state, None

    bars = day_bars.copy().sort_values("timestamp_dk").reset_index(drop=True)
    bars["trade_date"] = pd.to_datetime(bars["trade_date"], errors="coerce").dt.normalize()
    latest_trade_date = pd.to_datetime(bars["trade_date"], errors="coerce").dropna().max()
    trade_date_token = pd.Timestamp(latest_trade_date).strftime("%Y-%m-%d") if pd.notna(latest_trade_date) else ""
    session_clock = resolve_school_run_session_clock_dk(
        cfg.bar1_start,
        trade_date=latest_trade_date if pd.notna(latest_trade_date) else observed_at.date(),
    )
    if trade_date_token and state.get("trade_date") != trade_date_token:
        state = _default_custom_human_live_state(trade_date=trade_date_token)

    latest_ts = pd.to_datetime(bars["timestamp_dk"], errors="coerce").dropna().max()
    state["trade_date"] = trade_date_token
    state["market_timestamp"] = latest_ts.isoformat() if pd.notna(latest_ts) else ""

    if state["phase"] == "flat":
        state["last_note"] = state.get("last_note") or "Position er allerede flattet for dagen."
        return state, None
    if state["phase"] == "expired" and not bool(state.get("position_open")):
        state["last_note"] = state.get("last_note") or "Trigger-vinduet er udløbet."
        return state, None

    setup = _school_run_setup(day_bars=bars, cfg=cfg)
    if setup is None:
        state["phase"] = "expired" if observed_at.astimezone(APP_TIMEZONE).time() >= session_close_dk else "waiting_for_setup"
        state["last_note"] = (
            f"Afventer Bar 1 + Bar 2. Dagens Bar 1: {session_clock.bar1_start_dk.strftime('%H:%M')} DK, "
            f"Bar 2: {session_clock.bar2_start_dk.strftime('%H:%M')} DK."
            if state["phase"] != "expired"
            else "Session sluttede uden School Run setup."
        )
        return state, None

    state["start_bar"] = int(setup.start_bar)
    state["max_trigger_bar"] = int(setup.start_bar) + int(cfg.max_trigger_bars) - 1

    if not bool(state.get("position_open")) and state.get("entry_price") is None:
        trigger = _custom_human_first_dual_trigger_details(
            day_bars=bars,
            start_bar=int(setup.start_bar),
            max_trigger_bars=int(cfg.max_trigger_bars),
            long_entry_level=float(setup.long_entry),
            short_entry_level=float(setup.short_entry),
        )
        if trigger is None:
            last_bar_index = int(pd.to_numeric(bars["bar_index"], errors="coerce").fillna(0).astype(int).max())
            session_closed = observed_at.astimezone(APP_TIMEZONE).time() >= session_close_dk
            state["phase"] = "expired" if session_closed or last_bar_index > int(state["max_trigger_bar"] or 0) else "armed"
            state["last_note"] = (
                f"Setup armed – afventer breakout fra {session_clock.trigger_start_dk.strftime('%H:%M')} DK."
                if state["phase"] != "expired"
                else "Trigger-vinduet udløb uden entry."
            )
            return state, None

        direction = str(trigger["direction"])
        entry_price = float(trigger["entry_price"])
        raw_stop = float(setup.long_stop if direction == "long" else setup.short_stop)
        mgmt = _management_config_for_model(
            execution_model=_custom_human_execution_model_id(str(cfg.execution_model)),
            strategy_name="School Run",
        )
        stop_price = _apply_stop_cap(
            entry_level=float(entry_price),
            stop_level=float(raw_stop),
            direction=direction,
            max_stop_pts=mgmt.max_stop_pts,
        )
        risk_pts = abs(float(entry_price) - float(stop_price))
        if risk_pts <= 0:
            state["phase"] = "error"
            state["last_note"] = "Ugyldig live stop-distance for School Run."
            return state, None
        state["phase"] = "entry_pending"
        state["last_note"] = f"Entry klar: {direction} @ {entry_price:.2f}"
        return state, {
            "event": "entry",
            "direction": direction,
            "trade_date": trade_date_token,
            "signal_price": float(entry_price),
            "entry_price": float(entry_price),
            "stop_price": float(stop_price),
            "risk_pts": float(risk_pts),
            "bar_index": int(trigger["entry_bar_index"]),
        }

    if not bool(state.get("position_open")):
        return state, None

    direction = str(state.get("direction", "")).strip().lower()
    entry_price = _safe_float(state.get("entry_price"))
    initial_stop = _safe_float(state.get("stop_price"))
    active_stop = _safe_float(state.get("active_stop"))
    risk_pts = _safe_float(state.get("risk_pts"))
    entry_bar_index = state.get("entry_bar_index")
    if direction not in {"long", "short"} or entry_price is None or initial_stop is None or risk_pts is None or entry_bar_index is None:
        state["phase"] = "error"
        state["last_note"] = "Live state mangler entry/stop/risk metadata."
        return state, None

    mgmt = _management_config_for_model(
        execution_model=_custom_human_execution_model_id(str(cfg.execution_model)),
        strategy_name="School Run",
    )
    max_adds_allowed = max(0, min(int(max_add_to_winners), int(mgmt.max_add_ons)))
    bar_index = pd.to_numeric(bars["bar_index"], errors="coerce").fillna(0).astype(int).to_numpy(dtype=int)
    highs = pd.to_numeric(bars["high"], errors="coerce").to_numpy(dtype=float)
    lows = pd.to_numeric(bars["low"], errors="coerce").to_numpy(dtype=float)
    closes = pd.to_numeric(bars["close"], errors="coerce").to_numpy(dtype=float)
    start_bar = int(entry_bar_index)
    last_processed = state.get("last_processed_bar_index")
    if last_processed is None:
        start_idx = int(np.searchsorted(bar_index, start_bar, side="left"))
    else:
        start_idx = int(np.searchsorted(bar_index, int(last_processed) + 1, side="left"))

    current_stop = float(active_stop if active_stop is not None else initial_stop)
    break_even_armed = bool(state.get("break_even_armed", False))
    add_count_sent = int(max(0, int(state.get("add_count_sent", 0) or 0)))
    max_favorable_pts = float(_safe_float(state.get("max_favorable_pts")) or 0.0)
    max_adverse_pts = float(_safe_float(state.get("max_adverse_pts")) or 0.0)

    for idx in range(start_idx, len(bar_index)):
        bar_no = int(bar_index[idx])
        if bar_no < start_bar:
            continue

        bar_high = float(highs[idx])
        bar_low = float(lows[idx])
        bar_close = float(closes[idx])

        if direction == "long":
            if bar_low <= current_stop:
                exit_sequence = _next_custom_human_exit_sequence(state)
                state["phase"] = "exit_pending"
                state["last_note"] = f"Stop/trail ramt @ {current_stop:.2f}"
                return state, {
                    "event": "exit",
                    "direction": direction,
                    "trade_date": trade_date_token,
                    "signal_price": float(current_stop),
                    "entry_price": float(entry_price),
                    "stop_price": float(initial_stop),
                    "risk_pts": float(risk_pts),
                    "bar_index": bar_no,
                    "exit_reason": "stop_or_trail",
                    "exit_sequence": int(exit_sequence),
                }
            favorable_now = max(0.0, bar_high - float(entry_price))
            adverse_now = max(0.0, float(entry_price) - bar_low)
        else:
            if bar_high >= current_stop:
                exit_sequence = _next_custom_human_exit_sequence(state)
                state["phase"] = "exit_pending"
                state["last_note"] = f"Stop/trail ramt @ {current_stop:.2f}"
                return state, {
                    "event": "exit",
                    "direction": direction,
                    "trade_date": trade_date_token,
                    "signal_price": float(current_stop),
                    "entry_price": float(entry_price),
                    "stop_price": float(initial_stop),
                    "risk_pts": float(risk_pts),
                    "bar_index": bar_no,
                    "exit_reason": "stop_or_trail",
                    "exit_sequence": int(exit_sequence),
                }
            favorable_now = max(0.0, float(entry_price) - bar_low)
            adverse_now = max(0.0, bar_high - float(entry_price))

        max_favorable_pts = max(max_favorable_pts, float(favorable_now))
        max_adverse_pts = max(max_adverse_pts, float(adverse_now))
        favorable_r = favorable_now / float(risk_pts)

        if not break_even_armed and favorable_r >= float(mgmt.break_even_trigger_r):
            break_even_armed = True
            if direction == "long":
                current_stop = max(current_stop, float(entry_price))
            else:
                current_stop = min(current_stop, float(entry_price))

        if break_even_armed and favorable_r >= float(mgmt.trail_activation_r):
            locked_favorable = max(0.0, favorable_now - float(mgmt.trail_giveback_r) * float(risk_pts))
            trail_candidate = (
                float(entry_price) + locked_favorable
                if direction == "long"
                else float(entry_price) - locked_favorable
            )
            if direction == "long":
                current_stop = max(current_stop, float(trail_candidate))
            else:
                current_stop = min(current_stop, float(trail_candidate))

        state["active_stop"] = float(current_stop)
        state["break_even_armed"] = bool(break_even_armed)
        state["max_favorable_pts"] = float(max_favorable_pts)
        state["max_adverse_pts"] = float(max_adverse_pts)
        state["phase"] = "in_position"
        state["last_note"] = f"Position aktiv · stop {current_stop:.2f}"

        if add_count_sent < max_adds_allowed:
            next_add_idx = add_count_sent + 1
            if mgmt.add_on_step_pts is not None and mgmt.add_on_step_pts > 0:
                next_add_level_pts = float(next_add_idx) * float(mgmt.add_on_step_pts)
            else:
                next_add_level_pts = float(next_add_idx) * float(mgmt.add_on_trigger_r) * float(risk_pts)
            if favorable_now >= next_add_level_pts:
                state["last_note"] = f"Add #{next_add_idx} klar"
                return state, {
                    "event": "add",
                    "direction": direction,
                    "trade_date": trade_date_token,
                    "signal_price": (
                        float(entry_price) + next_add_level_pts
                        if direction == "long"
                        else float(entry_price) - next_add_level_pts
                    ),
                    "entry_price": float(entry_price),
                    "stop_price": float(initial_stop),
                    "risk_pts": float(risk_pts),
                    "bar_index": bar_no,
                    "add_index": int(next_add_idx),
                    "add_trigger_r": float(next_add_level_pts / float(risk_pts)),
                }

        state["last_processed_bar_index"] = int(bar_no)

    latest_close = float(closes[-1]) if closes.size > 0 else None
    if latest_close is not None and observed_at.astimezone(APP_TIMEZONE).time() >= session_close_dk:
        state["phase"] = "exit_pending"
        state["last_note"] = f"Session close flatten @ {latest_close:.2f}"
        return state, {
            "event": "exit",
            "direction": direction,
            "trade_date": trade_date_token,
            "signal_price": float(latest_close),
            "entry_price": float(entry_price),
            "stop_price": float(initial_stop),
            "risk_pts": float(risk_pts),
            "bar_index": int(bar_index[-1]) if bar_index.size > 0 else None,
            "exit_reason": "session_close",
        }

    return state, None


def _apply_custom_human_live_candidate(
    *,
    state_raw: dict[str, Any] | None,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    state = _coerce_custom_human_live_state(state_raw)
    event = str(candidate.get("event", "")).strip().lower()
    if event == "entry":
        state["position_open"] = True
        state["phase"] = "in_position"
        state["direction"] = str(candidate.get("direction", "")).strip().lower()
        state["entry_price"] = float(candidate.get("entry_price"))
        state["stop_price"] = float(candidate.get("stop_price"))
        state["active_stop"] = float(candidate.get("stop_price"))
        state["risk_pts"] = float(candidate.get("risk_pts"))
        state["entry_bar_index"] = int(candidate.get("bar_index"))
        state["last_processed_bar_index"] = int(candidate.get("bar_index")) - 1
        state["break_even_armed"] = False
        state["add_count_sent"] = 0
        state["max_favorable_pts"] = 0.0
        state["max_adverse_pts"] = 0.0
        state["exit_signal_seq"] = 0
        state["last_note"] = f"Entry queued @ {float(candidate.get('entry_price')):.2f}"
        return state
    if event == "add":
        state["add_count_sent"] = int(max(0, int(state.get("add_count_sent", 0) or 0))) + 1
        state["phase"] = "in_position"
        if candidate.get("bar_index") is not None:
            state["last_processed_bar_index"] = int(candidate.get("bar_index"))
        state["last_note"] = f"Add #{int(candidate.get('add_index', 0))} queued"
        return state
    if event == "exit":
        state["position_open"] = False
        state["phase"] = "flat"
        if candidate.get("bar_index") is not None:
            state["last_processed_bar_index"] = int(candidate.get("bar_index"))
        state["last_note"] = f"Flatten queued ({candidate.get('exit_reason', 'exit')})"
        return state
    return state


def _next_custom_human_exit_sequence(state: dict[str, Any]) -> int:
    seq = int(max(0, int(state.get("exit_signal_seq", 0) or 0))) + 1
    state["exit_signal_seq"] = seq
    return seq


def _custom_human_snapshot_stop_cross_candidate(
    *,
    state_raw: dict[str, Any] | None,
    snapshot_raw: dict[str, Any] | None,
    observed_at: datetime,
    management_config: TomLiveManagementConfig | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Real-time stop/trail check that runs on every snapshot poll (~2s).

    When *management_config* is provided the function also performs intra-bar
    trailing (Fix 2 / AGGRESSIVE model):

    1. Track ``intrabar_best_seen`` — the running most-favorable price since
       entry (ask for longs, bid for shorts).  Monotonically ratchets.
    2. Arm break-even when ``favorable_r >= break_even_trigger_r``.
    3. Ratchet ``active_stop`` up/down when ``favorable_r >= trail_activation_r``.
    4. Then check whether market_price has crossed ``active_stop`` → exit.

    The intra-bar trail update runs on every call regardless of a stop cross,
    so ``active_stop`` in state stays current between bar closes.
    """
    state = _coerce_custom_human_live_state(state_raw)
    if bool(state.get("reconcile_required", False)) or str(state.get("pending_signal_id", "")).strip():
        return state, None
    if not bool(state.get("position_open", False)):
        return state, None

    direction = str(state.get("direction", "") or "").strip().lower()
    entry_price = _safe_float(state.get("entry_price"))
    stop_price = _safe_float(state.get("stop_price"))
    active_stop = _safe_float(state.get("active_stop"))
    risk_pts = _safe_float(state.get("risk_pts"))
    entry_bar_index = state.get("entry_bar_index")
    if direction not in {"long", "short"} or entry_price is None or stop_price is None or risk_pts is None or entry_bar_index is None:
        return state, None

    snapshot = _coerce_custom_human_tradovate_snapshot(snapshot_raw)
    if snapshot is None or not bool(snapshot.get("connected", False)) or not bool(snapshot.get("quote_ready", False)):
        return state, None
    if not bool(snapshot.get("position_open", False)):
        return state, None

    current_stop = float(active_stop if active_stop is not None else stop_price)

    bid_price = _safe_float(snapshot.get("bid_price")) or _safe_float(snapshot.get("bid_price_text"))
    ask_price = _safe_float(snapshot.get("ask_price")) or _safe_float(snapshot.get("ask_price_text"))
    last_price = _safe_float(snapshot.get("last_price")) or _safe_float(snapshot.get("last_price_text"))
    market_price = (
        float(bid_price) if direction == "long" and bid_price is not None
        else float(ask_price) if direction == "short" and ask_price is not None
        else float(last_price) if last_price is not None
        else None
    )
    if market_price is None:
        return state, None

    # ------------------------------------------------------------------
    # Intra-bar trailing stop update (runs every ~2s, AGGRESSIVE model).
    # Uses the most favorable snapshot price as a running high-water mark
    # so active_stop ratchets continuously rather than waiting for bar close.
    # ------------------------------------------------------------------
    if management_config is not None and float(risk_pts) > 0:
        # Favorable-side price: what we'd receive if we closed now.
        # Ask for longs (sell at ask), bid for shorts (buy back at bid).
        favorable_price = (
            float(ask_price) if direction == "long" and ask_price is not None
            else float(bid_price) if direction == "short" and bid_price is not None
            else float(market_price)
        )
        # Running ratchet — only moves in the favorable direction.
        intrabar_best = _safe_float(state.get("intrabar_best_seen"))
        if intrabar_best is None:
            intrabar_best = favorable_price
        elif direction == "long":
            intrabar_best = max(intrabar_best, favorable_price)
        else:
            intrabar_best = min(intrabar_best, favorable_price)
        state["intrabar_best_seen"] = float(intrabar_best)

        favorable_pts = (
            (intrabar_best - float(entry_price)) if direction == "long"
            else (float(entry_price) - intrabar_best)
        )
        favorable_r = favorable_pts / float(risk_pts)

        # Break-even: mirrors bar-based logic — arm once, never disarm.
        break_even_armed = bool(state.get("break_even_armed", False))
        if not break_even_armed and favorable_r >= float(management_config.break_even_trigger_r):
            break_even_armed = True
            state["break_even_armed"] = True
            be_stop = float(entry_price)
            current_stop = max(current_stop, be_stop) if direction == "long" else min(current_stop, be_stop)

        # Trail: ratchet active_stop toward the best price minus giveback.
        if break_even_armed and favorable_r >= float(management_config.trail_activation_r):
            giveback_pts = float(management_config.trail_giveback_r) * float(risk_pts)
            if direction == "long":
                provisional = float(intrabar_best) - giveback_pts
                current_stop = max(current_stop, provisional)
            else:
                provisional = float(intrabar_best) + giveback_pts
                current_stop = min(current_stop, provisional)

        # Write active_stop back if it moved (observer loop persists after each iteration).
        if abs(current_stop - float(active_stop if active_stop is not None else stop_price)) > 1e-9:
            state["active_stop"] = float(current_stop)

    # ------------------------------------------------------------------
    # Exit check: has market price crossed the (now up-to-date) stop?
    # ------------------------------------------------------------------
    stop_hit = market_price <= current_stop if direction == "long" else market_price >= current_stop
    if not stop_hit:
        return state, None

    exit_sequence = _next_custom_human_exit_sequence(state)
    state["phase"] = "exit_pending"
    state["last_note"] = f"Snapshot stop/trail ramt @ {current_stop:.2f}"
    state["market_timestamp"] = str(snapshot.get("observed_at", "") or observed_at.isoformat())
    return state, {
        "event": "exit",
        "direction": direction,
        "trade_date": str(state.get("trade_date", "") or observed_at.date().isoformat()),
        "signal_price": float(current_stop),
        "entry_price": float(entry_price),
        "stop_price": float(stop_price),
        "risk_pts": float(risk_pts),
        "bar_index": int(entry_bar_index),
        "exit_reason": "snapshot_stop_or_trail",
        "exit_sequence": int(exit_sequence),
    }


def _custom_human_snapshot_add_candidate(
    *,
    state_raw: dict[str, Any] | None,
    execution_model: str,
    max_add_to_winners: int,
    management_config: TomLiveManagementConfig | None = None,
    observed_at: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Intra-bar "Add to Winners" trigger — AGGRESSIVE model only.

    Fires at most once per add-level (one-shot via ``add_count_sent``).
    Must be called AFTER ``_custom_human_snapshot_stop_cross_candidate`` so
    that ``state["intrabar_best_seen"]`` is already set for this poll cycle.

    Exit always wins: the observer loop calls this only when the stop-cross
    function returned no exit candidate.
    """
    state = _coerce_custom_human_live_state(state_raw)

    # Only active in aggressive execution model.
    if _custom_human_execution_model_id(execution_model) != EXECUTION_MODEL_TOM_AGGRESSIVE:
        return state, None
    if management_config is None:
        return state, None
    if bool(state.get("reconcile_required", False)) or str(state.get("pending_signal_id", "")).strip():
        return state, None
    if not bool(state.get("position_open", False)):
        return state, None

    direction = str(state.get("direction", "") or "").strip().lower()
    entry_price = _safe_float(state.get("entry_price"))
    stop_price = _safe_float(state.get("stop_price"))
    risk_pts = _safe_float(state.get("risk_pts"))
    entry_bar_index = state.get("entry_bar_index")
    add_count_sent = int(max(0, int(state.get("add_count_sent", 0) or 0)))

    if (
        direction not in {"long", "short"}
        or entry_price is None
        or stop_price is None
        or risk_pts is None
        or float(risk_pts) <= 0
    ):
        return state, None

    # One-shot guard: add_count_sent is incremented at RESERVATION time, so
    # if add_count_sent >= max_adds_allowed the level is already dispatched.
    max_adds_allowed = max(0, min(int(max_add_to_winners), int(management_config.max_add_ons)))
    if add_count_sent >= max_adds_allowed:
        return state, None

    # intrabar_best_seen is set by _custom_human_snapshot_stop_cross_candidate.
    intrabar_best = _safe_float(state.get("intrabar_best_seen"))
    if intrabar_best is None:
        return state, None

    favorable_pts = (
        (intrabar_best - float(entry_price)) if direction == "long"
        else (float(entry_price) - intrabar_best)
    )
    if favorable_pts <= 0:
        return state, None

    next_add_idx = add_count_sent + 1
    if management_config.add_on_step_pts is not None and management_config.add_on_step_pts > 0:
        next_add_level_pts = float(next_add_idx) * float(management_config.add_on_step_pts)
    else:
        next_add_level_pts = float(next_add_idx) * float(management_config.add_on_trigger_r) * float(risk_pts)

    if favorable_pts < next_add_level_pts:
        return state, None

    active_stop = _safe_float(state.get("active_stop"))
    current_stop = float(active_stop if active_stop is not None else stop_price)

    return state, {
        "event": "add",
        "direction": direction,
        "trade_date": str(state.get("trade_date", "") or observed_at.date().isoformat()),
        "signal_price": float(intrabar_best),
        "entry_price": float(entry_price),
        "stop_price": float(current_stop),
        "risk_pts": float(risk_pts),
        "bar_index": int(entry_bar_index) if entry_bar_index is not None else 0,
        "add_index": int(next_add_idx),
        "add_trigger_r": round(favorable_pts / float(risk_pts), 3),
        "exit_reason": None,
        "source": "snapshot_intrabar",
    }


def _build_custom_human_live_signal(
    *,
    candidate: dict[str, Any],
    runtime_profile: dict[str, Any],
) -> dict[str, Any]:
    event = str(candidate.get("event", "")).strip().lower()
    direction = str(candidate.get("direction", "")).strip().lower()
    if event not in {"entry", "add", "exit"}:
        raise ValueError(f"Unsupported live candidate event: {event}")
    if direction not in {"long", "short"}:
        raise ValueError(f"Unsupported live candidate direction: {direction}")

    ticker = str(runtime_profile.get("ticker") or runtime_profile.get("contract_symbol") or "MYM").strip().upper() or "MYM"
    trade_date = str(candidate.get("trade_date", "")).strip()
    strategy_name = str(runtime_profile.get("strategy_name") or "School Run").strip() or "School Run"
    position_key = f"{ticker}:{trade_date}:{strategy_name}:{direction}"
    if event == "entry":
        action = "buy" if direction == "long" else "sell"
        signal = "BUY" if direction == "long" else "SELL"
        event_index = 0
    elif event == "add":
        action = "add"
        signal = "BUY" if direction == "long" else "SELL"
        event_index = int(candidate.get("add_index", 1))
    else:
        action = "exit"
        signal = "SELL" if direction == "long" else "BUY"
        event_index = int(candidate.get("exit_sequence", 99) or 99)

    raw = f"{position_key}|{event}|{event_index}"
    if event == "exit":
        exit_reason = str(candidate.get("exit_reason", "") or "exit").strip().lower() or "exit"
        signal_price = _safe_float(candidate.get("signal_price")) or 0.0
        raw = f"{raw}|{exit_reason}|{signal_price:.4f}"
    signal_id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    entry_price = float(candidate.get("entry_price"))
    stop_price = float(candidate.get("stop_price"))
    stop_loss_amount = abs(float(entry_price) - float(stop_price))
    signal_payload: dict[str, Any] = {
        "signal": signal,
        "action": action,
        "event": event,
        "signal_id": signal_id,
        "position_key": position_key,
        "trade_date": trade_date,
        "strategy_name": strategy_name,
        "direction": direction,
        "instrument": ticker,
        "interval": "15",
        "timeframe": "15m",
        "signal_price": float(candidate.get("signal_price")),
        "entry_price": entry_price,
        "stop_price": stop_price,
        "stop_loss_amount": float(stop_loss_amount),
        "risk_pts": float(candidate.get("risk_pts")),
        "quantity": int(max(1, int(round(float(runtime_profile.get("fixed_contracts", 1) or 1))))),
        "tick_value": float(CUSTOM_FUTURES_MYMPV),
    }
    if event == "add":
        signal_payload["add_index"] = int(candidate.get("add_index", 1))
        signal_payload["add_trigger_r"] = float(candidate.get("add_trigger_r", 0.0))
    if event == "exit":
        signal_payload["exit_reason"] = str(candidate.get("exit_reason", "") or "exit")
        signal_payload["exit_sequence"] = int(candidate.get("exit_sequence", 99) or 99)
    # Propagate bar_index → entry_bar_index so _build_custom_human_trade_intent can
    # include it in the idempotency key, preventing same-day duplicate blocking.
    _cand_bar_index = candidate.get("bar_index")
    if _cand_bar_index is not None:
        signal_payload["entry_bar_index"] = int(_cand_bar_index)
    return signal_payload


def _custom_human_live_observer_loop(
    stop_event: threading.Event,
    shared: dict[str, Any],
) -> None:
    observer_cfg = shared.get("live_observer_cfg")
    if not isinstance(observer_cfg, dict) or not bool(observer_cfg.get("enabled")):
        shared["live_observer_running"] = False
        return

    cfg = observer_cfg["config"]
    runtime_profile = dict(observer_cfg.get("runtime_profile", {}))
    pipeline = observer_cfg["pipeline"]
    router = observer_cfg["router"]
    live_cfg = dict(observer_cfg.get("live_cfg", {}))
    kill_switch = bool(observer_cfg.get("kill_switch", False))
    timezone_name = str(observer_cfg.get("timezone_name", "Europe/Copenhagen"))
    overnight_start_dk = observer_cfg.get("overnight_start_dk", time(0, 0))
    overnight_end_dk = observer_cfg.get("overnight_end_dk", time(8, 0))
    session_close_dk = observer_cfg.get("session_close_dk", time(22, 0))
    poll_seconds = float(observer_cfg.get("poll_seconds", CUSTOM_HUMAN_LIVE_POLL_SECONDS))
    stale_after_seconds = float(observer_cfg.get("stale_after_seconds", CUSTOM_HUMAN_LIVE_STALE_SECONDS))
    shared["live_observer_running"] = True
    try:
        while not stop_event.is_set():
            # Fix 3: Re-read kill_switch on every iteration so a RiskGate or UI
            # change takes effect immediately without requiring an observer restart.
            kill_switch = bool((shared.get("live_observer_cfg") or {}).get("kill_switch", False))
            base_loop_poll_seconds = _custom_human_live_observer_poll_seconds(shared.get("live_state"))
            base_loop_poll_seconds = max(0.5, min(base_loop_poll_seconds, poll_seconds))
            loop_poll_seconds = _custom_human_jittered_interval(base_loop_poll_seconds, floor=0.5)
            loop_poll_seconds = _apply_custom_human_bio_modulation(
                shared,
                loop_poll_seconds,
                floor=0.5,
            )
            if not bool(shared.get("running")):
                shared["live_observer_status"] = "Stoppet fordi CDP worker ikke længere kører."
                _persist_custom_human_runtime_state(shared)
                break
            try:
                observed_at = datetime.now(tz=APP_TIMEZONE)
                day_bars, market_meta = _load_custom_human_live_day_bars(
                    cfg=cfg,
                    timezone_name=timezone_name,
                    overnight_start_dk=overnight_start_dk,
                    overnight_end_dk=overnight_end_dk,
                    observed_at=observed_at,
                    shared=shared,
                )
                trade_date_for_session_close = ""
                if isinstance(market_meta, dict):
                    trade_date_for_session_close = str(market_meta.get("trade_date", "") or "")
                dynamic_session_close_dk = _custom_human_session_close_dk(
                    str(cfg.instrument),
                    trade_date_for_session_close,
                )
                latest_source_ts = pd.to_datetime(
                    market_meta.get("latest_source_timestamp"),
                    errors="coerce",
                )
                if pd.notna(latest_source_ts):
                    market_ts = pd.Timestamp(latest_source_ts)
                    if market_ts.tzinfo is None:
                        market_ts = market_ts.tz_localize(APP_TIMEZONE)
                    else:
                        market_ts = market_ts.tz_convert(APP_TIMEZONE)
                    age_seconds = max(0.0, (observed_at - market_ts.to_pydatetime()).total_seconds())
                    if age_seconds > stale_after_seconds:
                        shared["live_observer_status"] = (
                            f"Afventer frisk data – seneste feed er {age_seconds / 60.0:.1f} min gammelt."
                        )
                        _persist_custom_human_runtime_state(shared)
                        stop_event.wait(timeout=loop_poll_seconds)
                        continue

                current_state = _coerce_custom_human_live_state(shared.get("live_state"))
                position_was_open = bool(current_state.get("position_open", False))
                current_state = _reconcile_custom_human_live_state_with_broker_snapshot(
                    state_raw=current_state,
                    snapshot_raw=shared.get("tradovate_snapshot"),
                    runtime_profile=runtime_profile,
                )
                shared["live_state"] = current_state
                recovered_open_position = (not position_was_open) and bool(current_state.get("position_open", False))
                pending_signal_id = str(current_state.get("pending_signal_id", "")).strip()
                if recovered_open_position and not pending_signal_id:
                    shared["live_observer_status"] = str(
                        current_state.get("last_note", "Broker-position gendannet efter reconnect.")
                    ).strip() or "Broker-position gendannet efter reconnect."
                    _persist_custom_human_runtime_state(shared)
                    stop_event.wait(timeout=loop_poll_seconds)
                    continue
                if pending_signal_id:
                    confirmation = _pop_cdp_execution_confirmation(shared, pending_signal_id)
                    if confirmation is None:
                        confirmation = _consume_custom_human_terminal_inflight_confirmation(
                            shared,
                            pending_signal_id,
                        )
                    if confirmation is None:
                        confirmation = _synthesize_custom_human_inflight_confirmation_from_snapshot(
                            shared,
                            current_state,
                        )
                    if confirmation is None:
                        shared["live_state"] = current_state
                        shared["live_observer_status"] = str(
                            current_state.get("last_note", "Afventer broker-bekræftelse...")
                        ).strip() or "Afventer broker-bekræftelse..."
                        _persist_custom_human_runtime_state(shared)
                        stop_event.wait(timeout=max(0.5, min(5.0, loop_poll_seconds)))
                        continue

                    # Fix 1: Snapshot-rescue for failed add verifications.
                    # Tradovate DOM kan have lag – adden gik igennem men vi fik
                    # ikke 2 consecutive snapshots inden for timeout.  Tjek
                    # live snapshot én gang til før vi accepterer "failed".
                    confirmation = _rescue_failed_add_confirmation_from_snapshot(
                        shared,
                        current_state,
                        confirmation,
                        runtime_profile,
                    )

                    position_was_open_before_confirmation = bool(current_state.get("position_open", False))
                    current_state = _apply_custom_human_live_confirmation(
                        state_raw=current_state,
                        confirmation=confirmation,
                    )
                    shared["live_state"] = current_state
                    cleared_inflight = _clear_custom_human_inflight_order(
                        shared,
                        pending_signal_id,
                        persist=False,
                    )
                    shared["live_last_confirmation"] = confirmation
                    shared["live_observer_status"] = str(
                        current_state.get("last_note", "Broker-bekræftelse behandlet.")
                    ).strip() or "Broker-bekræftelse behandlet."
                    _persist_custom_human_runtime_state(shared)

                    # Auto-release ALL idempotency keys on confirmed exit, then
                    # hard-reset in-trade metadata so the next setup starts clean.
                    #
                    # Bug that was here: the original entry-key release did NOT pass
                    # bar_index, so the SHA-256 hash never matched the stored key
                    # (which was built WITH bar_index when entry_bar_index was set).
                    # Additionally, add_1 / add_2 keys were never released at all.
                    _conf_status = str(confirmation.get("status", "")).strip()
                    _conf_ok = bool(confirmation.get("confirmed", False))
                    if _conf_status == "confirmed_flat" and _conf_ok and not bool(current_state.get("reconcile_required", False)):
                        _rg = getattr(pipeline, "risk_gate", None)
                        if _rg is not None and hasattr(_rg, "release_idempotency_key"):
                            _ik_strat = str(
                                current_state.get("strategy_name", "")
                                or runtime_profile.get("strategy_name", "")
                                or ""
                            ).strip()
                            _ik_instr = str(current_state.get("instrument", "") or "").strip()
                            _ik_dir = str(current_state.get("direction", "") or "").strip()
                            _ik_tdate = str(current_state.get("trade_date", "") or "").strip()
                            # bar_index was embedded in the key when it was built —
                            # must pass it here so the SHA-256 hash matches.
                            _ik_bar = (
                                int(current_state["entry_bar_index"])
                                if current_state.get("entry_bar_index") is not None
                                else None
                            )
                            # max_add_to_winners controls how many ADD slots exist.
                            _max_adds = int(
                                runtime_profile.get("max_add_to_winners") or 0
                            )
                            if _ik_strat and _ik_instr and _ik_dir and _ik_tdate:
                                try:
                                    _released_keys: list[str] = []
                                    # 1. Entry key (fixed: now includes bar_index)
                                    if _rg.release_idempotency_key(
                                        strategy=_ik_strat,
                                        instrument=_ik_instr,
                                        direction=_ik_dir,
                                        trade_date=_ik_tdate,
                                        suffix="entry",
                                        bar_index=_ik_bar,
                                    ):
                                        _released_keys.append("entry")
                                    # 2. ADD keys — release add_1 … add_N
                                    for _add_n in range(1, max(3, _max_adds) + 1):
                                        if _rg.release_idempotency_key(
                                            strategy=_ik_strat,
                                            instrument=_ik_instr,
                                            direction=_ik_dir,
                                            trade_date=_ik_tdate,
                                            suffix=f"add_{_add_n}",
                                            bar_index=_ik_bar,
                                        ):
                                            _released_keys.append(f"add_{_add_n}")
                                    if _released_keys:
                                        _append_custom_human_diagnostic_event(
                                            shared,
                                            kind="observer",
                                            headline="ALLE LÅS FRIGIVET",
                                            detail=(
                                                f"Position lukket ({_ik_instr} {_ik_dir}) – "
                                                f"nøgler renset: {', '.join(_released_keys)}. "
                                                "Klar til ny entry."
                                            ),
                                            tone="active",
                                        )
                                        _serialize_custom_human_risk_gate_state(pipeline, shared)
                                        _APP_LOGGER.info(
                                            "[GHOST] confirmed_flat → idempotency-nøgler frigivet: "
                                            "%s/%s/%s/%s — %s",
                                            _ik_strat, _ik_instr, _ik_dir, _ik_tdate,
                                            _released_keys,
                                        )
                                except Exception as _ik_exc:
                                    _APP_LOGGER.warning(
                                        "Idempotency auto-release på confirmed_flat fejlede (non-critical): %s",
                                        _ik_exc,
                                    )

                        _release_custom_human_router_cycle_for_flat_position(
                            shared,
                            router=router,
                            position_key=str((cleared_inflight or {}).get("position_key", "") or ""),
                            reset_context="confirmed_flat",
                        )

                        # Hard-reset all in-trade metadata and transition phase to
                        # waiting_for_setup so the next bar evaluation starts clean.
                        current_state = _post_flat_hard_reset(current_state)
                        shared["live_state"] = current_state
                        _persist_custom_human_runtime_state(shared)
                        _APP_LOGGER.info(
                            "[GHOST] confirmed_flat → hard reset gennemført. "
                            "Phase: waiting_for_setup. Klar til næste trade."
                        )

                    if bool(current_state.get("reconcile_required", False)):
                        # Fix 4: Prøv auto-recovery — ryd reconcile hvis snapshot
                        # viser flat position (add fejlede, broker er allerede ren).
                        # _clear_stale_custom_human_reconcile_from_snapshot returnerer
                        # True KUN hvis snapshot er tilgængeligt og position_open=False.
                        if _clear_stale_custom_human_reconcile_from_snapshot(
                            shared,
                            reset_context="auto-recovery i observer-loop",
                        ):
                            current_state = _coerce_custom_human_live_state(shared.get("live_state"))
                            _append_custom_human_diagnostic_event(
                                shared,
                                kind="observer",
                                headline="RECONCILE AUTO-RYDDET",
                                detail=(
                                    "Snapshot viser flat position – reconcile-blokering ophævet automatisk. "
                                    "Klar til nye entries."
                                ),
                                tone="active",
                            )
                            _persist_custom_human_runtime_state(shared)
                            stop_event.wait(timeout=loop_poll_seconds)
                            continue
                        # Fix 3: Selv under reconcile-blokering tjekker vi om
                        # stop/trail er ramt i real-time.  Exits skal altid
                        # kunne gå igennem — ellers sidder positionen åben
                        # uden styring mens vi venter på manuel afklaring.
                        _r_state, _r_cand = _custom_human_snapshot_stop_cross_candidate(
                            state_raw=current_state,
                            snapshot_raw=shared.get("tradovate_snapshot"),
                            observed_at=observed_at,
                        )
                        if _r_cand is not None:
                            # Stop ramt — ophæv reconcile og lad exit-dispatchen
                            # nedenfor håndtere det (snapshot_stop_cross_candidate
                            # vil returnere samme kandidat igen i normal flow).
                            current_state = dict(_r_state)
                            current_state["reconcile_required"] = False
                            shared["live_state"] = current_state
                            _append_custom_human_diagnostic_event(
                                shared,
                                kind="observer",
                                headline="NØDUDGANG UNDER RECONCILE",
                                detail=(
                                    f"Snapshot-stop ramt under reconcile-blokering – exit frigøres. "
                                    f"({_r_cand.get('exit_reason', '')})"
                                ),
                                tone="warning",
                            )
                            _persist_custom_human_runtime_state(shared)
                            # Fall through — normal stop-cross dispatch køres nu.
                        else:
                            stop_event.wait(timeout=loop_poll_seconds)
                            continue
                    confirmation_opened_position = (
                        not position_was_open_before_confirmation
                        and bool(current_state.get("position_open", False))
                    )
                    if confirmation_opened_position:
                        stop_event.wait(timeout=loop_poll_seconds)
                        continue

                watchdog = _custom_human_watchdog_snapshot(shared)
                _record_custom_human_watchdog_diagnostic(shared)
                if bool(watchdog.get("block_new_entries", False)) and not bool(current_state.get("position_open", False)):
                    shared["live_state"] = current_state
                    shared["live_observer_status"] = (
                        f"Watchdog aktiv · {str(watchdog.get('detail', '')).strip()}"
                    ).strip()
                    _persist_custom_human_runtime_state(shared)
                    stop_event.wait(timeout=loop_poll_seconds)
                    continue

                # Management config for intra-bar trail + snapshot adds.
                # Computed once per loop iteration from the live execution model
                # and strategy so trail/add thresholds stay in sync with bar eval.
                _snap_strategy = str(runtime_profile.get("strategy_name") or "School Run").strip() or "School Run"
                _snap_mgmt = _management_config_for_model(
                    execution_model=_custom_human_execution_model_id(str(cfg.execution_model)),
                    strategy_name=_snap_strategy,
                )

                # 1. Real-time stop/trail check (always runs, updates active_stop intra-bar).
                state, candidate = _custom_human_snapshot_stop_cross_candidate(
                    state_raw=current_state,
                    snapshot_raw=shared.get("tradovate_snapshot"),
                    observed_at=observed_at,
                    management_config=_snap_mgmt,
                )
                # 2. Intra-bar add (AGGRESSIVE only, exit always has priority).
                if candidate is None:
                    state, candidate = _custom_human_snapshot_add_candidate(
                        state_raw=state,
                        execution_model=str(cfg.execution_model),
                        max_add_to_winners=int(runtime_profile.get("max_add_to_winners", 0)),
                        management_config=_snap_mgmt,
                        observed_at=observed_at,
                    )
                # 3. Bar-close evaluation (entries, session-close, non-aggressive adds).
                if candidate is None:
                    state, candidate = _evaluate_custom_human_live_state(
                        day_bars=day_bars,
                        cfg=cfg,
                        state_raw=state,
                        observed_at=observed_at,
                        max_add_to_winners=int(runtime_profile.get("max_add_to_winners", 0)),
                        session_close_dk=dynamic_session_close_dk,
                    )
                shared["live_state"] = state
                shared["live_market_meta"] = market_meta

                if candidate is None:
                    shared["live_observer_status"] = str(state.get("last_note", "Afventer live signal..."))
                    _persist_custom_human_runtime_state(shared)
                    stop_event.wait(timeout=loop_poll_seconds)
                    continue

                if _custom_human_watchdog_blocks_candidate(watchdog, candidate):
                    shared["live_state"] = state
                    shared["live_observer_status"] = (
                        f"{str(candidate.get('event', '')).upper()} blokeret af watchdog · "
                        f"{str(watchdog.get('detail', '')).strip()}"
                    ).strip()
                    _append_custom_human_diagnostic_event(
                        shared,
                        kind="observer",
                        headline=f"{str(candidate.get('event', '')).upper()} blokeret af watchdog",
                        detail=str(watchdog.get("detail", "")).strip(),
                        tone="warning",
                    )
                    _persist_custom_human_runtime_state(shared)
                    stop_event.wait(timeout=loop_poll_seconds)
                    continue

                # Fix 2b: Skip re-dispatch of a suppressed signal on the same bar.
                # When the engine restarts mid-session and sees a signal that was already
                # handled (restart_suppressed_signal / duplicate signal_id), it marks the
                # (event, bar_index) pair as suppressed.  On the next poll the same
                # candidate would be re-presented from the queue; we skip it here so the
                # observer doesn't spam the router 10× per second for the rest of the bar.
                # The suppression is cleared automatically once a new bar arrives.
                _sup_key = shared.get("_suppressed_signal_key")
                _cur_key = (str(candidate.get("event", "")), candidate.get("bar_index"))
                if _sup_key is not None:
                    if _sup_key[1] != _cur_key[1]:
                        # New bar → clear the stale suppression so fresh signals are dispatched
                        shared.pop("_suppressed_signal_key", None)
                    elif _sup_key == _cur_key:
                        # Same bar, same event → already suppressed; wait and try next tick
                        stop_event.wait(timeout=loop_poll_seconds)
                        continue

                signal_item = _build_custom_human_live_signal(
                    candidate=candidate,
                    runtime_profile=runtime_profile,
                )
                state = _reserve_custom_human_pending_dispatch(
                    shared=shared,
                    state_raw=state,
                    candidate=candidate,
                    signal_item=signal_item,
                    observed_at=observed_at,
                )
                shared["live_observer_status"] = (
                    f"{str(candidate.get('event', '')).upper()} reserveret · dispatch til broker i gang."
                ).strip()
                try:
                    dispatch_row, _ = _process_custom_human_signal_via_engine(
                        signal_item=signal_item,
                        runtime_profile=runtime_profile,
                        pipeline=pipeline,
                        router=router,
                        live_cfg=live_cfg,
                        kill_switch=kill_switch,
                        now=observed_at,
                    )
                except Exception as exc:
                    state = _rollback_custom_human_pending_dispatch(
                        shared=shared,
                        state_raw=state,
                        signal_item=signal_item,
                        note=(
                            f"{str(candidate.get('event', '')).upper()} dispatch fejlede – reservation ryddet. {exc}"
                        ).strip(),
                    )
                    raise
                shared["live_last_dispatch"] = dispatch_row
                if dispatch_row.get("status") == "queued_to_cdp":
                    _upsert_custom_human_inflight_order(
                        shared,
                        signal_item,
                        status="queued",
                        observed_at=observed_at,
                        message=str(dispatch_row.get("message", "") or "Signal queued til CDP worker."),
                        persist=True,
                    )
                    shared["live_observer_status"] = (
                        f"{str(candidate.get('event', '')).upper()} queued · afventer broker-bekræftelse."
                    ).strip()
                    _append_custom_human_diagnostic_event(
                        shared,
                        kind="observer",
                        headline=f"{str(candidate.get('event', '')).upper()} queued",
                        detail=str(dispatch_row.get("message", "") or "Afventer broker-bekræftelse."),
                        tone="active",
                    )
                else:
                    state = _rollback_custom_human_pending_dispatch(
                        shared=shared,
                        state_raw=state,
                        signal_item=signal_item,
                        note=(
                            f"{str(candidate.get('event', '')).upper()} blokeret – reservation ryddet."
                        ).strip(),
                    )
                    shared["live_observer_status"] = (
                        f"{str(candidate.get('event', '')).upper()} blokeret · {dispatch_row.get('message', '')}"
                    ).strip()
                    # Idempotency / recovery blocks are expected/normal – use 'inactive' tone so
                    # they don't pollute the dashboard with alarming orange warnings.
                    # Covers: (a) RiskGate duplicate-key blocks, (b) restart_suppressed_signal
                    # recovery windows, and (c) router-level "duplicate signal_id" dedup.
                    _dispatch_status = str(dispatch_row.get("status", "")).lower()
                    _dispatch_msg = str(dispatch_row.get("message", "")).lower()
                    _block_is_idempotency = (
                        _dispatch_status in {"blocked_risk_gate", "restart_suppressed_signal"}
                        or "duplicate trade intent" in _dispatch_msg
                        or "duplicate signal_id" in _dispatch_msg
                    )
                    # Fix 2b: Remember the (event, bar_index) tuple that was suppressed so the
                    # observer can skip re-dispatching it on the very next poll of the same bar.
                    if _dispatch_status == "restart_suppressed_signal" or "duplicate signal_id" in _dispatch_msg:
                        shared["_suppressed_signal_key"] = (
                            str(candidate.get("event", "")),
                            candidate.get("bar_index"),
                        )
                    _block_tone = "inactive" if _block_is_idempotency else "warning"
                    _append_custom_human_diagnostic_event(
                        shared,
                        kind="observer",
                        headline=f"{str(candidate.get('event', '')).upper()} blokeret",
                        detail=str(dispatch_row.get("message", "") or ""),
                        tone=_block_tone,
                    )
                _persist_custom_human_runtime_state(shared)
            except Exception as exc:
                shared["live_observer_status"] = f"Live observer fejl: {exc}"
                _append_custom_human_diagnostic_event(
                    shared,
                    kind="observer",
                    headline="LIVE OBSERVER FEJL",
                    detail=str(exc),
                    tone="warning",
                )
                _persist_custom_human_runtime_state(shared)

            stop_event.wait(timeout=loop_poll_seconds)
    finally:
        shared["live_observer_running"] = False
        _persist_custom_human_runtime_state(shared)


_CDP_AUTO_TRADE_SINGLETON_ATTR = "_research_tom_cdp_auto_trade_shared"


def _build_cdp_auto_trade_shared_state() -> dict[str, Any]:
    state: dict[str, Any] = {
        "running": False,
        "stop_event": None,       # threading.Event set by the UI to stop the loop
        "auto_requested": False,
        "last_stop_reason": "",
        "auto_restart_attempts": 0,
        "supervisor_running": False,
        "supervisor_stop_event": None,
        "supervisor_last_reason": "",
        "connected": False,
        "runtime_active": False,
        "runtime_profile": {},
        "runtime_config": {},
        "adapter": None,
        "signal_queue": queue.Queue(),
        "buy_info": None,         # dict with selector / x / y for the Buy button
        "sell_info": None,        # dict with selector / x / y for the Sell button
        "flat_info": None,        # dict with selector / x / y for the flatten button
        "last_result": "",        # last status message for display in the UI
        "lock": threading.RLock(),  # guards non-queue shared status fields when needed
        "execution_confirmations": {},
        "inflight_orders": {},
        "expected_account_tokens": (),
        "persisted_gate_state": None,
        "runtime_state_loaded": False,
        "live_feed_cache": None,
        "live_feed_cache_fetched_at": "",
        "tradovate_snapshot": None,
        "tradovate_snapshot_status": "",
        "tradovate_snapshot_running": False,
        "tradovate_snapshot_cfg": None,
        "tradovate_price_samples": [],
        "tradovate_15m_bars": [],
        "tradovate_bar_builder_status": "",
        "diagnostics_events": [],
        "diagnostic_markers": {},
        "last_worker_interaction_at": None,
        "idle_health_check_next_at": None,
        "last_auto_recovered_at": None,
        "bio_polling_enabled": False,
        "polling_phase_offset": None,
    }
    # Mistrust Factor buffers (threshold=3 by default, configurable).
    # _position_buffer and _account_buffer are the canonical names;
    # position_side_verification_buffer is a legacy alias pointing at the same object.
    _pos_buf = StateVerificationBuffer(threshold=STATE_VERIFICATION_REQUIRED_CONFIRMATIONS, name="position")
    _acc_buf = StateVerificationBuffer(threshold=STATE_VERIFICATION_REQUIRED_CONFIRMATIONS, name="account")
    state["_position_buffer"] = _pos_buf
    state["_account_buffer"] = _acc_buf
    state["position_side_verification_buffer"] = _pos_buf  # legacy alias
    return state


def _custom_human_shared_lock(shared: dict[str, Any]) -> Any:
    lock = shared.get("lock") if isinstance(shared, dict) else None
    if lock is not None and hasattr(lock, "acquire") and hasattr(lock, "release"):
        return lock
    return None


def _get_cdp_auto_trade_shared_singleton() -> dict[str, Any]:
    shared = getattr(builtins, _CDP_AUTO_TRADE_SINGLETON_ATTR, None)
    if not isinstance(shared, dict):
        shared = _build_cdp_auto_trade_shared_state()
        setattr(builtins, _CDP_AUTO_TRADE_SINGLETON_ATTR, shared)

    shared.setdefault("running", False)
    shared.setdefault("stop_event", None)
    shared.setdefault("auto_requested", False)
    shared.setdefault("last_stop_reason", "")
    shared.setdefault("auto_restart_attempts", 0)
    shared.setdefault("supervisor_running", False)
    shared.setdefault("supervisor_stop_event", None)
    shared.setdefault("supervisor_last_reason", "")
    shared.setdefault("connected", False)
    shared.setdefault("runtime_active", False)
    shared.setdefault("runtime_profile", {})
    shared.setdefault("runtime_config", {})
    shared.setdefault("adapter", None)
    signal_queue = shared.get("signal_queue")
    if signal_queue is None or not hasattr(signal_queue, "get") or not hasattr(signal_queue, "put"):
        shared["signal_queue"] = queue.Queue()
    shared.setdefault("buy_info", None)
    shared.setdefault("sell_info", None)
    shared.setdefault("flat_info", None)
    shared.setdefault("last_result", "")
    shared.setdefault("expected_account_tokens", ())
    shared.setdefault("persisted_gate_state", None)
    shared.setdefault("runtime_state_loaded", False)
    shared.setdefault("live_feed_cache", None)
    shared.setdefault("live_feed_cache_fetched_at", "")
    shared.setdefault("tradovate_snapshot", None)
    shared.setdefault("tradovate_snapshot_status", "")
    shared.setdefault("tradovate_snapshot_running", False)
    shared.setdefault("tradovate_snapshot_cfg", None)
    shared.setdefault("tradovate_price_samples", [])
    shared.setdefault("tradovate_15m_bars", [])
    shared.setdefault("tradovate_bar_builder_status", "")
    shared.setdefault("diagnostics_events", [])
    shared.setdefault("diagnostic_markers", {})
    shared.setdefault("last_worker_interaction_at", None)
    shared.setdefault("idle_health_check_next_at", None)
    shared.setdefault("last_auto_recovered_at", None)
    shared.setdefault("bio_polling_enabled", False)
    shared.setdefault("polling_phase_offset", None)
    if not isinstance(shared.get("_position_buffer"), StateVerificationBuffer):
        shared["_position_buffer"] = StateVerificationBuffer(
            threshold=STATE_VERIFICATION_REQUIRED_CONFIRMATIONS, name="position"
        )
    if not isinstance(shared.get("_account_buffer"), StateVerificationBuffer):
        shared["_account_buffer"] = StateVerificationBuffer(
            threshold=STATE_VERIFICATION_REQUIRED_CONFIRMATIONS, name="account"
        )
    # Legacy alias: position_side_verification_buffer → _position_buffer
    shared["position_side_verification_buffer"] = shared["_position_buffer"]
    lock_obj = shared.get("lock")
    if lock_obj is None or not hasattr(lock_obj, "acquire") or not hasattr(lock_obj, "release"):
        shared["lock"] = threading.RLock()
    confirmations = shared.get("execution_confirmations")
    if not isinstance(confirmations, dict):
        shared["execution_confirmations"] = {}
    inflight_orders = shared.get("inflight_orders")
    if not isinstance(inflight_orders, dict):
        shared["inflight_orders"] = {}
    if not bool(shared.get("runtime_state_loaded", False)):
        try:
            _restore_custom_human_runtime_state_into_shared(shared)
        except Exception as exc:  # noqa: BLE001
            shared["runtime_state_loaded"] = False
            _APP_LOGGER.exception(
                "Custom Human runtime restore failed during shared singleton init."
            )
            _capture_startup_snapshot(
                "custom_human_runtime_restore_failed",
                exc,
                shared=shared,
            )
            raise
    return shared



def _custom_human_supervisor_guardrail_status(shared: dict[str, Any]) -> tuple[bool, str, str]:
    if not bool(shared.get("auto_requested", False)):
        return False, "not_requested", "Auto er ikke requested."
    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    phase = str(live_state.get("phase", "") or "").strip().lower()
    if bool(live_state.get("reconcile_required", False)) or phase == "manual_reconcile":
        return False, "manual_reconcile", "Manual reconcile kræver brugerindgriben."
    adapter = shared.get("adapter")
    connected = bool(shared.get("connected", False))
    if isinstance(adapter, CDPHumanAdapter):
        connected = bool(connected and adapter.is_connected)
    else:
        connected = False
    if not connected:
        return False, "no_chrome", "Chrome/CDP er ikke forbundet."
    if not bool(shared.get("runtime_active", False)):
        return False, "no_runtime", "Ingen aktiv Custom Human runtime-profil er armed."
    if not (shared.get("buy_info") or shared.get("sell_info")):
        return False, "targets_missing", "Ingen validerede auto-targets findes endnu."
    return True, "ok", ""


def _start_custom_human_runtime_components(
    adapter: CDPHumanAdapter,
    shared: dict[str, Any],
    *,
    reset_signal_queue: bool,
) -> dict[str, Any]:
    stop_event = threading.Event()
    shared["running"] = True
    shared["stop_event"] = stop_event
    # Fix 1: Reset snapshot to a "reconnecting" sentinel so the UI never shows stale
    # bid/ask prices from a previous session after the supervisor restarts components.
    # The live-data thread will overwrite this as soon as a fresh quote arrives.
    shared["tradovate_snapshot"] = {
        "connected": False,
        "account_ok": False,
        "instrument_visible": False,
        "bid_price": None,
        "ask_price": None,
        "quote_ready": False,
        "observed_at": datetime.now(tz=APP_TIMEZONE).isoformat(),
        "source": "reconnect_sentinel",
    }
    if reset_signal_queue:
        _reset_cdp_signal_queue()
    shared["last_result"] = "Klar – venter på signal..."
    restored_live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    restored_live_state, stale_reconcile_reset = _reset_stale_custom_human_reconcile_on_start(
        state_raw=restored_live_state,
        snapshot_raw=shared.get("tradovate_snapshot"),
        trade_date=datetime.now(APP_TIMEZONE).strftime("%Y-%m-%d"),
    )
    shared["live_state"] = restored_live_state
    shared["live_observer_status"] = str(restored_live_state.get("last_note", "") or "").strip()
    if stale_reconcile_reset:
        shared["live_last_confirmation"] = None
        _persist_custom_human_runtime_state(shared)
    shared["live_market_meta"] = (
        dict(shared.get("live_market_meta"))
        if isinstance(shared.get("live_market_meta"), dict)
        else {}
    )
    shared["live_last_dispatch"] = (
        dict(shared.get("live_last_dispatch"))
        if isinstance(shared.get("live_last_dispatch"), dict)
        else None
    )
    shared["live_last_confirmation"] = (
        dict(shared.get("live_last_confirmation"))
        if isinstance(shared.get("live_last_confirmation"), dict)
        else None
    )
    shared["live_feed_cache"] = None
    shared["live_feed_cache_fetched_at"] = ""
    shared["execution_confirmations"] = {}
    shared["tradovate_snapshot_running"] = False
    _seed_custom_human_bio_polling_profile(shared, force_new=True)

    worker_thread = threading.Thread(
        target=_cdp_auto_trade_loop,
        args=(adapter, stop_event, shared),
        daemon=True,
        name="cdp-auto-trade",
    )
    worker_thread.start()

    observer_label = "Klik-worker only"
    observer_snapshot = shared.get("live_observer_cfg")
    if isinstance(observer_snapshot, dict) and bool(observer_snapshot.get("enabled")):
        shared["live_observer_running"] = True
        live_thread = threading.Thread(
            target=_custom_human_live_observer_loop,
            args=(stop_event, shared),
            daemon=True,
            name="cdp-live-school-run",
        )
        live_thread.start()
        observer_label = "School Run live observer aktiv"
    elif isinstance(observer_snapshot, dict):
        shared["live_observer_status"] = str(observer_snapshot.get("message", "")).strip()
        observer_label = "Live observer ikke startet"
    else:
        shared["live_observer_cfg"] = None

    tradovate_snapshot_label = "Tradovate snapshot manuel"
    try:
        _refresh_custom_human_tradovate_snapshot_health(
            adapter,
            shared,
        )
        tradovate_snapshot_label = "Tradovate snapshot aktiv"
    except Exception as exc:
        _APP_LOGGER.warning("Read-only Tradovate snapshot refresh fejlede ved runtime-start: %s", exc)
        shared["tradovate_snapshot_status"] = f"Read-only snapshot fejl: {exc}"

    shared["tradovate_snapshot_running"] = True
    tradovate_thread = threading.Thread(
        target=_custom_human_tradovate_snapshot_loop,
        args=(stop_event, shared, adapter),
        daemon=True,
        name="cdp-tradovate-snapshot",
    )
    tradovate_thread.start()
    return {
        "stop_event": stop_event,
        "observer_label": observer_label,
        "snapshot_label": tradovate_snapshot_label,
    }


def _stop_custom_human_auto_runtime(
    shared: dict[str, Any],
    *,
    user_initiated: bool,
    reason: str,
) -> None:
    stop_event = shared.get("stop_event")
    if isinstance(stop_event, threading.Event):
        stop_event.set()
    shared["running"] = False
    shared["stop_event"] = None
    if user_initiated:
        shared["auto_requested"] = False
        shared["last_stop_reason"] = reason
        supervisor_stop = shared.get("supervisor_stop_event")
        if isinstance(supervisor_stop, threading.Event):
            supervisor_stop.set()
        shared["supervisor_running"] = False
        shared["supervisor_stop_event"] = None
    _reset_cdp_signal_queue()
    shared["last_result"] = "Stoppet af bruger." if user_initiated else str(reason or "Auto stoppet.")
    shared["live_observer_status"] = "Stoppet af bruger." if user_initiated else str(reason or "Auto stoppet.")
    shared["live_observer_running"] = False
    shared["tradovate_snapshot_status"] = "Stoppet af bruger." if user_initiated else str(reason or "Auto stoppet.")
    shared["tradovate_snapshot_running"] = False
    shared["expected_account_tokens"] = ()
    shared["execution_confirmations"] = {}
    shared["live_feed_cache"] = None
    shared["live_feed_cache_fetched_at"] = ""
    shared["bio_polling_enabled"] = False
    _sanitize_custom_human_runtime_after_stop(shared, reason=reason)
    _persist_custom_human_runtime_state(shared)


def _disable_invalid_custom_human_auto_request(
    shared: dict[str, Any],
    *,
    reason: str,
) -> None:
    _stop_custom_human_auto_runtime(shared, user_initiated=False, reason=reason)
    shared["auto_requested"] = False
    shared["last_stop_reason"] = str(reason or "Invalid runtime.")
    _persist_custom_human_runtime_state(shared)


def _custom_human_supervisor_tick(shared: dict[str, Any]) -> dict[str, Any]:
    _clear_stale_custom_human_reconcile_from_snapshot(
        shared,
        reset_context="supervisor health check",
    )
    ok, reason, detail = _custom_human_supervisor_guardrail_status(shared)
    shared["supervisor_last_reason"] = str(reason or "")
    if not ok:
        if reason == "no_runtime":
            _disable_invalid_custom_human_auto_request(
                shared,
                reason=str(detail or "Ingen aktiv Custom Human runtime-profil er armed."),
            )
            return {
                "ok": False,
                "reason": "invalid_runtime",
                "detail": str(detail or "Ingen aktiv Custom Human runtime-profil er armed."),
            }
        return {"ok": False, "reason": reason, "detail": detail}

    adapter = shared.get("adapter")
    if not isinstance(adapter, CDPHumanAdapter):
        return {"ok": False, "reason": "no_adapter", "detail": "Ingen adapter til supervisor."}

    observer_cfg = shared.get("live_observer_cfg")
    observer_enabled = bool(isinstance(observer_cfg, dict) and observer_cfg.get("enabled"))
    runtime_restarted = False

    stop_event = shared.get("stop_event")
    stop_event_set = bool(isinstance(stop_event, threading.Event) and stop_event.is_set())
    if not bool(shared.get("running", False)) or stop_event_set:
        _start_custom_human_runtime_components(adapter, shared, reset_signal_queue=False)
        shared["auto_restart_attempts"] = int(shared.get("auto_restart_attempts", 0) or 0) + 1
        _mark_custom_human_recent_recovery(shared)
        _append_custom_human_diagnostic_event(
            shared,
            kind="system",
            headline="AUTO GENOPRETTET",
            detail="Supervisor genstartede worker, observer og snapshot.",
            tone="active",
        )
        _persist_custom_human_runtime_state(shared)
        return {"ok": True, "reason": "restarted_runtime", "detail": "Runtime genstartet."}

    if observer_enabled and not bool(shared.get("live_observer_running", False)):
        shared["live_observer_running"] = True
        live_thread = threading.Thread(
            target=_custom_human_live_observer_loop,
            args=(stop_event, shared),
            daemon=True,
            name="cdp-live-school-run",
        )
        live_thread.start()
        runtime_restarted = True

    if not bool(shared.get("tradovate_snapshot_running", False)):
        shared["tradovate_snapshot_running"] = True
        tradovate_thread = threading.Thread(
            target=_custom_human_tradovate_snapshot_loop,
            args=(stop_event, shared, adapter),
            daemon=True,
            name="cdp-tradovate-snapshot",
        )
        tradovate_thread.start()
        runtime_restarted = True

    if runtime_restarted:
        shared["auto_restart_attempts"] = int(shared.get("auto_restart_attempts", 0) or 0) + 1
        _append_custom_human_diagnostic_event(
            shared,
            kind="system",
            headline="AUTO GENOPRETTER DELLOOP",
            detail="Supervisor genstartede observer eller snapshot.",
            tone="active",
        )
        _persist_custom_human_runtime_state(shared)
        return {"ok": True, "reason": "restarted_partial", "detail": "Delloop genstartet."}

    return {"ok": True, "reason": "healthy", "detail": ""}


def _custom_human_auto_supervisor_loop(
    stop_event: threading.Event,
    shared: dict[str, Any],
) -> None:
    shared["supervisor_running"] = True
    try:
        while not stop_event.is_set():
            if not bool(shared.get("auto_requested", False)):
                break
            try:
                _custom_human_supervisor_tick(shared)
            except Exception as exc:
                _APP_LOGGER.exception("Custom Human supervisor loop fejlede: %s", exc)
                shared["last_result"] = f"⚠️ Supervisor-fejl: {exc}"
                _persist_custom_human_runtime_state(shared)
            stop_event.wait(timeout=0.5)
    finally:
        shared["supervisor_running"] = False
        if shared.get("supervisor_stop_event") is stop_event:
            shared["supervisor_stop_event"] = None


def _ensure_custom_human_supervisor_running(shared: dict[str, Any]) -> None:
    if not bool(shared.get("auto_requested", False)):
        return
    if bool(shared.get("supervisor_running", False)):
        return
    supervisor_stop_event = threading.Event()
    shared["supervisor_stop_event"] = supervisor_stop_event
    thread = threading.Thread(
        target=_custom_human_auto_supervisor_loop,
        args=(supervisor_stop_event, shared),
        daemon=True,
        name="cdp-auto-supervisor",
    )
    thread.start()






def _record_cdp_execution_confirmation(
    shared: dict[str, Any],
    signal_payload: dict[str, Any] | None,
    *,
    confirmed: bool,
    status: str,
    message: str,
    position_snapshot: dict[str, Any] | None = None,
) -> None:
    payload = signal_payload if isinstance(signal_payload, dict) else {}
    signal_id = str(payload.get("signal_id", "")).strip()
    if not signal_id:
        return
    confirmation = {
        "signal_id": signal_id,
        "signal": str(payload.get("signal", "")).strip().upper(),
        "event": str(payload.get("event", "")).strip().lower(),
        "instrument": str(payload.get("instrument", "")).strip().upper(),
        "confirmed": bool(confirmed),
        "status": str(status),
        "message": str(message),
        "position_snapshot": dict(position_snapshot) if isinstance(position_snapshot, dict) else None,
        "confirmed_at": datetime.now(tz=APP_TIMEZONE).isoformat(),
    }
    lock = shared.get("lock")
    if lock is not None and hasattr(lock, "acquire") and hasattr(lock, "release"):
        with lock:
            shared.setdefault("execution_confirmations", {})[signal_id] = confirmation
    else:
        shared.setdefault("execution_confirmations", {})[signal_id] = confirmation
    _append_custom_human_diagnostic_event(
        shared,
        kind="confirmation",
        headline=f"{confirmation['event'].upper() or 'SIGNAL'} {confirmation['status']}",
        detail=str(confirmation.get("message", "") or ""),
        tone="active" if confirmed else "warning",
    )


def _pop_cdp_execution_confirmation(shared: dict[str, Any], signal_id: str) -> dict[str, Any] | None:
    token = str(signal_id or "").strip()
    if not token:
        return None
    lock = shared.get("lock")
    confirmations = shared.get("execution_confirmations")
    if not isinstance(confirmations, dict):
        return None
    if lock is not None and hasattr(lock, "acquire") and hasattr(lock, "release"):
        with lock:
            confirmation = confirmations.pop(token, None)
    else:
        confirmation = confirmations.pop(token, None)
    return dict(confirmation) if isinstance(confirmation, dict) else None


def _broker_snapshot_confirmation_requirement() -> int:
    return max(1, int(CUSTOM_HUMAN_BROKER_CONFIRMATION_SNAPSHOTS))


def _custom_human_confirmation_snapshots_match(
    left: dict[str, Any] | None,
    right: dict[str, Any] | None,
) -> bool:
    left_payload = _normalize_custom_human_confirmation_snapshot(left)
    right_payload = _normalize_custom_human_confirmation_snapshot(right)
    if not isinstance(left_payload, dict) or not isinstance(right_payload, dict):
        return False
    return left_payload == right_payload



def _verify_cdp_signal_broker_state(
    adapter: "CDPHumanAdapter",
    signal_payload: dict[str, Any],
    stop_event: threading.Event,
    *,
    shared: dict[str, Any] | None = None,
    before_snapshot: dict[str, Any] | None = None,
    expected_account_tokens: tuple[str, ...] = (),
    timeout_s: float = CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS,
    poll_s: float = 0.5,
) -> dict[str, Any]:
    payload = dict(signal_payload) if isinstance(signal_payload, dict) else {}
    instrument = str(payload.get("instrument", "")).strip().upper()
    event = str(payload.get("event", "")).strip().lower()
    signal = str(payload.get("signal", "")).strip().upper()
    action = str(payload.get("action", "")).strip().lower()
    expected_flat = signal == "FLAT" or event == "exit"
    if not instrument:
        return {
            "confirmed": False,
            "status": "missing_instrument",
            "message": "Broker-state verification sprang over: instrument mangler i signalet.",
            "position_snapshot": None,
        }

    deadline = time_module.time() + max(0.5, float(timeout_s))
    last_snapshot: dict[str, Any] | None = None
    before_qty = _coerce_broker_snapshot_qty(before_snapshot)
    expected_order_qty = max(1.0, float(_safe_float(payload.get("quantity")) or 1.0))
    required_consecutive = _broker_snapshot_confirmation_requirement()
    consecutive_flat_matches = 0
    consecutive_open_matches = 0
    last_flat_match_snapshot: dict[str, Any] | None = None
    last_open_match_snapshot: dict[str, Any] | None = None
    while time_module.time() < deadline and not stop_event.is_set():
        try:
            snapshot = _run_cdp_adapter_task(
                adapter,
                lambda inst=instrument, tokens=list(expected_account_tokens): adapter.get_broker_state_snapshot(
                    inst,
                    expected_account_tokens=tokens,
                ),
            )
        except Exception as exc:
            return {
                "confirmed": False,
                "status": "verification_error",
                "message": f"Broker-state check fejlede: {exc}",
                "position_snapshot": None,
            }

        last_snapshot = dict(snapshot) if isinstance(snapshot, dict) else None
        position_qty = _coerce_broker_snapshot_qty(last_snapshot)
        has_position = position_qty > 0.0
        account_ok = bool(last_snapshot.get("account_ok", True)) if isinstance(last_snapshot, dict) else True
        instrument_ok = bool(last_snapshot.get("instrument_visible", False)) if isinstance(last_snapshot, dict) else False

        if expected_flat and not has_position:
            if consecutive_flat_matches > 0 and not _custom_human_confirmation_snapshots_match(
                last_flat_match_snapshot,
                last_snapshot,
            ):
                if shared is not None:
                    _log_custom_human_runtime_event(
                        shared,
                        headline="BROKER SNAPSHOT FLIMREDE",
                        detail=(
                            f"FLAT confirmation snapshots differed for {instrument}. "
                            + _format_custom_human_snapshot_pair_for_log(last_flat_match_snapshot, last_snapshot)
                        ),
                        tone="warning",
                        kind="confirmation",
                        level=logging.WARNING,
                    )
                consecutive_flat_matches = 1
            else:
                consecutive_flat_matches += 1
            last_flat_match_snapshot = last_snapshot
            if consecutive_flat_matches >= required_consecutive:
                return {
                    "confirmed": True,
                    "status": "confirmed_flat",
                    "message": (
                        f"{instrument} er ikke længere åben i positionspanelet "
                        f"({consecutive_flat_matches}/{required_consecutive} snapshots)."
                    ),
                    "position_snapshot": last_snapshot,
                }
        else:
            consecutive_flat_matches = 0
            last_flat_match_snapshot = None
        if not expected_flat and has_position and account_ok and instrument_ok:
            minimum_expected_qty = expected_order_qty if before_qty <= 0 else before_qty + expected_order_qty
            if action == "add":
                minimum_expected_qty = before_qty + expected_order_qty
            if position_qty >= max(1.0, minimum_expected_qty):
                if consecutive_open_matches > 0 and not _custom_human_confirmation_snapshots_match(
                    last_open_match_snapshot,
                    last_snapshot,
                ):
                    if shared is not None:
                        _log_custom_human_runtime_event(
                            shared,
                            headline="BROKER SNAPSHOT FLIMREDE",
                            detail=(
                                f"OPEN confirmation snapshots differed for {instrument}. "
                                + _format_custom_human_snapshot_pair_for_log(last_open_match_snapshot, last_snapshot)
                            ),
                            tone="warning",
                            kind="confirmation",
                            level=logging.WARNING,
                        )
                    consecutive_open_matches = 1
                else:
                    consecutive_open_matches += 1
                last_open_match_snapshot = last_snapshot
                if consecutive_open_matches >= required_consecutive:
                    status = "confirmed_add" if action == "add" else "confirmed_open"
                    message = (
                        f"{instrument} bekræftet med position {position_qty:g} "
                        f"({consecutive_open_matches}/{required_consecutive} snapshots)."
                        if action != "add"
                        else (
                            f"{instrument} add bekræftet – position steg til {position_qty:g} "
                            f"({consecutive_open_matches}/{required_consecutive} snapshots)."
                        )
                    )
                    return {
                        "confirmed": True,
                        "status": status,
                        "message": message,
                        "position_snapshot": last_snapshot,
                    }
            else:
                consecutive_open_matches = 0
                last_open_match_snapshot = None
        else:
            consecutive_open_matches = 0
            last_open_match_snapshot = None
        if not expected_flat and has_position and instrument_ok and not account_ok:
            return {
                "confirmed": False,
                "status": "account_mismatch_after_click",
                "message": "Broker-state viste position, men forventet konto-token matchede ikke efter klik.",
                "position_snapshot": last_snapshot,
            }
        if not expected_flat and has_position and account_ok and not instrument_ok:
            return {
                "confirmed": False,
                "status": "instrument_mismatch_after_click",
                "message": f"Broker-state viste position, men ikke forventet {instrument}-instrument efter klik.",
                "position_snapshot": last_snapshot,
            }
        stop_event.wait(timeout=max(0.1, float(poll_s)))

    if expected_flat:
        return {
            "confirmed": False,
            "status": "unconfirmed_flat",
            "message": (
                f"Kunne ikke bekræfte at {instrument} blev flattet med "
                f"{required_consecutive} ens snapshots inden for {float(timeout_s):.0f}s."
            ),
            "position_snapshot": last_snapshot,
        }
    return {
        "confirmed": False,
        "status": "unconfirmed_open",
        "message": (
            f"Kunne ikke bekræfte åben {instrument}-position med "
            f"{required_consecutive} ens snapshots inden for {float(timeout_s):.0f}s."
        ),
        "position_snapshot": last_snapshot,
    }


def _custom_human_tradovate_snapshot_price(snapshot_raw: dict[str, Any] | None) -> float | None:
    snapshot = _coerce_custom_human_tradovate_snapshot(snapshot_raw)
    if snapshot is None:
        return None
    last_price = _safe_float(snapshot.get("last_price"))
    if last_price is None:
        last_price = _safe_float(snapshot.get("last_price_text"))
    if last_price is not None:
        return float(last_price)
    bid_price = _safe_float(snapshot.get("bid_price"))
    if bid_price is None:
        bid_price = _safe_float(snapshot.get("bid_price_text"))
    ask_price = _safe_float(snapshot.get("ask_price"))
    if ask_price is None:
        ask_price = _safe_float(snapshot.get("ask_price_text"))
    if bid_price is not None and ask_price is not None:
        return float((bid_price + ask_price) / 2.0)
    if bid_price is not None:
        return float(bid_price)
    if ask_price is not None:
        return float(ask_price)
    return None


def _append_custom_human_tradovate_price_sample(
    shared: dict[str, Any],
    snapshot_raw: dict[str, Any] | None,
) -> dict[str, Any] | None:
    snapshot = _coerce_custom_human_tradovate_snapshot(snapshot_raw)
    if snapshot is None:
        return None
    observed_at = pd.to_datetime(snapshot.get("observed_at"), errors="coerce")
    if pd.isna(observed_at):
        return None
    observed_ts = pd.Timestamp(observed_at)
    if observed_ts.tzinfo is None:
        observed_ts = observed_ts.tz_localize(APP_TIMEZONE)
    else:
        observed_ts = observed_ts.tz_convert(APP_TIMEZONE)
    price = _custom_human_tradovate_snapshot_price(snapshot)
    if price is None:
        return None
    sample = {
        "observed_at": observed_ts.isoformat(),
        "price": float(price),
        "instrument_match": str(snapshot.get("instrument_match", "") or ""),
        "account_value": str(snapshot.get("account_value", "") or ""),
        "price_source": (
            "last"
            if _safe_float(snapshot.get("last_price")) is not None
            or _safe_float(snapshot.get("last_price_text")) is not None
            else "mid"
            if (
                (_safe_float(snapshot.get("bid_price")) is not None or _safe_float(snapshot.get("bid_price_text")) is not None)
                and (_safe_float(snapshot.get("ask_price")) is not None or _safe_float(snapshot.get("ask_price_text")) is not None)
            )
            else "bid"
            if _safe_float(snapshot.get("bid_price")) is not None or _safe_float(snapshot.get("bid_price_text")) is not None
            else "ask"
        ),
    }
    samples = _coerce_custom_human_tradovate_price_samples(shared.get("tradovate_price_samples"))
    if samples and samples[-1]["observed_at"] == sample["observed_at"]:
        samples[-1] = sample
    else:
        samples.append(sample)
    cutoff = (observed_ts - pd.Timedelta(hours=18)).to_pydatetime()
    filtered = [
        item
        for item in samples
        if pd.to_datetime(item.get("observed_at"), errors="coerce").to_pydatetime() >= cutoff
    ]
    shared["tradovate_price_samples"] = filtered[-15000:]
    return sample


def _rebuild_custom_human_tradovate_15m_bars(
    shared: dict[str, Any],
) -> list[dict[str, Any]]:
    samples = _coerce_custom_human_tradovate_price_samples(shared.get("tradovate_price_samples"))
    if not samples:
        shared["tradovate_15m_bars"] = []
        shared["tradovate_bar_builder_status"] = "Ingen Tradovate price samples endnu."
        return []

    frame = pd.DataFrame(samples)
    frame["observed_at"] = pd.to_datetime(frame["observed_at"], errors="coerce")
    frame = frame.dropna(subset=["observed_at", "price"]).sort_values("observed_at").reset_index(drop=True)
    if frame.empty:
        shared["tradovate_15m_bars"] = []
        shared["tradovate_bar_builder_status"] = "Ingen gyldige Tradovate price samples kunne bruges."
        return []

    frame["timestamp_dk"] = frame["observed_at"].apply(
        lambda value: pd.Timestamp(value).tz_localize(APP_TIMEZONE)
        if pd.Timestamp(value).tzinfo is None
        else pd.Timestamp(value).tz_convert(APP_TIMEZONE)
    )
    frame["bar_start"] = frame["timestamp_dk"].dt.floor("15min")
    frame["trade_date"] = frame["bar_start"].dt.normalize()

    grouped = (
        frame.groupby(["trade_date", "bar_start"], as_index=False)
        .agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            sample_count=("price", "count"),
            instrument_match=("instrument_match", "last"),
        )
        .sort_values(["trade_date", "bar_start"])
        .reset_index(drop=True)
    )
    grouped["bar_index"] = grouped.groupby("trade_date").cumcount() + 1

    bars: list[dict[str, Any]] = []
    for row in grouped.tail(64).itertuples(index=False):
        bars.append(
            {
                "trade_date": pd.Timestamp(row.trade_date).strftime("%Y-%m-%d"),
                "timestamp_dk": pd.Timestamp(row.bar_start).isoformat(),
                "bar_index": int(row.bar_index),
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
                "sample_count": int(row.sample_count),
                "instrument_match": str(row.instrument_match or ""),
            }
        )

    shared["tradovate_15m_bars"] = bars
    latest = bars[-1]
    shared["tradovate_bar_builder_status"] = (
        f"{len(bars)} bars bygget | seneste {latest['trade_date']} #{latest['bar_index']} "
        f"O:{latest['open']:.2f} H:{latest['high']:.2f} L:{latest['low']:.2f} C:{latest['close']:.2f}"
    )
    return bars





def _refresh_custom_human_tradovate_snapshot_health(
    adapter: "CDPHumanAdapter",
    shared: dict[str, Any],
    *,
    observer_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = dict(observer_cfg) if isinstance(observer_cfg, dict) else dict(shared.get("tradovate_snapshot_cfg") or {})
    instrument = str(cfg.get("instrument") or "MYM").strip()
    tokens = [str(token).strip() for token in cfg.get("expected_account_tokens", []) if str(token).strip()]
    snapshot_raw = _run_cdp_adapter_task(
        adapter,
        lambda inst=instrument, expected_tokens=tokens: adapter.get_tradovate_read_only_snapshot(
            inst,
            expected_tokens,
        ),
    )
    snapshot = _coerce_custom_human_tradovate_snapshot(snapshot_raw)
    if snapshot is None:
        raise RuntimeError("Tradovate snapshot returnerede intet læsbart payload.")

    # --- Mistrust Factor: StateVerificationBuffer ---
    # Both position_side and account_value must be seen 3 times in a row before
    # being committed to shared state, to prevent browser UI flicker from
    # triggering incorrect trade logic.

    # Position buffer (_position_buffer / position_side_verification_buffer alias)
    raw_position_side = str(snapshot.get("position_side", "") or "").strip().lower()
    pos_buf: StateVerificationBuffer | None = shared.get("_position_buffer")
    if isinstance(pos_buf, StateVerificationBuffer):
        confirmed = pos_buf.update(raw_position_side)
        if not confirmed and pos_buf.get_confirmed_state() is not _STATE_VERIFICATION_SENTINEL:
            # Override the freshly-read value with the last confirmed one
            snapshot = dict(snapshot)
            snapshot["position_side"] = str(pos_buf.get_confirmed_state())

    # Account-value buffer (_account_buffer)
    raw_account_value = str(snapshot.get("account_value", "") or "").strip()
    acc_buf: StateVerificationBuffer | None = shared.get("_account_buffer")
    if isinstance(acc_buf, StateVerificationBuffer):
        acc_confirmed = acc_buf.update(raw_account_value)
        if not acc_confirmed and acc_buf.get_confirmed_state() is not _STATE_VERIFICATION_SENTINEL:
            snapshot = dict(snapshot)
            snapshot["account_value"] = str(acc_buf.get_confirmed_state())
    # ------------------------------------------------

    shared["tradovate_snapshot"] = snapshot
    status = _custom_human_tradovate_snapshot_status(snapshot)
    shared["tradovate_snapshot_status"] = str(status.get("detail", "") or "")

    # --- Startup Inflight Deadlock Guard ---
    # If the engine loaded an inflight state from disk at startup but the
    # first live snapshot confirms no open position before the deadline, the
    # pending order can never be verified. Reset to FLAT so the system can
    # proceed without requiring manual intervention.
    startup_deadline = shared.get("_startup_inflight_deadline")
    if (
        isinstance(startup_deadline, (int, float))
        and time_module.time() > float(startup_deadline)
        and not bool(snapshot.get("position_open", False))
    ):
        live_state_raw = shared.get("live_state")
        live_state_check = _coerce_custom_human_live_state(live_state_raw)
        pending_at_startup = str(live_state_check.get("pending_signal_id", "") or "").strip()
        if pending_at_startup:
            _APP_LOGGER.warning(
                "[GHOST-V6.6] Startup inflight guard expired (%.1fs). "
                "Ingen åben position bekræftet via snapshot. Nulstiller til FLAT.",
                float(startup_deadline) - (time_module.time() - 8.0),
            )
            live_state_check["pending_signal_id"] = ""
            live_state_check["pending_event"] = ""
            live_state_check["pending_candidate"] = None
            live_state_check["position_open"] = False
            live_state_check["phase"] = "flat"
            live_state_check["last_note"] = (
                "Startup inflight-guard: ingen position fundet inden for fristen – nulstillet til FLAT."
            )
            shared["live_state"] = live_state_check
            shared["inflight_orders"] = {}
            shared["live_observer_status"] = live_state_check["last_note"]
            _append_custom_human_diagnostic_event(
                shared,
                kind="system",
                headline="STARTUP INFLIGHT RESET",
                detail=(
                    "Inflight-ordre fra disk kunne ikke bekræftes via CDP inden for 8s. "
                    "System nulstillet til FLAT automatisk."
                ),
                tone="warning",
            )
        # Disarm the deadline regardless so this only fires once
        shared.pop("_startup_inflight_deadline", None)
    # -----------------------------------------------

    _clear_stale_custom_human_reconcile_from_snapshot(
        shared,
        reset_context="frisk Tradovate snapshot",
    )
    _append_custom_human_tradovate_price_sample(shared, snapshot)
    _rebuild_custom_human_tradovate_15m_bars(shared)
    _record_custom_human_snapshot_diagnostic(shared, status)
    return snapshot


def _custom_human_tradovate_snapshot_loop(
    stop_event: threading.Event,
    shared: dict[str, Any],
    adapter: "CDPHumanAdapter",
) -> None:
    observer_cfg = shared.get("tradovate_snapshot_cfg")
    if not isinstance(observer_cfg, dict) or not bool(observer_cfg.get("enabled")):
        shared["tradovate_snapshot_running"] = False
        return

    poll_seconds = float(observer_cfg.get("poll_seconds", CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_POLL_SECONDS))
    shared["tradovate_snapshot_running"] = True
    try:
        while not stop_event.is_set():
            base_loop_poll_seconds = max(0.5, min(_custom_human_snapshot_poll_seconds(shared), poll_seconds))
            loop_poll_seconds = _custom_human_jittered_interval(base_loop_poll_seconds, floor=0.5)
            loop_poll_seconds = _apply_custom_human_bio_modulation(
                shared,
                loop_poll_seconds,
                floor=0.5,
            )
            if not bool(shared.get("running")):
                shared["tradovate_snapshot_status"] = "Tradovate snapshot stoppet fordi auto-worker ikke længere kører."
                _persist_custom_human_runtime_state(shared)
                break
            try:
                _refresh_custom_human_tradovate_snapshot_health(
                    adapter,
                    shared,
                    observer_cfg=observer_cfg,
                )
                _persist_custom_human_runtime_state(shared)
            except Exception as exc:
                shared["tradovate_snapshot"] = {
                    "connected": False,
                    "observed_at": datetime.now(tz=APP_TIMEZONE).isoformat(),
                    "account_ok": False,
                    "instrument_visible": False,
                    "quote_ready": False,
                    "page_title": "",
                    "page_url": "",
                }
                shared["tradovate_snapshot_status"] = f"Read-only snapshot fejl: {exc}"
                _append_custom_human_diagnostic_event(
                    shared,
                    kind="snapshot",
                    headline="TRADOVATE SNAPSHOT FEJL",
                    detail=str(exc),
                    tone="warning",
                )
                _persist_custom_human_runtime_state(shared)
            stop_event.wait(timeout=loop_poll_seconds)
    finally:
        shared["tradovate_snapshot_running"] = False
        _persist_custom_human_runtime_state(shared)


def _run_custom_human_signal_preflight(
    adapter: "CDPHumanAdapter",
    signal_payload: dict[str, Any] | None,
    shared: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(signal_payload) if isinstance(signal_payload, dict) else {}
    action = str(payload.get("action", "")).strip().lower()
    signal = str(payload.get("signal", "")).strip().upper()
    if signal == "FLAT" or action == "exit":
        return {"ok": True, "status": "bypass_exit", "message": "Exit bypasser quantity/account preflight."}

    if action not in {"buy", "sell", "add"}:
        return {"ok": True, "status": "skipped", "message": "Preflight sprang over for ikke-strategisk signal."}

    quantity = _safe_float(payload.get("quantity"))
    if quantity is not None and quantity > 0:
        qty_result = _run_cdp_adapter_task(adapter, lambda q=quantity: adapter.sync_order_quantity(q))
        if not isinstance(qty_result, dict) or not bool(qty_result.get("ok")):
            message = str((qty_result or {}).get("message", "Quantity preflight fejlede."))
            return {"ok": False, "status": "quantity_preflight_failed", "message": message}

    tokens = tuple(shared.get("expected_account_tokens") or ())
    if tokens:
        account_result = _run_cdp_adapter_task(adapter, lambda t=list(tokens): adapter.page_contains_tokens(t))
        if not isinstance(account_result, dict) or not bool(account_result.get("ok")):
            message = str((account_result or {}).get("message", "Account preflight fejlede."))
            return {"ok": False, "status": "account_preflight_failed", "message": message}

    preflight_notes: list[str] = []
    if quantity is not None and quantity > 0:
        preflight_notes.append(f"qty {int(max(1, round(float(quantity))))} klar")
    if tokens:
        preflight_notes.append(f"konto-token OK ({', '.join(tokens)})")
    return {
        "ok": True,
        "status": "preflight_ok",
        "message": " | ".join(preflight_notes) if preflight_notes else "Preflight OK.",
    }


def _cdp_auto_trade_loop(
    adapter: "CDPHumanAdapter",
    stop_event: threading.Event,
    shared: dict[str, Any],
    cooldown_s: float = 7.0,
) -> None:
    """Background trading loop that runs in its own thread.

    Consumes queued ``BUY`` / ``SELL`` / ``FLAT`` signals in FIFO order. When a signal
    arrives the appropriate button is clicked with human-like Bézier
    movements via CDP, then a cooldown pause is applied so the platform is
    not spammed.

    The loop exits when ``stop_event`` is set.
    """
    signal_queue = _ensure_cdp_signal_queue()
    try:
        while not stop_event.is_set():
            signal_payload: dict[str, Any] | None = None
            try:
                signal_payload = signal_queue.get(timeout=1.0)
            except queue.Empty:
                if _maybe_schedule_custom_human_idle_scroll_noise(shared):
                    try:
                        _run_cdp_adapter_task(adapter, adapter.perform_idle_scroll_noise)
                        shared["last_result"] = "🟢 Armed – afventer signal (idle scroll-noise udført)."
                    except Exception:
                        pass
                if _maybe_schedule_custom_human_idle_health_check(shared):
                    try:
                        _refresh_custom_human_tradovate_snapshot_health(
                            adapter,
                            shared,
                            observer_cfg=shared.get("tradovate_snapshot_cfg"),
                        )
                        shared["last_result"] = "🟢 Armed – idle health-check udført."
                        _persist_custom_human_runtime_state(shared)
                    except Exception as exc:
                        shared["last_result"] = f"⚠️ Armed – idle health-check fejlede: {exc}"
                stop_event.wait(timeout=0.1)
                continue

            signal = str((signal_payload or {}).get("signal", "")).strip().upper()
            signal_id = str((signal_payload or {}).get("signal_id", "")).strip()

            try:
                if signal in ("BUY", "SELL", "FLAT"):
                    info: dict[str, Any] | None
                    if signal == "BUY":
                        info = shared.get("buy_info")
                    elif signal == "SELL":
                        info = shared.get("sell_info")
                    else:
                        info = shared.get("flat_info")
                    if signal in ("BUY", "SELL") and info is None:
                        # Self-healing: DOM selector went missing (e.g. Tradovate module
                        # reloaded). Try re-scanning up to 3 times with brief backoff
                        # before giving up, so transient DOM flicker doesn't kill the
                        # trade loop.
                        _rescan_attempts = 3
                        _rescan_delays = (0.5, 1.0, 2.0)
                        for _attempt in range(_rescan_attempts):
                            stop_event.wait(timeout=_rescan_delays[_attempt])
                            if stop_event.is_set():
                                break
                            try:
                                _discovered = _run_cdp_adapter_task(
                                    adapter,
                                    adapter.auto_discover_trading_buttons,
                                )
                                if isinstance(_discovered, dict):
                                    if signal == "BUY" and _discovered.get("buy"):
                                        shared["buy_info"] = _discovered["buy"]
                                        info = _discovered["buy"]
                                    elif signal == "SELL" and _discovered.get("sell"):
                                        shared["sell_info"] = _discovered["sell"]
                                        info = _discovered["sell"]
                            except Exception as exc:
                                _APP_LOGGER.debug("[GHOST-V6.6] DOM auto-discovery self-heal fejlede: %s", exc)
                            if info is not None:
                                _APP_LOGGER.info(
                                    "[GHOST-V6.6] Self-heal: %s knap-info genfundet efter %d forsøg.",
                                    signal,
                                    _attempt + 1,
                                )
                                break
                        if info is None:
                            message = f"{signal}: knap-info mangler efter {_rescan_attempts} rescan-forsøg – springer over."
                            shared["last_result"] = f"⚠️ {message}"
                            _record_cdp_execution_confirmation(
                                shared,
                                signal_payload,
                                confirmed=False,
                                status="button_info_missing",
                                message=message,
                            )
                            continue

                    selector: str | None = info.get("selector") if isinstance(info, dict) else None
                    try:
                        preflight = _run_custom_human_signal_preflight(adapter, signal_payload, shared)
                        if not bool(preflight.get("ok", False)):
                            message = str(preflight.get("message", "Preflight fejlede.")).strip() or "Preflight fejlede."
                            shared["last_result"] = f"⚠️ {signal}: {message}"
                            _record_cdp_execution_confirmation(
                                shared,
                                signal_payload,
                                confirmed=False,
                                status=str(preflight.get("status", "preflight_failed")),
                                message=message,
                            )
                            continue
                        before_snapshot: dict[str, Any] | None = None
                        # Fix 5: Track whether we successfully sent the click so
                        # we can warn about a possible orphaned broker position if
                        # Chrome disconnects after the click but before confirmation.
                        clicked: bool = False
                        try:
                            before_snapshot_raw = _run_cdp_adapter_task(
                                adapter,
                                lambda inst=str((signal_payload or {}).get("instrument", "")).strip().upper(),
                                tokens=list(shared.get("expected_account_tokens") or ()): adapter.get_broker_state_snapshot(
                                    inst,
                                    expected_account_tokens=tokens,
                                ),
                            )
                            if isinstance(before_snapshot_raw, dict):
                                before_snapshot = dict(before_snapshot_raw)
                        except Exception as exc:
                            _APP_LOGGER.debug("Before-snapshot capture fejlede (non-critical): %s", exc)
                            before_snapshot = None
                        if signal == "FLAT":
                            if selector:
                                clicked = _run_cdp_adapter_task(
                                    adapter,
                                    lambda: adapter.click_element(selector, jitter_px=2.0),
                                )
                            elif isinstance(info, dict) and {"x", "y"}.issubset(info):
                                cx = float(info.get("x", 0))
                                cy = float(info.get("y", 0))
                                _run_cdp_adapter_task(
                                    adapter,
                                    lambda: adapter.human_click_at(cx, cy),
                                )
                                clicked = True
                            else:
                                _run_cdp_adapter_task(adapter, adapter.close_all_positions)
                                clicked = True
                        elif selector:
                            clicked = _run_cdp_adapter_task(
                                adapter,
                                lambda: adapter.click_element(selector, jitter_px=2.0),
                            )
                            # Fallback: if selector-based click failed but auto_discover
                            # stored known x,y coordinates, use them directly via CDP.
                            # This handles transient DOM states where the element exists
                            # visually but the JS query can't resolve it (animation frames,
                            # z-index stacking, pointer-events transitions, etc.).
                            if not clicked and isinstance(info, dict) and {"x", "y"}.issubset(info):
                                _fallback_cx = float(info["x"])
                                _fallback_cy = float(info["y"])
                                if _fallback_cx > 0 and _fallback_cy > 0:
                                    _APP_LOGGER.warning(
                                        "[GHOST-V6.6] %s: selector '%s' fejlede – falder tilbage til "
                                        "gemte koordinater (%.0f, %.0f).",
                                        signal,
                                        selector,
                                        _fallback_cx,
                                        _fallback_cy,
                                    )
                                    _run_cdp_adapter_task(
                                        adapter,
                                        lambda _cx=_fallback_cx, _cy=_fallback_cy: adapter.human_click_at(_cx, _cy),
                                    )
                                    clicked = True
                        else:
                            cx = float(info.get("x", 0))
                            cy = float(info.get("y", 0))
                            _run_cdp_adapter_task(
                                adapter,
                                lambda: adapter.human_click_at(cx, cy),
                            )
                            clicked = True

                        signal_label = f"{signal} [{signal_id[:8]}]" if signal_id else signal
                        if clicked:
                            _upsert_custom_human_inflight_order(
                                shared,
                                signal_payload or {},
                                status="clicked",
                                observed_at=datetime.now(tz=APP_TIMEZONE),
                                message=f"{signal_label} klik sendt til broker.",
                                position_snapshot=before_snapshot,
                                persist=True,
                            )
                            if selector:
                                location_label = f"selector `{selector}`"
                            elif isinstance(info, dict) and {"x", "y"}.issubset(info):
                                location_label = f"koordinater ({info.get('x', 0):.0f},{info.get('y', 0):.0f})"
                            else:
                                location_label = "platform close selectors"
                            verification = _verify_cdp_signal_broker_state(
                                adapter,
                                signal_payload or {},
                                stop_event,
                                shared=shared,
                                before_snapshot=before_snapshot,
                                expected_account_tokens=tuple(shared.get("expected_account_tokens") or ()),
                                timeout_s=(
                                    max(
                                        float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS),
                                        float(CUSTOM_HUMAN_EXIT_CONFIRMATION_TIMEOUT_SECONDS),
                                    )
                                    if signal == "FLAT"
                                    else float(CUSTOM_HUMAN_BROKER_CONFIRMATION_TIMEOUT_SECONDS)
                                ),
                            )
                            verification_message = str(verification.get("message", "")).strip()
                            verification_status = str(verification.get("status", "")).strip() or "verified"
                            confirmed = bool(verification.get("confirmed", False))
                            if confirmed:
                                shared["last_result"] = (
                                    f"✅ {signal_label} klik udført via {location_label} – broker-state bekræftet"
                                )
                            else:
                                shared["last_result"] = (
                                    f"⚠️ {signal_label} klik udført via {location_label}, "
                                    f"men broker-state kunne ikke bekræftes"
                                )
                            _upsert_custom_human_inflight_order(
                                shared,
                                signal_payload or {},
                                status="confirmed" if confirmed else "failed",
                                observed_at=datetime.now(tz=APP_TIMEZONE),
                                message=verification_message or shared["last_result"],
                                position_snapshot=verification.get("position_snapshot"),
                                persist=True,
                            )
                            _record_cdp_execution_confirmation(
                                shared,
                                signal_payload,
                                confirmed=confirmed,
                                status=verification_status,
                                message=verification_message or shared["last_result"],
                                position_snapshot=verification.get("position_snapshot"),
                            )
                            _mark_custom_human_worker_interaction(shared)
                            cooldown_timeout = _custom_human_jittered_interval(cooldown_s, floor=0.1)
                            if signal == "FLAT":
                                try:
                                    _run_cdp_adapter_task(
                                        adapter,
                                        lambda: adapter.reset_ui_focus(
                                            should_abort=lambda: _should_abort_custom_human_ui_reset(
                                                stop_event,
                                                signal_queue,
                                            ),
                                        ),
                                    )
                                except Exception as exc:
                                    _APP_LOGGER.warning("Post-FLAT UI-focus-reset fejlede (non-critical): %s", exc)
                                _wait_for_custom_human_post_flat_cooldown(
                                    stop_event,
                                    signal_queue,
                                    cooldown_timeout,
                                )
                            else:
                                stop_event.wait(timeout=cooldown_timeout)
                        else:
                            message = f"{signal_label}: element `{selector}` ikke fundet i DOM"
                            shared["last_result"] = f"❌ {message}"
                            _upsert_custom_human_inflight_order(
                                shared,
                                signal_payload or {},
                                status="failed",
                                observed_at=datetime.now(tz=APP_TIMEZONE),
                                message=message,
                                persist=True,
                            )
                            _record_cdp_execution_confirmation(
                                shared,
                                signal_payload,
                                confirmed=False,
                                status="dom_missing",
                                message=message,
                            )
                    except Exception as exc:
                        # Fix 5: Chrome-disconnect EFTER klik = mulig orphaned position.
                        # Vi ved at klikket gik til Tradovate men forbindelsen røg inden
                        # broker-verifikation. Position kan eksistere uden at systemet ved det.
                        _is_cdp_disconnect = isinstance(exc, CDPConnectionError)
                        if _is_cdp_disconnect and clicked:
                            _APP_LOGGER.critical(
                                "MULIG ORPHANED POSITION: Chrome mistet forbindelsen EFTER klik på %s. "
                                "Tjek Tradovate manuelt! Signal: %s [%s]",
                                signal,
                                signal,
                                str((signal_payload or {}).get("signal_id", ""))[:12],
                            )
                            _orphan_msg = (
                                f"⚠️ MULIG ORPHANED POSITION: Chrome-forbindelsen mistet EFTER klik – "
                                f"Tjek Tradovate manuelt! Signal: {signal_label}"
                            )
                            shared["last_result"] = _orphan_msg
                            _append_custom_human_diagnostic_event(
                                shared,
                                kind="observer",
                                headline="MULIG ORPHANED POSITION",
                                detail=(
                                    f"Chrome-forbindelsen fejlede efter klik på {signal_label}. "
                                    f"Tjek Tradovate UI manuelt – position kan eksistere."
                                ),
                                tone="warning",
                            )
                        message = f"{signal}: fejlede: {exc}"
                        if not (_is_cdp_disconnect and clicked):
                            shared["last_result"] = f"❌ {message}"
                        _upsert_custom_human_inflight_order(
                            shared,
                            signal_payload or {},
                            status="failed",
                            observed_at=datetime.now(tz=APP_TIMEZONE),
                            message=(
                                f"MULIG ORPHANED POSITION – {message}"
                                if _is_cdp_disconnect and clicked
                                else message
                            ),
                            persist=True,
                        )
                        _record_cdp_execution_confirmation(
                            shared,
                            signal_payload,
                            confirmed=False,
                            status="click_failed_connection_lost" if (_is_cdp_disconnect and clicked) else "click_failed",
                            message=message,
                        )
                else:
                    shared["last_result"] = f"⚠️ Ignorerede ugyldigt signal `{signal}`."
            finally:
                signal_queue.task_done()
    finally:
        shared["running"] = False


def _validate_custom_human_auto_targets(
    adapter: "CDPHumanAdapter",
    targets: dict[str, Any],
) -> dict[str, dict[str, Any] | None]:
    """Validate CSS selector targets for full-auto mode without clicking."""
    validated: dict[str, dict[str, Any] | None] = {"buy": None, "sell": None, "flat": None}
    for side in ("buy", "sell", "flat"):
        info = targets.get(side)
        if not isinstance(info, dict):
            continue
        selector = str(info.get("selector", "")).strip()
        if not selector:
            continue
        center = _run_cdp_adapter_task(adapter, lambda s=selector: adapter.get_element_center(s))
        if center is None:
            continue
        validated[side] = {
            "selector": selector,
            "x": float(center[0]),
            "y": float(center[1]),
        }
    return validated


def _run_custom_human_dom_contract_preflight(
    adapter: "CDPHumanAdapter",
    targets: dict[str, Any],
) -> dict[str, Any]:
    """Run a read-only Tradovate UI contract check before full-auto starts."""
    if str(getattr(adapter, "platform", "") or "").strip().lower() != "tradovate":
        return {
            "ok": True,
            "status": "skipped_non_tradovate",
            "message": "UI contract-check springes over for ikke-Tradovate platforme.",
            "contract": None,
        }

    required_actions: list[str] = []
    if isinstance(targets.get("buy"), dict):
        required_actions.append("buy")
    if isinstance(targets.get("sell"), dict):
        required_actions.append("sell")
    if isinstance(targets.get("flat"), dict):
        required_actions.append("flat")
    expected_instrument_token = str(targets.get("ticker", "") or "").strip().upper()

    contract = _run_cdp_adapter_task(
        adapter,
        lambda actions=tuple(required_actions), instrument_token=expected_instrument_token: adapter.inspect_tradovate_ui_contract(
            required_actions=actions,
            expected_instrument_token=instrument_token,
        ),
    )
    contract_payload = contract if isinstance(contract, dict) else {}
    issues = [
        str(issue).strip()
        for issue in contract_payload.get("issues", [])
        if str(issue).strip()
    ]
    warnings = [
        str(item).strip()
        for item in contract_payload.get("warnings", [])
        if str(item).strip()
    ]
    blocking_error_code = str(contract_payload.get("blocking_error_code", "") or "").strip()
    if not bool(contract_payload.get("ok", False)):
        message = " | ".join(issues) if issues else "Tradovate UI contract-check fejlede."
        if blocking_error_code:
            message = f"{blocking_error_code}: {message}"
        return {
            "ok": False,
            "status": str(contract_payload.get("status", "invalid")),
            "message": message,
            "contract": contract_payload,
            "issues": issues,
            "warnings": warnings,
            "error_code": blocking_error_code,
        }

    active_module = contract_payload.get("active_module", {})
    module_label = str(active_module.get("class_name", "")).strip() or "ukendt module"
    quantity_payload = contract_payload.get("quantity", {})
    quantity_value = str(quantity_payload.get("value", "")).strip()
    quantity_selector = str(quantity_payload.get("selector_used", "")).strip()
    entry_status = str((contract_payload.get("entry_integrity") or {}).get("status", "ok") or "ok")
    exit_status = str((contract_payload.get("exit_integrity") or {}).get("status", "ok") or "ok")
    degraded_note = " | fallback i brug" if "degraded" in {entry_status, exit_status} else ""
    return {
        "ok": True,
        "status": str(contract_payload.get("status", "ok") or "ok"),
        "message": (
            f"Tradovate UI contract OK ({module_label}) | "
            f"qty `{quantity_value or 'N/A'}` via `{quantity_selector or 'ukendt selector'}`.{degraded_note}"
        ),
        "contract": contract_payload,
        "issues": issues,
        "warnings": warnings,
        "error_code": blocking_error_code,
    }


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
    except Exception as exc:
        _APP_LOGGER.warning("Kunne ikke parse YAML-fil %s: %s", path, exc)
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _load_live_execution_config(repo_root: Path) -> dict[str, Any]:
    return _coerce_live_execution_config(_load_yaml_mapping(repo_root / "config" / "live_execution.yaml"))


def _build_trading_pipeline(
    account_balance: float,
    *,
    account_config_override: dict[str, Any] | None = None,
) -> ExecutionPipeline:
    repo_root = _repo_root()
    output_dir = repo_root / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    account_config = (
        dict(account_config_override)
        if isinstance(account_config_override, dict)
        else _load_yaml_mapping(repo_root / "config" / "account_config.yaml")
    )
    sizing_config = load_sizing_config(repo_root / "config" / "position_sizing.yaml")
    pipeline_config = PipelineConfig(
        live_execution=False,
        shadow_mode=True,
        account_config=account_config,
        sizing_config=sizing_config,
    )
    pipeline = ExecutionPipeline(
        broker=None,
        audit_db=AuditDB(db_path=output_dir / "trading_audit.sqlite3"),
        config=pipeline_config,
        account_balance=account_balance,
    )
    pipeline.load_strategies()
    return pipeline


def _run_async_task(task_factory: Callable[[], Awaitable[Any]]) -> Any:
    """Run async work safely from Streamlit callback code."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(task_factory())

    result: dict[str, Any] = {}
    error: list[BaseException] = []

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(task_factory())
        except BaseException as exc:  # pragma: no cover - defensive fallback
            error.append(exc)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if error:
        raise error[0]
    return result.get("value")


def _run_cdp_adapter_task(adapter: "CDPHumanAdapter", task_factory: Callable[[], Awaitable[Any]]) -> Any:
    """Run CDP adapter work on the adapter's persistent event loop."""
    return run_with_reconnect(adapter, task_factory)


try:
    _cdp_auto_trade_shared: dict[str, Any] = _get_cdp_auto_trade_shared_singleton()
except Exception as exc:  # noqa: BLE001
    _APP_LOGGER.critical(
        "Failed to initialize Custom Human shared runtime state during module import.",
        exc_info=True,
    )
    _capture_startup_snapshot(
        "module_level_shared_singleton_init_failed",
        exc,
    )
    raise

for _patch_name in [
    '_append_custom_human_diagnostic_event',
    '_build_cdp_auto_trade_shared_state',
    '_build_trading_pipeline',
    '_capture_custom_human_runtime_state',
    '_coerce_custom_human_live_state',
    '_custom_human_auto_supervisor_loop',
    '_custom_human_diagnostics_snapshot',
    '_custom_human_jittered_interval',
    '_custom_human_live_observer_loop',
    '_custom_human_preconfigured_auto_targets',
    '_custom_human_supervisor_tick',
    '_custom_human_tradovate_snapshot_loop',
    '_default_custom_human_live_state',
    '_ensure_cdp_signal_queue',
    '_ensure_custom_human_supervisor_running',
    '_get_cdp_auto_trade_shared_singleton',
    '_load_custom_human_live_candidate_from_dukascopy',
    '_load_custom_human_live_candidate_from_tradovate',
    '_load_custom_human_live_candidate_from_yahoo',
    '_load_custom_human_live_day_bars',
    '_load_custom_human_runtime_state',
    '_maybe_schedule_custom_human_idle_health_check',
    '_maybe_schedule_custom_human_idle_scroll_noise',
    '_persist_custom_human_runtime_state',
    '_prepare_custom_human_auto_targets',
    '_process_custom_human_signal_via_engine',
    '_queue_cdp_signal_from_custom_human',
    '_refresh_custom_human_tradovate_snapshot_health',
    '_reset_cdp_signal_queue',
    '_reset_stale_custom_human_reconcile_on_start',
    '_restore_custom_human_runtime_state_into_shared',
    '_run_cdp_adapter_task',
    '_run_custom_human_dom_contract_preflight',
    '_run_custom_human_signal_preflight',
    '_save_custom_human_runtime_state',
    '_start_custom_human_runtime_components',
    '_stop_custom_human_auto_runtime',
    '_validate_custom_human_auto_targets',
]:
    _install_patch_proxy(_patch_name)

__all__ = [
    'register_patch_namespace',
    '_cdp_auto_trade_shared',
    '_CDP_AUTO_TRADE_SINGLETON_ATTR',
    'CUSTOM_HUMAN_RUNTIME_STATE_FILE',
    'CDPHumanAdapter',
    'ExecutionPipeline',
    'TradingSignalRouter',
    '_build_cdp_auto_trade_shared_state',
    '_get_cdp_auto_trade_shared_singleton',
    '_capture_custom_human_runtime_state',
    '_persist_custom_human_runtime_state',
    '_restore_custom_human_runtime_state_into_shared',
    '_process_custom_human_signal_via_engine',
    '_custom_human_live_observer_loop',
    '_start_custom_human_runtime_components',
    '_stop_custom_human_auto_runtime',
    '_custom_human_supervisor_tick',
    '_custom_human_auto_supervisor_loop',
    '_ensure_custom_human_supervisor_running',
    '_prepare_custom_human_auto_targets',
    '_run_custom_human_signal_preflight',
    '_run_cdp_adapter_task',
    '_ensure_cdp_signal_queue',
    '_reset_cdp_signal_queue',
    '_queue_cdp_signal_from_custom_human',
    '_refresh_custom_human_tradovate_snapshot_health',
    '_custom_human_tradovate_snapshot_loop',
    '_custom_human_diagnostics_snapshot',
    '_append_custom_human_diagnostic_event',
    '_custom_human_jittered_interval',
    '_default_custom_human_live_state',
    '_coerce_custom_human_live_state',
    '_build_trading_pipeline',
    '_custom_human_store_key',
    '_custom_human_runtime_profile_is_valid',
    '_custom_human_preconfigured_auto_targets',
    '_custom_human_watchdog_snapshot',
    '_coerce_custom_human_tradovate_snapshot',
    '_custom_human_tradovate_snapshot_status',
    '_load_custom_human_runtime_state',
    '_save_custom_human_runtime_state',
]
