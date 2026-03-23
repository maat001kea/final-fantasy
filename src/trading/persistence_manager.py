"""Runtime-state persistence helpers for the Custom Human trading engine.

This module owns the eleven functions that capture, save, load and restore
the engine's runtime state to/from disk, plus the two startup-guard helpers
that run immediately after a load:

  _custom_human_store_key                       – session-state key builder
  _load_custom_human_runtime_state              – read JSON payload from disk
  _save_custom_human_runtime_state              – atomic write JSON to disk
  _custom_human_inflight_timestamp_value        – sort key for inflight entries
  _apply_custom_human_startup_inflight_guard    – re-arm inflight order on boot
  _reset_stale_custom_human_reconcile_on_start  – clear stale reconcile flag
  _normalize_custom_human_auto_requested        – normalise auto-requested flag
  _custom_human_runtime_profile_is_valid        – validate runtime profile
  _capture_custom_human_runtime_state           – build save payload from shared
  _persist_custom_human_runtime_state           – capture + save in one call
  _restore_custom_human_runtime_state_into_shared – load + write into shared

All eleven were previously defined in engine_core.py.  They were extracted here
as Phase 6 of the engine_core split.

Dependency note
───────────────
Two lazy imports are used inside function bodies to avoid circular imports:

  1. ``_external_value`` (engine_core) — reads from ``_PATCH_NAMESPACE``, a
     module-level global in engine_core used by the test-patching infrastructure.
     Moving it here would break ``patch("src.trading.engine_core._PATCH_NAMESPACE")``.

  2. ``_clear_custom_human_live_pending`` (engine_core) — used in three other
     engine_core functions that are not part of this cluster, so it must remain
     in engine_core and be imported lazily here.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time as time_module
from datetime import datetime
from pathlib import Path
from typing import Any, MutableMapping

import pandas as pd

from src.trading.coercion_manager import (
    _coerce_custom_human_diagnostics_events,
    _coerce_custom_human_diagnostics_events_startup,
    _coerce_custom_human_inflight_orders,
    _coerce_custom_human_inflight_orders_startup,
    _coerce_custom_human_live_state,
    _coerce_custom_human_tradovate_bars,
    _coerce_custom_human_tradovate_bars_startup,
    _coerce_custom_human_tradovate_price_samples,
    _coerce_custom_human_tradovate_price_samples_startup,
    _coerce_custom_human_tradovate_snapshot_startup,
    _default_custom_human_live_state,
    _safe_float_startup,
)
from src.trading.diagnostics_manager import APP_TIMEZONE
from src.trading.risk_gate_manager import _serialize_custom_human_risk_gate_state

_LOG = logging.getLogger("final_fantasy.engine_core")

# Path to the runtime-state JSON file.  Kept in sync with the definition in
# engine_core.py (line 196).  Defined here as a plain constant so that
# persistence_manager never needs to import from engine_core at module level.
CUSTOM_HUMAN_RUNTIME_STATE_FILE: Path = (
    Path(__file__).resolve().parents[2] / ".streamlit" / "custom_human_runtime_state.json"
)


# ---------------------------------------------------------------------------
# Store-key helper
# ---------------------------------------------------------------------------

def _custom_human_store_key(suffix: str) -> str:
    return f"sa_custom_human_strategy_{suffix}"


# ---------------------------------------------------------------------------
# File I/O  (atomic read / write)
# ---------------------------------------------------------------------------

def _load_custom_human_runtime_state() -> dict[str, Any]:
    # Lazy import: _external_value reads _PATCH_NAMESPACE which must stay in
    # engine_core to keep test-patching working.
    from src.trading import engine_core as _ec  # noqa: PLC0415

    runtime_state_file = Path(_ec._external_value("CUSTOM_HUMAN_RUNTIME_STATE_FILE", CUSTOM_HUMAN_RUNTIME_STATE_FILE))
    if not runtime_state_file.exists():
        return {}
    try:
        with runtime_state_file.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        _LOG.warning("Kunne ikke loade Custom Human runtime state: %s", exc)
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_custom_human_runtime_state(payload: dict[str, Any]) -> None:
    # Lazy import: same reason as _load_custom_human_runtime_state above.
    from src.trading import engine_core as _ec  # noqa: PLC0415

    runtime_state_file = Path(_ec._external_value("CUSTOM_HUMAN_RUNTIME_STATE_FILE", CUSTOM_HUMAN_RUNTIME_STATE_FILE))
    runtime_state_file.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"{runtime_state_file.name}.",
        suffix=".tmp",
        dir=str(runtime_state_file.parent),
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, runtime_state_file)
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Inflight sort key
# ---------------------------------------------------------------------------

def _custom_human_inflight_timestamp_value(entry: dict[str, Any]) -> float:
    for key in ("confirmed_at", "clicked_at", "queued_at", "reserved_at"):
        raw = str(entry.get(key, "") or "").strip()
        if not raw:
            continue
        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.isna(parsed):
            continue
        if getattr(parsed, "tzinfo", None) is None:
            parsed = parsed.tz_localize(APP_TIMEZONE)
        else:
            parsed = parsed.tz_convert(APP_TIMEZONE)
        return float(parsed.timestamp())
    return 0.0


# ---------------------------------------------------------------------------
# Startup guards
# ---------------------------------------------------------------------------

def _apply_custom_human_startup_inflight_guard(shared: dict[str, Any]) -> bool:
    # Lazy import: _clear_custom_human_live_pending is also used in three other
    # engine_core functions outside this cluster, so it must remain there.
    from src.trading import engine_core as _ec  # noqa: PLC0415

    inflight_orders = _coerce_custom_human_inflight_orders(shared.get("inflight_orders"))
    if not inflight_orders:
        return False

    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    nonterminal_statuses = {"reserved", "queued", "clicked"}
    nonterminal_entries = [
        entry for entry in inflight_orders.values()
        if str(entry.get("status", "") or "").strip().lower() in nonterminal_statuses
    ]
    changed = False

    if len(nonterminal_entries) > 1:
        live_state = _ec._clear_custom_human_live_pending(live_state)
        live_state["reconcile_required"] = True
        live_state["phase"] = "manual_reconcile"
        live_state["last_note"] = (
            "Flere uafklarede inflight-ordrer fundet ved opstart. Nye signaler er pauset."
        )
        shared["live_state"] = live_state
        shared["live_observer_status"] = live_state["last_note"]
        return True

    target_entry: dict[str, Any] | None = None
    candidate_entries = nonterminal_entries if nonterminal_entries else list(inflight_orders.values())
    if candidate_entries:
        target_entry = max(candidate_entries, key=_custom_human_inflight_timestamp_value)
    if not isinstance(target_entry, dict):
        return False

    target_signal_id = str(target_entry.get("signal_id", "") or "").strip()
    if not target_signal_id:
        return False

    if str(live_state.get("pending_signal_id", "") or "").strip() != target_signal_id:
        live_state["pending_signal_id"] = target_signal_id
        live_state["pending_event"] = str(target_entry.get("event", "") or "").strip().lower()
        changed = True
    if not isinstance(live_state.get("pending_candidate"), dict) and isinstance(target_entry.get("candidate"), dict):
        live_state["pending_candidate"] = dict(target_entry.get("candidate"))
        changed = True
    if changed:
        live_state["reconcile_required"] = False
        live_state["last_note"] = (
            f"Genoptager inflight {str(target_entry.get('event', 'signal') or 'signal').upper()} "
            "fra disk ved opstart – afventer broker-bekræftelse."
        ).strip()
        shared["live_state"] = live_state
        shared["live_observer_status"] = live_state["last_note"]
        shared["_startup_inflight_deadline"] = time_module.time() + 8.0
    return changed


def _reset_stale_custom_human_reconcile_on_start(
    *,
    state_raw: dict[str, Any] | None,
    snapshot_raw: dict[str, Any] | None,
    trade_date: str = "",
    reset_context: str = "ny auto-start",
) -> tuple[dict[str, Any], bool]:
    state = _coerce_custom_human_live_state(state_raw)
    snapshot = _coerce_custom_human_tradovate_snapshot_startup(snapshot_raw)
    if snapshot is None:
        return state, False

    phase = str(state.get("phase", "") or "").strip().lower()
    reconcile_required = bool(state.get("reconcile_required", False))
    pending_signal_id = str(state.get("pending_signal_id", "") or "").strip()
    position_open = bool(snapshot.get("position_open", False))
    position_qty = _safe_float_startup(snapshot.get("position_qty")) or 0.0

    stale_reconcile = reconcile_required or phase == "manual_reconcile"
    if not stale_reconcile or pending_signal_id or position_open or abs(position_qty) > 0.0:
        return state, False

    reset_state = _default_custom_human_live_state(trade_date=trade_date or str(state.get("trade_date", "") or ""))
    reset_state["broker_position_qty"] = 0.0
    reset_state["broker_account_value"] = str(snapshot.get("account_value", "") or "")
    reset_state["last_broker_snapshot"] = dict(snapshot)
    reset_state["last_note"] = f"Tidligere manual reconcile ryddet ved {str(reset_context or 'ny auto-start').strip()}."
    return reset_state, True


# ---------------------------------------------------------------------------
# Profile validators / normalisers
# ---------------------------------------------------------------------------

def _normalize_custom_human_auto_requested(
    auto_requested: Any,
    *,
    active: Any,
    runtime_profile: Any,
) -> bool:
    def _profile_valid(profile_raw: Any) -> bool:
        profile = dict(profile_raw) if isinstance(profile_raw, dict) else {}
        strategy_name = str(profile.get("strategy_name", "") or "").strip()
        return bool(strategy_name)

    profile = dict(runtime_profile) if isinstance(runtime_profile, dict) else {}
    if not bool(auto_requested):
        return False
    if not bool(active):
        return False
    return _profile_valid(profile)


def _custom_human_runtime_profile_is_valid(runtime_profile: Any) -> bool:
    profile = dict(runtime_profile) if isinstance(runtime_profile, dict) else {}
    strategy_name = str(profile.get("strategy_name", "") or "").strip()
    return bool(strategy_name)


# ---------------------------------------------------------------------------
# Capture / persist / restore
# ---------------------------------------------------------------------------

def _capture_custom_human_runtime_state(
    shared: dict[str, Any],
    *,
    session_state: MutableMapping[str, Any] | None = None,
) -> dict[str, Any]:
    # Lazy import: _load_custom_human_runtime_state is wrapped in a _PatchSafeProxy
    # inside engine_core so that monkeypatch.setattr(app, "_load_...", mock) is
    # respected by callers inside this module.
    from src.trading import engine_core as _ec  # noqa: PLC0415

    persisted = _ec._load_custom_human_runtime_state() if session_state is None else {}
    target_state = session_state if isinstance(session_state, MutableMapping) else {}
    active_key = _custom_human_store_key("active")
    runtime_profile_key = _custom_human_store_key("runtime_profile")
    config_store_key = _custom_human_store_key("config")
    shared_runtime_profile = dict(shared.get("runtime_profile")) if isinstance(shared.get("runtime_profile"), dict) else {}
    shared_runtime_config = dict(shared.get("runtime_config")) if isinstance(shared.get("runtime_config"), dict) else {}
    runtime_profile = target_state.get(
        runtime_profile_key,
        shared_runtime_profile if shared_runtime_profile else persisted.get("runtime_profile", {}),
    )
    config_raw = target_state.get(
        config_store_key,
        shared_runtime_config if shared_runtime_config else persisted.get("config", {}),
    )
    active = bool(
        target_state.get(
            active_key,
            shared.get("runtime_active", persisted.get("active", False)),
        )
    ) and _custom_human_runtime_profile_is_valid(runtime_profile)
    return {
        "version": 1,
        "saved_at": datetime.now(tz=APP_TIMEZONE).isoformat(),
        "active": active,
        "auto_requested": _normalize_custom_human_auto_requested(
            shared.get("auto_requested", persisted.get("auto_requested", False)),
            active=active,
            runtime_profile=runtime_profile,
        ),
        "last_stop_reason": str(shared.get("last_stop_reason", persisted.get("last_stop_reason", "")) or ""),
        "auto_restart_attempts": int(shared.get("auto_restart_attempts", persisted.get("auto_restart_attempts", 0)) or 0),
        "runtime_profile": dict(runtime_profile) if isinstance(runtime_profile, dict) else {},
        "config": dict(config_raw) if isinstance(config_raw, dict) else {},
        "expected_account_tokens": list(shared.get("expected_account_tokens") or ()),
        "live_state": _coerce_custom_human_live_state(shared.get("live_state")),
        "live_market_meta": dict(shared.get("live_market_meta")) if isinstance(shared.get("live_market_meta"), dict) else {},
        "live_last_dispatch": (
            dict(shared.get("live_last_dispatch")) if isinstance(shared.get("live_last_dispatch"), dict) else None
        ),
        "live_last_confirmation": (
            dict(shared.get("live_last_confirmation")) if isinstance(shared.get("live_last_confirmation"), dict) else None
        ),
        "inflight_orders": _coerce_custom_human_inflight_orders(shared.get("inflight_orders")),
        "live_observer_status": str(shared.get("live_observer_status", "") or ""),
        "tradovate_snapshot": (
            dict(shared.get("tradovate_snapshot")) if isinstance(shared.get("tradovate_snapshot"), dict) else None
        ),
        "tradovate_snapshot_status": str(shared.get("tradovate_snapshot_status", "") or ""),
        "tradovate_price_samples": _coerce_custom_human_tradovate_price_samples(
            shared.get("tradovate_price_samples")
        )[-3000:],
        "tradovate_15m_bars": _coerce_custom_human_tradovate_bars(shared.get("tradovate_15m_bars")),
        "tradovate_bar_builder_status": str(shared.get("tradovate_bar_builder_status", "") or ""),
        "diagnostics_events": _coerce_custom_human_diagnostics_events(shared.get("diagnostics_events")),
        "last_result": str(shared.get("last_result", "") or ""),
        "risk_gate_state": _serialize_custom_human_risk_gate_state(shared),
    }


def _persist_custom_human_runtime_state(
    shared: dict[str, Any],
    *,
    session_state: MutableMapping[str, Any] | None = None,
) -> None:
    # Lazy import: route through engine_core so _PatchSafeProxy intercepts mocks.
    from src.trading import engine_core as _ec  # noqa: PLC0415

    payload = _ec._capture_custom_human_runtime_state(shared, session_state=session_state)
    gate_state = payload.get("risk_gate_state")
    if isinstance(gate_state, dict):
        shared["persisted_gate_state"] = dict(gate_state)
    _ec._save_custom_human_runtime_state(payload)


def _restore_custom_human_runtime_state_into_shared(shared: dict[str, Any]) -> None:
    # Lazy import: route _load_.../_save_.../_capture_... through engine_core so
    # the _PatchSafeProxy there intercepts monkeypatch.setattr(app, ...) mocks.
    from src.trading import engine_core as _ec  # noqa: PLC0415

    payload = _ec._load_custom_human_runtime_state()
    shared["runtime_state_loaded"] = False
    if not isinstance(payload, dict) or not payload:
        shared["runtime_state_loaded"] = True
        return

    payload_active = bool(payload.get("active", False)) and _custom_human_runtime_profile_is_valid(
        payload.get("runtime_profile")
    )
    payload_runtime_profile = payload.get("runtime_profile")
    normalized_auto_requested = _normalize_custom_human_auto_requested(
        payload.get("auto_requested", False),
        active=payload_active,
        runtime_profile=payload_runtime_profile,
    )
    if normalized_auto_requested != bool(payload.get("auto_requested", False)):
        corrected_payload = dict(payload)
        corrected_payload["auto_requested"] = normalized_auto_requested
        _ec._save_custom_human_runtime_state(corrected_payload)
        payload = corrected_payload

    shared["running"] = False
    shared["stop_event"] = None
    shared["auto_requested"] = normalized_auto_requested
    shared["last_stop_reason"] = str(payload.get("last_stop_reason", "") or "")
    shared["auto_restart_attempts"] = int(payload.get("auto_restart_attempts", 0) or 0)
    shared["supervisor_running"] = False
    shared["supervisor_stop_event"] = None
    shared["supervisor_last_reason"] = ""
    shared["connected"] = False
    shared["runtime_active"] = payload_active
    shared["runtime_profile"] = dict(payload_runtime_profile) if isinstance(payload_runtime_profile, dict) else {}
    shared["runtime_config"] = dict(payload.get("config")) if isinstance(payload.get("config"), dict) else {}
    shared["adapter"] = None
    shared["expected_account_tokens"] = tuple(payload.get("expected_account_tokens") or ())
    shared["live_state"] = _coerce_custom_human_live_state(payload.get("live_state"))
    shared["live_market_meta"] = (
        dict(payload.get("live_market_meta")) if isinstance(payload.get("live_market_meta"), dict) else {}
    )
    shared["live_last_dispatch"] = (
        dict(payload.get("live_last_dispatch")) if isinstance(payload.get("live_last_dispatch"), dict) else None
    )
    shared["live_last_confirmation"] = (
        dict(payload.get("live_last_confirmation"))
        if isinstance(payload.get("live_last_confirmation"), dict)
        else None
    )
    shared["inflight_orders"] = _coerce_custom_human_inflight_orders_startup(payload.get("inflight_orders"))
    shared["live_observer_status"] = str(payload.get("live_observer_status", "") or "")
    shared["tradovate_snapshot"] = _coerce_custom_human_tradovate_snapshot_startup(payload.get("tradovate_snapshot"))
    shared["tradovate_snapshot_status"] = str(payload.get("tradovate_snapshot_status", "") or "")
    shared["tradovate_price_samples"] = _coerce_custom_human_tradovate_price_samples_startup(
        payload.get("tradovate_price_samples")
    )
    shared["tradovate_15m_bars"] = _coerce_custom_human_tradovate_bars_startup(payload.get("tradovate_15m_bars"))
    shared["tradovate_bar_builder_status"] = str(payload.get("tradovate_bar_builder_status", "") or "")
    shared["last_result"] = str(payload.get("last_result", "") or "")
    shared["diagnostics_events"] = _coerce_custom_human_diagnostics_events_startup(payload.get("diagnostics_events"))
    shared["persisted_gate_state"] = (
        dict(payload.get("risk_gate_state")) if isinstance(payload.get("risk_gate_state"), dict) else None
    )
    shared["execution_confirmations"] = {}
    restored_live_state, stale_reconcile_reset = _reset_stale_custom_human_reconcile_on_start(
        state_raw=shared.get("live_state"),
        snapshot_raw=shared.get("tradovate_snapshot"),
        trade_date=datetime.now(APP_TIMEZONE).strftime("%Y-%m-%d"),
        reset_context="app-genstart",
    )
    shared["live_state"] = restored_live_state
    startup_inflight_guard = _apply_custom_human_startup_inflight_guard(shared)
    if stale_reconcile_reset:
        shared["live_observer_status"] = str(restored_live_state.get("last_note", "") or "").strip()
        shared["live_last_confirmation"] = None
    if stale_reconcile_reset or startup_inflight_guard:
        _ec._save_custom_human_runtime_state(_ec._capture_custom_human_runtime_state(shared))
    shared["runtime_state_loaded"] = True
