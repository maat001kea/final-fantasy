"""Snapshot display helpers for the Custom Human trading engine.

This module owns the eight pure read-only display / status functions that
consume shared-state and return structured dicts for the UI and watchdog:

  _format_custom_human_diag_age                 – format age as "Xs" / "X.Xm"
  _custom_human_tradovate_snapshot_status       – tone/headline for broker snapshot
  _custom_human_preflight_snapshot              – last DOM-preflight summary
  _custom_human_watchdog_snapshot               – overall watchdog health
  _custom_human_watchdog_blocks_candidate       – should watchdog block a signal?
  _custom_human_post_entry_health_snapshot      – per-check open-position health
  _custom_human_diagnostics_snapshot            – full diagnostics payload for UI
  _format_custom_human_snapshot_pair_for_log    – JSON-serialise before/after pair

All eight were previously defined in engine_core.py.  They were extracted here
as Phase 5 of the engine_core split.

Dependency note
───────────────
These functions depend only on:
  • coercion_manager   (_coerce_custom_human_live_state,
                        _coerce_custom_human_tradovate_snapshot,
                        _normalize_custom_human_confirmation_snapshot,
                        _safe_float)
  • diagnostics_manager (_coerce_custom_human_diagnostics_events, APP_TIMEZONE)
  • stdlib / pandas

No lazy imports are needed — all dependencies live in already-extracted modules.
``diagnostics_manager._record_custom_human_watchdog_diagnostic`` previously used
a lazy import for ``_custom_human_watchdog_snapshot``; that import is now replaced
by a direct import from this module.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import pandas as pd

from src.trading.coercion_manager import (
    _coerce_custom_human_live_state,
    _coerce_custom_human_tradovate_snapshot,
    _normalize_custom_human_confirmation_snapshot,
    _safe_float,
)
from src.trading.diagnostics_manager import (
    APP_TIMEZONE,
    _coerce_custom_human_diagnostics_events,
)

_LOG = logging.getLogger("final_fantasy.engine_core")

# Kept here so snapshot functions can use it without importing from engine_core
# (which would create a circular dependency).  Must stay in sync with the
# definition in engine_core.py (line 523).
CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_STALE_SECONDS: float = 15.0


# ---------------------------------------------------------------------------
# Age formatter
# ---------------------------------------------------------------------------

def _format_custom_human_diag_age(age_seconds: float | None) -> str:
    if age_seconds is None:
        return "N/A"
    age = max(0.0, float(age_seconds))
    if age < 60.0:
        return f"{int(round(age))}s"
    return f"{age / 60.0:.1f}m"


# ---------------------------------------------------------------------------
# Tradovate snapshot status
# ---------------------------------------------------------------------------

def _custom_human_tradovate_snapshot_status(snapshot_raw: dict[str, Any] | None) -> dict[str, Any]:
    snapshot = _coerce_custom_human_tradovate_snapshot(snapshot_raw)
    if snapshot is None:
        return {
            "tone": "inactive",
            "headline": "INGEN TRADOVATE SNAPSHOT",
            "detail": "Ingen read-only browser snapshot er læst endnu.",
            "age_seconds": None,
        }

    observed_at = pd.to_datetime(snapshot.get("observed_at"), errors="coerce")
    age_seconds: float | None = None
    if pd.notna(observed_at):
        observed_ts = pd.Timestamp(observed_at)
        if observed_ts.tzinfo is None:
            observed_ts = observed_ts.tz_localize(APP_TIMEZONE)
        else:
            observed_ts = observed_ts.tz_convert(APP_TIMEZONE)
        age_seconds = max(0.0, (datetime.now(tz=APP_TIMEZONE) - observed_ts.to_pydatetime()).total_seconds())

    connected = bool(snapshot.get("connected"))
    account_ok = bool(snapshot.get("account_ok"))
    instrument_visible = bool(snapshot.get("instrument_visible"))
    quote_ready = bool(snapshot.get("quote_ready"))

    if age_seconds is not None and age_seconds > float(CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_STALE_SECONDS):
        tone = "warning"
        headline = "TRADOVATE SNAPSHOT STALE"
        detail = "Read-only snapshot er blevet for gammelt og opdateres ikke hurtigt nok."
    elif not connected:
        tone = "inactive"
        headline = "TRADOVATE SNAPSHOT AFBRUDT"
        detail = "Adapteren er ikke længere forbundet til Chrome."
    elif account_ok and instrument_visible and quote_ready:
        tone = "active"
        headline = "TRADOVATE SNAPSHOT KLAR"
        detail = "Account, instrument og quotes er synlige i browseren."
    elif not account_ok:
        tone = "warning"
        headline = "ACCOUNT MISMATCH I SNAPSHOT"
        detail = "Forventet konto-token blev ikke fundet i den synlige Tradovate-side."
    elif not instrument_visible:
        tone = "warning"
        headline = "INSTRUMENT IKKE SYNLIGT"
        detail = "Det forventede futures-symbol kunne ikke læses i den aktive Tradovate-side."
    else:
        tone = "warning"
        headline = "QUOTES IKKE KLAR"
        detail = "Snapshot læste konto/instrument, men kunne ikke læse LAST/BID/ASK stabilt."

    if age_seconds is not None:
        detail = f"{detail} | Opdateret for {age_seconds:.0f}s siden."

    return {
        "tone": tone,
        "headline": headline,
        "detail": detail,
        "age_seconds": age_seconds,
    }


# ---------------------------------------------------------------------------
# Preflight snapshot
# ---------------------------------------------------------------------------

def _custom_human_preflight_snapshot(shared_state: dict[str, Any] | None) -> dict[str, Any]:
    shared = shared_state if isinstance(shared_state, dict) else {}
    payload = dict(shared.get("last_dom_preflight")) if isinstance(shared.get("last_dom_preflight"), dict) else {}
    tradovate_snapshot_raw = _coerce_custom_human_tradovate_snapshot(shared.get("tradovate_snapshot"))
    tradovate_snapshot = tradovate_snapshot_raw if isinstance(tradovate_snapshot_raw, dict) else {}
    contract_payload = dict(payload.get("contract")) if isinstance(payload.get("contract"), dict) else {}
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
    entry_integrity = (
        dict(contract_payload.get("entry_integrity"))
        if isinstance(contract_payload.get("entry_integrity"), dict)
        else {}
    )
    exit_integrity = (
        dict(contract_payload.get("exit_integrity"))
        if isinstance(contract_payload.get("exit_integrity"), dict)
        else {}
    )
    health_map = (
        dict(contract_payload.get("health_map"))
        if isinstance(contract_payload.get("health_map"), dict)
        else {}
    )

    success = bool(payload.get("success", False))
    account_match = bool(payload.get("account_match", tradovate_snapshot.get("account_ok", False)))
    instrument_match = bool(payload.get("instrument_match", tradovate_snapshot.get("instrument_visible", False)))
    module_found = bool(payload.get("module_found", bool(active_module)))
    quantity_found = bool(payload.get("quantity_found", bool(quantity_payload.get("found", False))))
    quantity_value = str(payload.get("quantity_value", quantity_payload.get("value", "")) or "").strip()
    entry_safe = bool(payload.get("entry_safe", entry_integrity.get("ok", success)))
    exit_safe = bool(payload.get("exit_safe", exit_integrity.get("ok", success)))
    entry_status = str(payload.get("entry_status", entry_integrity.get("status", "unknown")) or "").strip().lower() or "unknown"
    exit_status = str(payload.get("exit_status", exit_integrity.get("status", "unknown")) or "").strip().lower() or "unknown"
    error_code = str(payload.get("error_code", contract_payload.get("blocking_error_code", "")) or "").strip()
    error_msg = str(payload.get("error_msg", "") or "").strip()
    if not error_msg:
        error_msg = (
            "Alle hardened preflight-checks bestået. Klar til eksekvering."
            if success
            else "Venter på UI synkronisering..."
        )

    return {
        "success": success,
        "account_match": account_match,
        "instrument_match": instrument_match,
        "module_found": module_found,
        "quantity_found": quantity_found,
        "quantity_value": quantity_value,
        "entry_safe": entry_safe,
        "exit_safe": exit_safe,
        "entry_status": entry_status,
        "exit_status": exit_status,
        "error_code": error_code,
        "error_msg": error_msg,
        "health_map": health_map,
        "contract": contract_payload,
    }


# ---------------------------------------------------------------------------
# Watchdog snapshot + blocks-candidate
# ---------------------------------------------------------------------------

def _custom_human_watchdog_snapshot(shared_state: dict[str, Any] | None) -> dict[str, Any]:
    shared = shared_state if isinstance(shared_state, dict) else {}
    auto_running = bool(shared.get("running", False))
    observer_cfg = shared.get("live_observer_cfg", {})
    observer_enabled = bool(isinstance(observer_cfg, dict) and observer_cfg.get("enabled"))
    observer_running = bool(shared.get("live_observer_running", False))
    snapshot_running = bool(shared.get("tradovate_snapshot_running", False))
    observer_status = str(shared.get("live_observer_status", "")).strip()
    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    if bool(live_state.get("reconcile_required", False)):
        return {
            "state": "manual_reconcile",
            "tone": "warning",
            "headline": "MANUEL RECONCILE KRÆVET",
            "detail": "Et klik blev sendt, men broker-state kunne ikke bekræftes. Nye signaler er pauset.",
            "block_new_entries": True,
        }
    if not auto_running:
        return {
            "state": "idle",
            "tone": "inactive",
            "headline": "WATCHDOG INAKTIV",
            "detail": "Full-auto er ikke startet.",
            "block_new_entries": False,
        }
    if observer_enabled and not observer_running:
        return {
            "state": "execution_only",
            "tone": "warning",
            "headline": "WATCHDOG: OBSERVER STOPPET",
            "detail": "Klik-worker kører, men live observer er stoppet. Nye entries/adds er blokeret.",
            "block_new_entries": True,
        }
    if not snapshot_running:
        return {
            "state": "snapshot_stopped",
            "tone": "warning",
            "headline": "WATCHDOG: SNAPSHOT STOPPET",
            "detail": "Tradovate snapshot-loop kører ikke. Nye entries/adds er blokeret.",
            "block_new_entries": True,
        }

    snapshot = _coerce_custom_human_tradovate_snapshot(shared.get("tradovate_snapshot"))
    snapshot_status = _custom_human_tradovate_snapshot_status(snapshot)
    if snapshot is None:
        return {
            "state": "snapshot_missing",
            "tone": "warning",
            "headline": "WATCHDOG: MANGLER SNAPSHOT",
            "detail": "Ingen Tradovate snapshot er læst endnu. Nye entries/adds er blokeret.",
            "block_new_entries": True,
        }
    if not bool(snapshot.get("connected", False)):
        return {
            "state": "cdp_disconnected",
            "tone": "warning",
            "headline": "WATCHDOG: CDP DISCONNECTED",
            "detail": "Tradovate snapshot kan ikke læses fra Chrome. Nye entries/adds er blokeret.",
            "block_new_entries": True,
        }
    if not bool(snapshot.get("account_ok", False)):
        return {
            "state": "account_mismatch",
            "tone": "warning",
            "headline": "WATCHDOG: ACCOUNT MISMATCH",
            "detail": "Tradovate snapshot matcher ikke den forventede konto. Nye entries/adds er blokeret.",
            "block_new_entries": True,
        }
    if not bool(snapshot.get("instrument_visible", False)):
        return {
            "state": "instrument_missing",
            "tone": "warning",
            "headline": "WATCHDOG: INSTRUMENT IKKE SYNLIGT",
            "detail": "Tradovate snapshot kan ikke se det forventede instrument. Nye entries/adds er blokeret.",
            "block_new_entries": True,
        }
    if not bool(snapshot.get("quote_ready", False)):
        return {
            "state": "quotes_unavailable",
            "tone": "warning",
            "headline": "WATCHDOG: QUOTES IKKE KLAR",
            "detail": "Tradovate snapshot mangler gyldige quotes. Nye entries/adds er blokeret.",
            "block_new_entries": True,
        }
    age_seconds = _safe_float(snapshot_status.get("age_seconds"))
    if age_seconds is not None and age_seconds > float(CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_STALE_SECONDS):
        return {
            "state": "observer_stale",
            "tone": "warning",
            "headline": "WATCHDOG: SNAPSHOT STALE",
            "detail": "Tradovate snapshot er for gammelt. Nye entries/adds er blokeret.",
            "block_new_entries": True,
        }
    if observer_status.lower().startswith("live observer fejl:"):
        return {
            "state": "observer_error",
            "tone": "warning",
            "headline": "WATCHDOG: OBSERVER FEJL",
            "detail": observer_status,
            "block_new_entries": True,
        }
    return {
        "state": "healthy",
        "tone": "active",
        "headline": "WATCHDOG OK",
        "detail": "Snapshot, observer og konto ser sunde ud.",
        "block_new_entries": False,
    }


def _custom_human_watchdog_blocks_candidate(
    watchdog: dict[str, Any] | None,
    candidate: dict[str, Any] | None,
) -> bool:
    payload = watchdog if isinstance(watchdog, dict) else {}
    if not bool(payload.get("block_new_entries", False)):
        return False
    event = str((candidate or {}).get("event", "")).strip().lower()
    return event in {"entry", "add"}


# ---------------------------------------------------------------------------
# Post-entry health snapshot
# ---------------------------------------------------------------------------

def _custom_human_post_entry_health_snapshot(shared_state: dict[str, Any] | None) -> dict[str, Any]:
    shared = shared_state if isinstance(shared_state, dict) else {}
    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    snapshot = _coerce_custom_human_tradovate_snapshot(shared.get("tradovate_snapshot"))
    watchdog = _custom_human_watchdog_snapshot(shared)
    runtime_profile = dict(shared.get("runtime_profile")) if isinstance(shared.get("runtime_profile"), dict) else {}
    last_confirmation = (
        dict(shared.get("live_last_confirmation")) if isinstance(shared.get("live_last_confirmation"), dict) else None
    )
    signal_queue = shared.get("signal_queue")

    queue_depth = 0
    if signal_queue is not None and hasattr(signal_queue, "qsize"):
        try:
            queue_depth = int(max(0, int(signal_queue.qsize())))
        except Exception:
            queue_depth = 0

    snapshot_position_open = bool(snapshot.get("position_open", False)) if isinstance(snapshot, dict) else False
    state_position_open = bool(live_state.get("position_open", False))
    position_active = snapshot_position_open or state_position_open
    if not position_active:
        return {
            "state": "inactive",
            "tone": "inactive",
            "headline": "POST-ENTRY HEALTH INAKTIV",
            "detail": "Ingen åben position at overvåge.",
            "checks": [],
            "position_qty": 0.0,
            "position_side": "flat",
            "expected_qty": None,
        }

    base_qty = max(1.0, float(int(max(1, int(round(float(runtime_profile.get("fixed_contracts", 1) or 1)))))))
    expected_qty = base_qty * max(1, 1 + int(live_state.get("add_count_sent", 0) or 0))
    actual_qty = abs(_safe_float((snapshot or {}).get("position_qty")) or 0.0)
    actual_side = str((snapshot or {}).get("position_side", "") or "").strip().lower()
    expected_side = str(live_state.get("direction", "") or "").strip().lower()
    confirmation_status = str((last_confirmation or {}).get("status", "") or "").strip().lower()
    pending_signal_id = str(live_state.get("pending_signal_id", "") or "").strip()
    reconcile_required = bool(live_state.get("reconcile_required", False))
    snapshot_age_seconds = _safe_float(_custom_human_tradovate_snapshot_status(snapshot).get("age_seconds")) if isinstance(snapshot, dict) else None

    checks: list[dict[str, Any]] = []

    def _add_check(label: str, passed: bool, detail: str) -> None:
        checks.append({"label": label, "passed": bool(passed), "detail": str(detail)})

    _add_check(
        "Watchdog",
        str(watchdog.get("state", "")) == "healthy",
        str(watchdog.get("headline", "WATCHDOG N/A")),
    )
    _add_check(
        "Broker position",
        snapshot_position_open and actual_qty > 0.0,
        f"qty {actual_qty:g} | side {actual_side or 'ukendt'}",
    )
    side_ok = True
    if expected_side in {"long", "short"} and actual_side in {"long", "short"}:
        side_ok = expected_side == actual_side
    _add_check(
        "Side match",
        side_ok,
        f"forventet {expected_side or 'ukendt'} | broker {actual_side or 'ukendt'}",
    )
    _add_check(
        "Qty match",
        actual_qty >= expected_qty,
        f"forventet >= {expected_qty:g} | broker {actual_qty:g}",
    )
    confirmation_ok = confirmation_status in {"confirmed_open", "confirmed_add"} or bool(
        str(live_state.get("last_reconciled_at", "") or "").strip()
    )
    _add_check(
        "Confirmation",
        confirmation_ok,
        confirmation_status or "Ingen confirmation endnu",
    )
    _add_check(
        "Pending clear",
        not pending_signal_id,
        pending_signal_id[:12] if pending_signal_id else "Ingen pending signal",
    )
    _add_check(
        "Queue clear",
        queue_depth == 0,
        f"queue depth {queue_depth}",
    )
    _add_check(
        "No reconcile",
        not reconcile_required,
        "manual_reconcile" if reconcile_required else "Ingen reconcile nødvendig",
    )
    stale_limit = float(CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_STALE_SECONDS)
    _add_check(
        "Snapshot age",
        snapshot_age_seconds is not None and snapshot_age_seconds <= stale_limit,
        f"{_format_custom_human_diag_age(snapshot_age_seconds)} / limit {int(stale_limit)}s",
    )

    failed = [item for item in checks if not bool(item.get("passed", False))]
    if failed:
        failed_bits = ", ".join(str(item.get("label", "")) for item in failed[:3] if str(item.get("label", "")).strip())
        return {
            "state": "warning",
            "tone": "warning",
            "headline": "POST-ENTRY HEALTH ADVARSEL",
            "detail": failed_bits or "Position åben, men en eller flere health-checks fejlede.",
            "checks": checks,
            "position_qty": actual_qty,
            "position_side": actual_side or "ukendt",
            "expected_qty": expected_qty,
        }

    return {
        "state": "healthy",
        "tone": "active",
        "headline": "POST-ENTRY HEALTH OK",
        "detail": f"Broker og runtime matcher åben {actual_side or 'position'} på {actual_qty:g}.",
        "checks": checks,
        "position_qty": actual_qty,
        "position_side": actual_side or "ukendt",
        "expected_qty": expected_qty,
    }


# ---------------------------------------------------------------------------
# Full diagnostics snapshot (UI payload)
# ---------------------------------------------------------------------------

def _custom_human_diagnostics_snapshot(shared_state: dict[str, Any] | None) -> dict[str, Any]:
    shared = shared_state if isinstance(shared_state, dict) else {}
    last_dispatch = dict(shared.get("live_last_dispatch")) if isinstance(shared.get("live_last_dispatch"), dict) else None
    last_confirmation = (
        dict(shared.get("live_last_confirmation")) if isinstance(shared.get("live_last_confirmation"), dict) else None
    )
    market_meta = dict(shared.get("live_market_meta")) if isinstance(shared.get("live_market_meta"), dict) else {}
    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    tradovate_snapshot = _coerce_custom_human_tradovate_snapshot(shared.get("tradovate_snapshot"))
    events = _coerce_custom_human_diagnostics_events(shared.get("diagnostics_events"))
    signal_queue = shared.get("signal_queue")

    queue_depth = 0
    if signal_queue is not None and hasattr(signal_queue, "qsize"):
        try:
            queue_depth = int(max(0, int(signal_queue.qsize())))
        except Exception:
            queue_depth = 0

    pending_signal_id = str(live_state.get("pending_signal_id", "") or "").strip()
    pending_event = str(live_state.get("pending_event", "") or "").strip().lower()

    dispatch_status = str((last_dispatch or {}).get("status", "")).strip() or "Ingen dispatch endnu"
    dispatch_event = str((last_dispatch or {}).get("event", "")).strip().lower()
    dispatch_signal = str((last_dispatch or {}).get("signal", "")).strip().upper()
    dispatch_summary = dispatch_status
    if dispatch_event or dispatch_signal:
        dispatch_summary = " / ".join(part for part in [dispatch_event.upper(), dispatch_signal, dispatch_status] if part)

    confirmation_status = str((last_confirmation or {}).get("status", "")).strip() or "Ingen confirmation endnu"
    confirmation_signal = str((last_confirmation or {}).get("signal", "")).strip().upper()
    confirmation_event = str((last_confirmation or {}).get("event", "")).strip().lower()
    confirmation_summary = confirmation_status
    if confirmation_signal or confirmation_event:
        confirmation_summary = " / ".join(
            part for part in [confirmation_event.upper(), confirmation_signal, confirmation_status] if part
        )

    snapshot_age_seconds = None
    snapshot_observed_at = pd.to_datetime((tradovate_snapshot or {}).get("observed_at"), errors="coerce")
    if pd.notna(snapshot_observed_at):
        snapshot_ts = pd.Timestamp(snapshot_observed_at)
        if snapshot_ts.tzinfo is None:
            snapshot_ts = snapshot_ts.tz_localize(APP_TIMEZONE)
        else:
            snapshot_ts = snapshot_ts.tz_convert(APP_TIMEZONE)
        snapshot_age_seconds = max(0.0, (datetime.now(tz=APP_TIMEZONE) - snapshot_ts.to_pydatetime()).total_seconds())

    feed_age_seconds = None
    latest_source_ts = pd.to_datetime(market_meta.get("latest_source_timestamp"), errors="coerce")
    if pd.notna(latest_source_ts):
        feed_ts = pd.Timestamp(latest_source_ts)
        if feed_ts.tzinfo is None:
            feed_ts = feed_ts.tz_localize(APP_TIMEZONE)
        else:
            feed_ts = feed_ts.tz_convert(APP_TIMEZONE)
        feed_age_seconds = max(0.0, (datetime.now(tz=APP_TIMEZONE) - feed_ts.to_pydatetime()).total_seconds())

    return {
        "watchdog": _custom_human_watchdog_snapshot(shared),
        "post_entry_health": _custom_human_post_entry_health_snapshot(shared),
        "preflight": _custom_human_preflight_snapshot(shared),
        "last_dispatch": last_dispatch,
        "last_dispatch_summary": dispatch_summary,
        "last_confirmation": last_confirmation,
        "last_confirmation_summary": confirmation_summary,
        "feed_source": str(market_meta.get("source", "N/A") or "N/A"),
        "feed_symbol": str(market_meta.get("symbol", "N/A") or "N/A"),
        "feed_timestamp": str(market_meta.get("latest_source_timestamp", "") or ""),
        "feed_age_seconds": feed_age_seconds,
        "snapshot_age_seconds": snapshot_age_seconds,
        "queue_depth": queue_depth,
        "pending_signal_id": pending_signal_id,
        "pending_event": pending_event,
        "events": events[-5:],
    }


# ---------------------------------------------------------------------------
# Snapshot pair formatter (for broker-confirmation logging)
# ---------------------------------------------------------------------------

def _format_custom_human_snapshot_pair_for_log(
    left: dict[str, Any] | None,
    right: dict[str, Any] | None,
) -> str:
    payload = {
        "previous": _normalize_custom_human_confirmation_snapshot(left),
        "current": _normalize_custom_human_confirmation_snapshot(right),
    }
    try:
        return json.dumps(payload, ensure_ascii=True, sort_keys=True)
    except Exception:
        return str(payload)
