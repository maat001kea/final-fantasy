"""RiskGate state management helpers for the Custom Human trading engine.

This module owns the three functions that read, serialize, and mutate the live
RiskGate object stored inside the shared-state dict:

  _custom_human_gate_state_snapshot   – snapshot for GateDecision audit rows
  _serialize_custom_human_risk_gate_state – export current gate state to dict
  _clear_custom_human_riskgate_cache  – clear idempotency cache + reconcile flags

All three were previously defined in engine_core.py.  They were extracted here
as Phase 1 of the engine_core split to reduce that file's line count and to give
RiskGate management a dedicated, auditable home.

Dependency note
───────────────
``_clear_custom_human_riskgate_cache`` needs two helpers that still live in
``engine_core`` (``_coerce_custom_human_live_state`` and
``_persist_custom_human_runtime_state``).  A *lazy* import is used inside the
function body so that:

  1. There is no module-level circular import between risk_gate_manager ↔ engine_core.
  2. ``unittest.mock.patch("src.trading.engine_core._persist_custom_human_runtime_state")``
     in the test suite still intercepts calls correctly, because we access the
     helpers through the live module object at call time.
"""
from __future__ import annotations

import logging
from typing import Any

from src.trading.execution_pipeline import ExecutionPipeline

_LOG = logging.getLogger("final_fantasy.risk_gate_manager")


# ---------------------------------------------------------------------------
# Gate-state snapshot (used in GateDecision audit rows)
# ---------------------------------------------------------------------------

def _custom_human_gate_state_snapshot(pipeline: ExecutionPipeline) -> dict[str, Any]:
    """Return a lightweight snapshot of the live RiskGate counters for audit rows."""
    gate_state = pipeline.risk_gate.state
    return {
        "daily_pnl": float(gate_state.daily_pnl),
        "weekly_pnl": float(gate_state.weekly_pnl),
        "monthly_pnl": float(gate_state.monthly_pnl),
        "trades_today": int(gate_state.trades_today),
        "consecutive_losses": int(gate_state.consecutive_losses),
        "kill_switch_active": bool(gate_state.kill_switch_active),
        "circuit_breaker_active": bool(gate_state.circuit_breaker_active),
    }


# ---------------------------------------------------------------------------
# RiskGate state serialisation
# ---------------------------------------------------------------------------

def _serialize_custom_human_risk_gate_state(shared: dict[str, Any]) -> dict[str, Any] | None:
    """Export the live RiskGate state to a plain dict for persistence.

    Falls back to the last persisted state when the live pipeline is unavailable
    (e.g. before the observer has started or after a restart).
    """
    observer_cfg = shared.get("live_observer_cfg")
    if not isinstance(observer_cfg, dict):
        return dict(shared.get("persisted_gate_state")) if isinstance(shared.get("persisted_gate_state"), dict) else None
    pipeline = observer_cfg.get("pipeline")
    risk_gate = getattr(pipeline, "risk_gate", None)
    if risk_gate is None or not hasattr(risk_gate, "export_state"):
        return dict(shared.get("persisted_gate_state")) if isinstance(shared.get("persisted_gate_state"), dict) else None
    try:
        return dict(risk_gate.export_state())
    except Exception as exc:
        _LOG.warning("Kunne ikke serialisere Custom Human RiskGate state: %s", exc)
        return dict(shared.get("persisted_gate_state")) if isinstance(shared.get("persisted_gate_state"), dict) else None


# ---------------------------------------------------------------------------
# RiskGate cache clear (idempotency + manual-reconcile reset)
# ---------------------------------------------------------------------------

def _clear_custom_human_riskgate_cache(shared: dict[str, Any]) -> dict[str, Any]:
    """Clear the RiskGate idempotency cache and reset manual-reconcile flags.

    Safe to call while the engine is running:
    - Zeroes ``seen_idempotency_keys`` on the live RiskGate object (if the
      observer pipeline is active) so new signals are no longer blocked.
    - Also updates the persisted gate state so the reset survives an engine
      restart within the same trading day.
    - Clears ``reconcile_required`` + resets ``phase`` to
      ``'waiting_for_setup'`` in the live_state so the watchdog stops
      reporting ``MANUEL RECONCILE KRÆVET``.
    - Flushes everything to disk via ``_persist_custom_human_runtime_state``.

    Returns a summary dict with ``cleared_keys`` count and ``reconcile_cleared``.
    """
    # Lazy import: avoids a circular dependency at module load time while still
    # letting unittest.mock.patch("src.trading.engine_core.*") intercept calls.
    from src.trading import engine_core as _ec  # noqa: PLC0415

    cleared_keys = 0
    reconcile_cleared = False

    # ── 1. Clear live RiskGate object ────────────────────────────────────────
    observer_cfg = shared.get("live_observer_cfg")
    if isinstance(observer_cfg, dict):
        pipeline = observer_cfg.get("pipeline")
        risk_gate = getattr(pipeline, "risk_gate", None)
        if risk_gate is not None and hasattr(risk_gate, "state"):
            try:
                cleared_keys = len(risk_gate.state.seen_idempotency_keys)
                risk_gate.state.seen_idempotency_keys = set()
                _LOG.info(
                    "[CLEAR_RISKGATE] Cleared %d live idempotency key(s) from RiskGate.",
                    cleared_keys,
                )
            except Exception as exc:  # pragma: no cover
                _LOG.warning("[CLEAR_RISKGATE] Could not clear live RiskGate keys: %s", exc)

    # ── 2. Update persisted gate state ───────────────────────────────────────
    persisted = _serialize_custom_human_risk_gate_state(shared)
    if isinstance(persisted, dict):
        if cleared_keys == 0:
            cleared_keys = len(persisted.get("seen_idempotency_keys") or [])
        persisted["seen_idempotency_keys"] = []
        shared["persisted_gate_state"] = persisted
        shared["risk_gate_state"] = persisted

    # ── 3. Clear manual-reconcile in live_state ──────────────────────────────
    live_state = _ec._coerce_custom_human_live_state(shared.get("live_state"))
    if bool(live_state.get("reconcile_required", False)) or str(live_state.get("phase", "")) == "manual_reconcile":
        reconcile_cleared = True
        live_state["reconcile_required"] = False
        # Only move to waiting_for_setup when there is no open position so we
        # do not silently discard an in-flight trade.
        if not bool(live_state.get("position_open", False)):
            live_state["phase"] = "waiting_for_setup"
        live_state["last_note"] = "RiskGate cache manuelt renset – klar til nye signaler."
        shared["live_state"] = live_state
        _LOG.info("[CLEAR_RISKGATE] manual_reconcile cleared from live_state.")

    # ── 4. Persist to disk ────────────────────────────────────────────────────
    try:
        _ec._persist_custom_human_runtime_state(shared)
    except Exception as exc:  # pragma: no cover
        _LOG.warning("[CLEAR_RISKGATE] _persist failed: %s", exc)

    return {
        "cleared_keys": cleared_keys,
        "reconcile_cleared": reconcile_cleared,
    }
