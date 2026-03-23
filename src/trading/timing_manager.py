"""Polling-interval, bio-modulation, and timing helpers for the Custom Human engine.

This module owns every function that decides *when* to poll, how to jitter
intervals, and how to track worker-interaction timestamps.  All functions here
are pure in the sense that they read from the ``shared`` state dict and
``time`` — no I/O, no network, no broker calls.

Extracted from engine_core.py as Phase 4 of the engine_core split.

Dependency note
───────────────
These functions call ``_coerce_custom_human_live_state`` which now lives in
``coercion_manager``.  That import is module-level and introduces no circular
dependency.
"""
from __future__ import annotations

import hashlib
import logging
import math
import random
import threading
import time as time_module
from typing import Any

from src.trading.coercion_manager import _coerce_custom_human_live_state

_LOG = logging.getLogger("final_fantasy.engine_core")

# ---------------------------------------------------------------------------
# Polling-interval constants
# ---------------------------------------------------------------------------

CUSTOM_HUMAN_LIVE_POLL_SECONDS: float = 5.0
CUSTOM_HUMAN_LIVE_ARMED_POLL_SECONDS: float = 0.6
CUSTOM_HUMAN_LIVE_POSITION_POLL_SECONDS: float = 1.0
CUSTOM_HUMAN_LIVE_CONFIRMATION_POLL_SECONDS: float = 0.5
CUSTOM_HUMAN_LIVE_DEGRADED_POLL_SECONDS: float = 2.0

CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_POLL_SECONDS: float = 5.0
CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_ACTIVE_POLL_SECONDS: float = 2.0
CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_ARMED_POLL_SECONDS: float = 0.75

CUSTOM_HUMAN_JITTER_MIN_FACTOR: float = 0.85
CUSTOM_HUMAN_JITTER_MAX_FACTOR: float = 1.15

CUSTOM_HUMAN_BIO_POLL_AMPLITUDE: float = 0.10
CUSTOM_HUMAN_BIO_POLL_PERIOD_SECONDS: float = 240.0

CUSTOM_HUMAN_IDLE_SCROLL_NOISE_MIN_SECONDS: float = 18.0
CUSTOM_HUMAN_IDLE_SCROLL_NOISE_MAX_SECONDS: float = 45.0
CUSTOM_HUMAN_IDLE_HEALTH_CHECK_AFTER_SECONDS: float = 45.0
CUSTOM_HUMAN_RECOVERY_DUPLICATE_WINDOW_SECONDS: float = 45.0


# ---------------------------------------------------------------------------
# Phase-aware poll-second helpers
# ---------------------------------------------------------------------------

def _custom_human_live_observer_poll_seconds(state_raw: dict[str, Any] | None) -> float:
    state = _coerce_custom_human_live_state(state_raw)
    phase = str(state.get("phase", "") or "").strip().lower()
    if bool(state.get("reconcile_required", False)) or phase == "manual_reconcile":
        return float(CUSTOM_HUMAN_LIVE_DEGRADED_POLL_SECONDS)
    if str(state.get("pending_signal_id", "") or "").strip():
        return float(CUSTOM_HUMAN_LIVE_CONFIRMATION_POLL_SECONDS)
    if bool(state.get("position_open", False)):
        return float(CUSTOM_HUMAN_LIVE_POSITION_POLL_SECONDS)
    if phase in {"armed", "waiting_for_trigger", "entry_pending"}:
        return float(CUSTOM_HUMAN_LIVE_ARMED_POLL_SECONDS)
    return float(CUSTOM_HUMAN_LIVE_POLL_SECONDS)


def _custom_human_snapshot_poll_seconds(shared_state: dict[str, Any] | None) -> float:
    shared = shared_state if isinstance(shared_state, dict) else {}
    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    phase = str(live_state.get("phase", "") or "").strip().lower()
    if bool(live_state.get("position_open", False)):
        return float(CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_ACTIVE_POLL_SECONDS)
    if phase in {"armed", "waiting_for_trigger", "entry_pending"} or str(live_state.get("pending_signal_id", "") or "").strip():
        return float(CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_ARMED_POLL_SECONDS)
    return float(CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_POLL_SECONDS)


def _custom_human_jittered_interval(
    base_interval: float,
    *,
    floor: float | None = None,
    ceiling: float | None = None,
) -> float:
    interval = max(0.0, float(base_interval))
    interval *= random.uniform(CUSTOM_HUMAN_JITTER_MIN_FACTOR, CUSTOM_HUMAN_JITTER_MAX_FACTOR)
    if floor is not None:
        interval = max(float(floor), interval)
    if ceiling is not None:
        interval = min(float(ceiling), interval)
    return float(interval)


# ---------------------------------------------------------------------------
# Bio-modulation (sinusoidal polling variance)
# ---------------------------------------------------------------------------

def _seed_custom_human_bio_polling_profile(
    shared: dict[str, Any] | None,
    *,
    force_new: bool = False,
) -> None:
    """Seed a per-runtime biological polling profile for passive loops."""
    if not isinstance(shared, dict):
        return
    phase_offset = shared.get("polling_phase_offset")
    if force_new or not isinstance(phase_offset, (int, float)):
        shared["polling_phase_offset"] = random.uniform(0.0, 2.0 * math.pi)
    shared["bio_polling_enabled"] = True


def _custom_human_bio_polling_eligible(shared: dict[str, Any] | None) -> bool:
    if not isinstance(shared, dict) or not bool(shared.get("bio_polling_enabled", False)):
        return False
    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    phase = str(live_state.get("phase", "") or "").strip().lower()
    if bool(live_state.get("reconcile_required", False)) or phase == "manual_reconcile":
        return False
    if str(live_state.get("pending_signal_id", "") or "").strip():
        return False
    return True


def _get_bio_modulation(
    shared: dict[str, Any] | None,
    *,
    now_wall: float | None = None,
) -> float:
    """Return a slow sinusoidal factor for passive polling intervals."""
    if not _custom_human_bio_polling_eligible(shared):
        return 1.0
    shared = shared if isinstance(shared, dict) else {}
    phase_offset = shared.get("polling_phase_offset")
    if not isinstance(phase_offset, (int, float)):
        return 1.0
    now_value = float(now_wall if now_wall is not None else time_module.time())
    factor = 1.0 + (CUSTOM_HUMAN_BIO_POLL_AMPLITUDE * math.sin(
        (now_value / float(CUSTOM_HUMAN_BIO_POLL_PERIOD_SECONDS)) + float(phase_offset)
    ))
    return float(factor)


def _apply_custom_human_bio_modulation(
    shared: dict[str, Any] | None,
    base_interval: float,
    *,
    floor: float | None = None,
    ceiling: float | None = None,
    now_wall: float | None = None,
) -> float:
    interval = max(0.0, float(base_interval))
    interval *= _get_bio_modulation(shared, now_wall=now_wall)
    if floor is not None:
        interval = max(float(floor), interval)
    if ceiling is not None:
        interval = min(float(ceiling), interval)
    return float(interval)


# ---------------------------------------------------------------------------
# Live-feed cache bypass
# ---------------------------------------------------------------------------

def _custom_human_should_bypass_live_feed_cache(
    shared: dict[str, Any] | None,
    cached_meta: dict[str, Any] | None,
) -> bool:
    """Bypass the short runtime cache only for armed Tradovate observer data."""
    if not isinstance(shared, dict) or not isinstance(cached_meta, dict):
        return False
    source = str(cached_meta.get("source", "") or "").strip()
    if source != "Tradovate Observer":
        return False
    live_state = _coerce_custom_human_live_state(shared.get("live_state"))
    phase = str(live_state.get("phase", "") or "").strip().lower()
    return phase in {"armed", "waiting_for_trigger", "entry_pending"}


# ---------------------------------------------------------------------------
# Idle scheduler helpers
# ---------------------------------------------------------------------------

def _maybe_schedule_custom_human_idle_scroll_noise(
    shared: dict[str, Any],
    *,
    now_mono: float | None = None,
) -> bool:
    now_value = float(now_mono if now_mono is not None else time_module.monotonic())
    state = _coerce_custom_human_live_state(shared.get("live_state"))
    phase = str(state.get("phase", "") or "").strip().lower()
    eligible = (
        bool(shared.get("running"))
        and phase == "armed"
        and not bool(state.get("position_open", False))
        and not bool(state.get("reconcile_required", False))
        and not str(state.get("pending_signal_id", "") or "").strip()
    )
    if not eligible:
        shared.pop("idle_scroll_noise_next_at", None)
        return False

    next_at_raw = shared.get("idle_scroll_noise_next_at")
    next_at = float(next_at_raw) if isinstance(next_at_raw, (int, float)) else None
    if next_at is None:
        base_delay = random.uniform(
            CUSTOM_HUMAN_IDLE_SCROLL_NOISE_MIN_SECONDS,
            CUSTOM_HUMAN_IDLE_SCROLL_NOISE_MAX_SECONDS,
        )
        shared["idle_scroll_noise_next_at"] = now_value + _apply_custom_human_bio_modulation(
            shared,
            base_delay,
            floor=0.1,
        )
        return False
    if now_value < next_at:
        return False

    base_delay = random.uniform(
        CUSTOM_HUMAN_IDLE_SCROLL_NOISE_MIN_SECONDS,
        CUSTOM_HUMAN_IDLE_SCROLL_NOISE_MAX_SECONDS,
    )
    shared["idle_scroll_noise_next_at"] = now_value + _apply_custom_human_bio_modulation(
        shared,
        base_delay,
        floor=0.1,
    )
    return True


def _maybe_schedule_custom_human_idle_health_check(
    shared: dict[str, Any],
    *,
    now_mono: float | None = None,
) -> bool:
    """Schedule a forced read-only health refresh after prolonged inactivity."""
    now_value = float(now_mono if now_mono is not None else time_module.monotonic())
    state = _coerce_custom_human_live_state(shared.get("live_state"))
    phase = str(state.get("phase", "") or "").strip().lower()
    eligible = (
        bool(shared.get("running"))
        and phase == "armed"
        and not bool(state.get("position_open", False))
        and not bool(state.get("reconcile_required", False))
        and not str(state.get("pending_signal_id", "") or "").strip()
    )
    if not eligible:
        shared.pop("idle_health_check_next_at", None)
        return False

    next_at_raw = shared.get("idle_health_check_next_at")
    if isinstance(next_at_raw, (int, float)):
        next_at = float(next_at_raw)
    else:
        last_interaction_raw = shared.get("last_worker_interaction_at")
        last_interaction = float(last_interaction_raw) if isinstance(last_interaction_raw, (int, float)) else now_value
        next_at = last_interaction + _apply_custom_human_bio_modulation(
            shared,
            float(CUSTOM_HUMAN_IDLE_HEALTH_CHECK_AFTER_SECONDS),
            floor=0.1,
        )
        shared["idle_health_check_next_at"] = next_at
        return False

    if now_value < next_at:
        return False

    shared["idle_health_check_next_at"] = now_value + _apply_custom_human_bio_modulation(
        shared,
        float(CUSTOM_HUMAN_IDLE_HEALTH_CHECK_AFTER_SECONDS),
        floor=0.1,
    )
    return True


# ---------------------------------------------------------------------------
# Worker interaction + recovery tracking
# ---------------------------------------------------------------------------

def _mark_custom_human_worker_interaction(
    shared: dict[str, Any],
    *,
    now_mono: float | None = None,
) -> float:
    """Record the most recent execution-side interaction for idle health checks."""
    now_value = float(now_mono if now_mono is not None else time_module.monotonic())
    shared["last_worker_interaction_at"] = now_value
    shared["idle_health_check_next_at"] = now_value + float(CUSTOM_HUMAN_IDLE_HEALTH_CHECK_AFTER_SECONDS)
    return now_value


def _mark_custom_human_recent_recovery(
    shared: dict[str, Any],
    *,
    now_mono: float | None = None,
) -> float:
    """Track the most recent successful recovery/restart timestamp."""
    now_value = float(now_mono if now_mono is not None else time_module.monotonic())
    shared["last_auto_recovered_at"] = now_value
    return now_value


def _custom_human_is_recent_recovery(
    shared: dict[str, Any] | None,
    *,
    now_mono: float | None = None,
    window_s: float = CUSTOM_HUMAN_RECOVERY_DUPLICATE_WINDOW_SECONDS,
) -> bool:
    if not isinstance(shared, dict):
        return False
    token = shared.get("last_auto_recovered_at")
    if not isinstance(token, (int, float)):
        return False
    now_value = float(now_mono if now_mono is not None else time_module.monotonic())
    return max(0.0, now_value - float(token)) <= float(window_s)


# ---------------------------------------------------------------------------
# Duplicate-retry helpers
# ---------------------------------------------------------------------------

def _custom_human_duplicate_retry_signal_id(signal_id: str) -> str:
    base = str(signal_id or "sig").strip() or "sig"
    suffix = hashlib.sha256(f"{base}|recovery|{time_module.time_ns()}".encode("utf-8")).hexdigest()[:8]
    return f"{base}-r{suffix}"


def _custom_human_should_retry_router_duplicate(
    shared: dict[str, Any] | None,
    signal_item: dict[str, Any] | None,
) -> bool:
    if not _custom_human_is_recent_recovery(shared):
        return False
    if not isinstance(shared, dict):
        return False
    state = _coerce_custom_human_live_state(shared.get("live_state"))
    if bool(state.get("position_open", False)) or str(state.get("pending_signal_id", "") or "").strip():
        return False
    action = str((signal_item or {}).get("action", "")).strip().lower()
    return action in {"buy", "sell", "add"}


def _classify_custom_human_restart_suppression(
    shared: dict[str, Any] | None,
    *,
    row_status: str,
    row_message: str,
) -> tuple[str, str]:
    if not _custom_human_is_recent_recovery(shared):
        return str(row_status), str(row_message)
    message_text = str(row_message or "").strip()
    message_lower = message_text.lower()
    if str(row_status) == "duplicate" or "duplicate signal_id" in message_lower:
        return "restart_suppressed_signal", (
            f"Signal undertrykt under restart/recovery: {message_text or 'duplicate signal_id.'}"
        )
    if str(row_status) == "blocked_risk_gate" and "duplicate trade intent" in message_lower:
        return "restart_suppressed_intent", (
            f"Trade intent undertrykt under restart/recovery: {message_text}"
        )
    return str(row_status), message_text


# ---------------------------------------------------------------------------
# UI-reset cooldown helpers
# ---------------------------------------------------------------------------

def _should_abort_custom_human_ui_reset(
    stop_event: threading.Event,
    signal_queue: Any,
) -> bool:
    """Abort post-FLAT UI reset when a fresh signal is already queued."""
    if stop_event.is_set():
        return True
    try:
        return not bool(signal_queue.empty())
    except Exception as exc:
        _LOG.debug("Kunne ikke inspicere signal queue under UI-reset: %s", exc)
        return False


def _wait_for_custom_human_post_flat_cooldown(
    stop_event: threading.Event,
    signal_queue: Any,
    timeout_s: float,
    *,
    chunk_s: float = 0.05,
) -> None:
    """Allow post-FLAT cooldown to yield early when a new signal is queued."""
    remaining = max(float(timeout_s), 0.0)
    while remaining > 0.0 and not stop_event.is_set():
        if _should_abort_custom_human_ui_reset(stop_event, signal_queue):
            return
        wait_chunk = min(float(chunk_s), remaining)
        started_at = float(time_module.monotonic())
        stop_event.wait(timeout=wait_chunk)
        elapsed = max(float(time_module.monotonic()) - started_at, 0.0)
        remaining -= elapsed if elapsed > 0.0 else wait_chunk
