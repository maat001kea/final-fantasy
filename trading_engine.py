from __future__ import annotations

import argparse
import hashlib
import logging
import os
import random
import shutil
import socket
import subprocess
import time as time_module
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from src.custom_types import coerce_custom_strategy_config
from src.trading.cdp_adapter import CDP_PORT, resolve_cdp_port
from src.trading import engine_core
from src.trading.runtime_control import probe_cdp_endpoint
from src.trading.state_buffer import StateVerificationBuffer, _SENTINEL as _STATE_VERIFICATION_SENTINEL
from src.trading_engine_bridge import claim_commands, init_bridge, publish_status, scrub_stale_commands


LOGGER = logging.getLogger("final_fantasy.trading_engine")
if not LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    LOGGER.addHandler(_handler)
    LOGGER.setLevel(logging.INFO)

BRIDGE_DB_PATH = Path(__file__).resolve().parent / "output" / "trading_engine_bridge.sqlite3"
ENGINE_POLL_SECONDS = 0.5
ENGINE_HEARTBEAT_SECONDS = 2.0
ENGINE_AUTO_HEAL_INTERVAL_SECONDS = 5.0
ENGINE_HALT_REASON_DEFAULT = "Kritisk engine-fejl. Nye trading-kommandoer er blokeret."

# --- Pitbull Reconnection ---
PITBULL_MAX_ATTEMPTS = 5
PITBULL_BACKOFF_SECONDS = (1.0, 2.0, 4.0, 8.0, 16.0)

# --- Watchdog Chrome auto-spawn ---
CHROME_SPAWN_FLAGS = [
    "--remote-debugging-port={port}",
    "--user-data-dir=chrome_profile",
    # Identity & first-run
    "--no-first-run",
    "--no-default-browser-check",
    # Pop-ups / notifications / info-bars – prevent dialogs breaking CDP
    "--disable-popup-blocking",
    "--disable-notifications",
    "--disable-infobars",
    # Background throttling – keep observer loop running at full speed
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    # Stability
    "--disable-crash-reporter",
    # Anti-bot detection hardening
    "--disable-blink-features=AutomationControlled",
]
CHROME_SOCKET_TIMEOUT = 1.0
WATCHDOG_BOOTSTRAP_TIMEOUT_SECONDS = 10.0
WATCHDOG_POLL_INTERVAL_SECONDS = 0.25

# --- Stealth Jitter ---
# Random micro-delay between engine poll ticks to prevent a machine-like
# sawtooth request pattern that broker anti-bot systems could fingerprint.
ENGINE_POLL_JITTER_MIN = 0.05
ENGINE_POLL_JITTER_MAX = 0.15


def _bridge_path(raw: str | Path | None = None) -> Path:
    # Always resolve to an absolute path so that the DB is reachable even when
    # the engine is spawned as a subprocess with a different working directory.
    if raw is not None:
        p = Path(raw)
        return p.resolve() if not p.is_absolute() else p
    return BRIDGE_DB_PATH


def _is_port_open(host: str, port: int, *, timeout: float = CHROME_SOCKET_TIMEOUT) -> bool:
    """Return True if a TCP connection to *host*:*port* can be established."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _spawn_chrome_if_needed(port: int) -> bool:
    """Fire-and-forget Chrome spawn when the CDP port is not responding.

    Returns True if a spawn was attempted, False if Chrome was already up.
    The engine does NOT wait for Chrome to finish loading – this is intentional
    so that the Pitbull retry loop handles the eventual successful connection.
    """
    if _is_port_open("127.0.0.1", port):
        return False
    flags = [flag.format(port=port) for flag in CHROME_SPAWN_FLAGS]
    chrome_candidates = [
        "google-chrome",
        "google-chrome-stable",
        "chromium-browser",
        "chromium",
        "brave-browser",
        "brave",
    ]
    for candidate in chrome_candidates:
        try:
            subprocess.Popen(
                [candidate] + flags,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
            LOGGER.info("[GHOST-V6.6] Watchdog: Chrome spawn attempted via '%s' on port %s.", candidate, port)
            return True
        except FileNotFoundError:
            continue
        except OSError as exc:
            LOGGER.debug("Watchdog: could not spawn '%s': %s", candidate, exc)
            continue
    LOGGER.warning(
        "[GHOST-V6.6] Watchdog: Chrome not found on PATH. Port %s unresponsive. Manual start may be required.", port
    )
    return False


def ensure_chrome_running(port: int = 9255) -> None:
    """Ensure a Chrome/Brave instance is listening on the requested CDP port."""
    port = int(port)
    if _is_port_open("127.0.0.1", port):
        LOGGER.info("[GHOST-V6.6] Watchdog: Chrome already listening on port %s.", port)
        return
    LOGGER.info("[GHOST-V6.6] Watchdog: Port %s unresponsive. Bootstrapping Chrome.", port)
    _spawn_chrome_if_needed(port)
    deadline = time_module.monotonic() + WATCHDOG_BOOTSTRAP_TIMEOUT_SECONDS
    while time_module.monotonic() < deadline:
        if _is_port_open("127.0.0.1", port):
            LOGGER.info("[GHOST-V6.6] Watchdog: Chrome responsive on port %s.", port)
            return
        time_module.sleep(WATCHDOG_POLL_INTERVAL_SECONDS)
    LOGGER.error("[GHOST-V6.6] Watchdog: Chrome bootstrap failed on port %s.", port)
    raise SystemExit("Watchdog: Chrome Bootstrap Failed")


def _pitbull_probe_with_backoff(port: int) -> dict[str, Any]:
    """Probe the CDP endpoint with exponential back-off retries.

    Attempts: 1 .. PITBULL_MAX_ATTEMPTS.
    Back-off delays between attempts: PITBULL_BACKOFF_SECONDS.

    Returns the last probe result dict (``{"ok": True/False, ...}``).
    Only after all attempts fail is the returned probe considered fatal.
    """
    last_probe: dict[str, Any] = {"ok": False}
    for attempt in range(1, PITBULL_MAX_ATTEMPTS + 1):
        last_probe = probe_cdp_endpoint(port)
        if bool(last_probe.get("ok", False)):
            LOGGER.debug("Pitbull: probe OK på forsøg %d/%d.", attempt, PITBULL_MAX_ATTEMPTS)
            return last_probe
        error_msg = str(last_probe.get("error", "ukendt fejl") or "ukendt fejl")
        if attempt < PITBULL_MAX_ATTEMPTS:
            delay = PITBULL_BACKOFF_SECONDS[attempt - 1]
            LOGGER.debug(
                "Pitbull: probe fejlede (forsøg %d/%d), venter %.1fs. Fejl: %s",
                attempt,
                PITBULL_MAX_ATTEMPTS,
                delay,
                error_msg,
            )
            time_module.sleep(delay)
        else:
            LOGGER.debug(
                "Pitbull: probe fejlede (forsøg %d/%d). Opgiver. Fejl: %s",
                attempt,
                PITBULL_MAX_ATTEMPTS,
                error_msg,
            )
    return last_probe


def _set_engine_fault(shared: dict[str, Any], code: str, reason: str, *, halt: bool) -> None:
    code_token = str(code or "ERROR").strip().upper() or "ERROR"
    reason_text = str(reason or ENGINE_HALT_REASON_DEFAULT).strip() or ENGINE_HALT_REASON_DEFAULT
    shared["engine_status_override"] = code_token
    shared["last_error"] = reason_text
    shared["last_result"] = reason_text
    shared["halted"] = bool(halt)
    shared["halt_reason"] = reason_text if halt else ""
    shared["halt_code"] = code_token if halt else ""
    shared["connected"] = False
    if bool(halt) and bool(shared.get("running", False)):
        try:
            engine_core._stop_custom_human_auto_runtime(
                shared,
                user_initiated=False,
                reason=f"HALT: {reason_text}",
            )
        except Exception:
            pass


def _clear_engine_fault(shared: dict[str, Any]) -> None:
    shared["engine_status_override"] = ""
    shared["last_error"] = ""
    shared["halted"] = False
    shared["halt_reason"] = ""
    shared["halt_code"] = ""


def _is_cdp_failure(exc: BaseException) -> bool:
    if isinstance(exc, engine_core.CDPConnectionError):
        return True
    message = str(exc or "").strip().lower()
    return "chrome" in message or "cdp" in message or "connect to host" in message


def _coerce_time_token(raw: Any, fallback: time) -> time:
    if isinstance(raw, time):
        return raw
    token = str(raw or "").strip()
    if not token:
        return fallback
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return time.fromisoformat(token) if fmt == "%H:%M:%S" else time.fromisoformat(f"{token}:00")
        except ValueError:
            continue
    return fallback


def _command_state(payload: dict[str, Any]) -> dict[str, Any]:
    runtime_profile = dict(payload.get("runtime_profile") or {})
    runtime_config = dict(payload.get("runtime_config") or {})
    return {
        engine_core._custom_human_store_key("runtime_profile"): runtime_profile,
        engine_core._custom_human_store_key("config"): runtime_config,
        engine_core._custom_human_store_key("active"): bool(payload.get("runtime_active", False)),
        "sa_custom_human_selector_mode": str(
            payload.get("selector_mode", "Auto (platform selectors)")
        ).strip()
        or "Auto (platform selectors)",
        "sa_custom_human_buy_selector": str(payload.get("buy_selector", "") or "").strip(),
        "sa_custom_human_sell_selector": str(payload.get("sell_selector", "") or "").strip(),
        "sa_custom_human_flatten_selector": str(payload.get("flatten_selector", "") or "").strip(),
        "sa_custom_human_expected_account_token": str(
            payload.get("expected_account_token", "") or ""
        ).strip(),
        "sa_app_timezone": str(payload.get("timezone_name", "Europe/Copenhagen") or "Europe/Copenhagen"),
        "sa_overnight_range_start_dk": _coerce_time_token(
            payload.get("overnight_start_dk"),
            time(0, 0),
        ),
        "sa_overnight_range_end_dk": _coerce_time_token(
            payload.get("overnight_end_dk"),
            time(8, 0),
        ),
    }


def _expected_account_tokens_from_state(state: dict[str, Any]) -> tuple[str, ...]:
    raw = str(state.get("sa_custom_human_expected_account_token", "") or "").strip()
    if not raw:
        return ()
    tokens: list[str] = []
    for separator in ("|", "\n", ";"):
        raw = raw.replace(separator, ",")
    for token in raw.split(","):
        cleaned = str(token).strip()
        if cleaned and cleaned not in tokens:
            tokens.append(cleaned)
    return tuple(tokens)


def _build_tradovate_snapshot_cfg(state: dict[str, Any]) -> dict[str, Any]:
    runtime_profile = state.get(engine_core._custom_human_store_key("runtime_profile"), {})
    if not isinstance(runtime_profile, dict):
        runtime_profile = {}
    instrument = str(
        runtime_profile.get("ticker")
        or runtime_profile.get("contract_symbol")
        or runtime_profile.get("instrument")
        or "MYM"
    ).strip()
    return {
        "enabled": True,
        "instrument": instrument,
        "expected_account_tokens": list(_expected_account_tokens_from_state(state)),
        "poll_seconds": float(engine_core.CUSTOM_HUMAN_TRADOVATE_SNAPSHOT_POLL_SECONDS),
    }


def _build_live_observer_cfg(payload: dict[str, Any]) -> dict[str, Any] | None:
    state = _command_state(payload)
    runtime_profile = state.get(engine_core._custom_human_store_key("runtime_profile"), {})
    config_raw = state.get(engine_core._custom_human_store_key("config"), {})
    runtime_active = bool(state.get(engine_core._custom_human_store_key("active"), False)) and engine_core._custom_human_runtime_profile_is_valid(
        runtime_profile
    )
    if not runtime_active:
        return None
    if not isinstance(runtime_profile, dict) or not isinstance(config_raw, dict):
        return None

    cfg = coerce_custom_strategy_config({**config_raw, "instrument": "DOW", "contract_symbol": "MYM"})
    if str(cfg.execution_model).strip() != "Aggressiv":
        return {
            "enabled": False,
            "message": "Live observer understøtter kun School Run + Aggressiv i denne version.",
        }

    repo_root = Path(__file__).resolve().parent
    live_cfg = engine_core._load_live_execution_config(repo_root=repo_root)
    account_cfg = dict(payload.get("risk_gate_account_config") or {})
    if not account_cfg:
        account_cfg = engine_core._load_yaml_mapping(repo_root / "config" / "account_config.yaml")
    account_balance = float(account_cfg.get("starting_balance", 1000.0) or 1000.0)
    kill_switch = bool(payload.get("kill_switch", False))
    webhook_url = str(payload.get("webhook_url", live_cfg.get("webhook_url", "")) or "").strip()

    pipeline = engine_core._build_trading_pipeline(
        account_balance=account_balance,
        account_config_override=account_cfg,
    )
    persisted_runtime = engine_core._load_custom_human_runtime_state()
    persisted_gate_state = (
        persisted_runtime.get("risk_gate_state") if isinstance(persisted_runtime, dict) else None
    )
    risk_gate = getattr(pipeline, "risk_gate", None)
    if isinstance(persisted_gate_state, dict) and risk_gate is not None and hasattr(risk_gate, "restore_state"):
        try:
            risk_gate.restore_state(persisted_gate_state)
        except Exception:
            pass

    router = engine_core._build_signal_router_runtime(
        repo_root=repo_root,
        live_cfg=live_cfg,
        webhook_url=webhook_url,
    )
    runtime_profile_payload = dict(runtime_profile)
    runtime_profile_payload.setdefault("strategy_name", "School Run")
    runtime_profile_payload.setdefault("bar1_start", str(cfg.bar1_start))
    runtime_profile_payload.setdefault("ticker", "MYM")
    runtime_profile_payload.setdefault("contract_symbol", "MYM")
    return {
        "enabled": True,
        "config": cfg,
        "runtime_profile": runtime_profile_payload,
        "pipeline": pipeline,
        "router": router,
        "live_cfg": dict(live_cfg),
        "kill_switch": bool(kill_switch),
        "timezone_name": str(payload.get("timezone_name", "Europe/Copenhagen") or "Europe/Copenhagen"),
        "overnight_start_dk": _coerce_time_token(payload.get("overnight_start_dk"), time(0, 0)),
        "overnight_end_dk": _coerce_time_token(payload.get("overnight_end_dk"), time(8, 0)),
        "session_close_dk": engine_core._custom_human_session_close_dk(str(cfg.instrument)),
        "poll_seconds": float(engine_core.CUSTOM_HUMAN_LIVE_POLL_SECONDS),
        "stale_after_seconds": float(engine_core.CUSTOM_HUMAN_LIVE_STALE_SECONDS),
        "risk_gate_account_config": dict(account_cfg),
    }


def _refresh_shared_context(
    shared: dict[str, Any],
    payload: dict[str, Any],
    *,
    include_live_observer: bool = True,
) -> dict[str, Any]:
    state = _command_state(payload)
    runtime_profile = state.get(engine_core._custom_human_store_key("runtime_profile"), {})
    runtime_config = state.get(engine_core._custom_human_store_key("config"), {})
    runtime_active = bool(state.get(engine_core._custom_human_store_key("active"), False)) and engine_core._custom_human_runtime_profile_is_valid(
        runtime_profile
    )
    shared["runtime_profile"] = dict(runtime_profile) if isinstance(runtime_profile, dict) else {}
    shared["runtime_config"] = dict(runtime_config) if isinstance(runtime_config, dict) else {}
    shared["runtime_active"] = bool(runtime_active)
    shared["expected_account_tokens"] = _expected_account_tokens_from_state(state)
    shared["selector_mode"] = str(state.get("sa_custom_human_selector_mode", "") or "").strip()
    shared["tradovate_snapshot_cfg"] = _build_tradovate_snapshot_cfg(state)
    if include_live_observer:
        shared["live_observer_cfg"] = _build_live_observer_cfg(payload)
    else:
        shared.setdefault("live_observer_cfg", None)
    shared["debug_port"] = resolve_cdp_port(payload.get("debug_port", CDP_PORT))
    return state


def _build_auto_rearm_payload(shared: dict[str, Any]) -> dict[str, Any]:
    persisted = engine_core._load_custom_human_runtime_state() or {}
    runtime_profile = dict(shared.get("runtime_profile")) if isinstance(shared.get("runtime_profile"), dict) else {}
    if not runtime_profile and isinstance(persisted.get("runtime_profile"), dict):
        runtime_profile = dict(persisted.get("runtime_profile") or {})
    runtime_config = dict(shared.get("runtime_config")) if isinstance(shared.get("runtime_config"), dict) else {}
    if not runtime_config and isinstance(persisted.get("config"), dict):
        runtime_config = dict(persisted.get("config") or {})
    runtime_active = bool(shared.get("runtime_active", False))
    if not runtime_active:
        runtime_active = bool(persisted.get("active", False)) and engine_core._custom_human_runtime_profile_is_valid(
            runtime_profile
        )

    expected_tokens = list(shared.get("expected_account_tokens") or persisted.get("expected_account_tokens") or ())
    adapter = shared.get("adapter")
    platform = str(getattr(adapter, "platform", "") or "tradovate").strip().lower() or "tradovate"
    debug_port = resolve_cdp_port(getattr(adapter, "cdp_port", shared.get("debug_port", CDP_PORT)))
    buy_info = shared.get("buy_info")
    sell_info = shared.get("sell_info")
    flat_info = shared.get("flat_info")
    return {
        "platform": platform,
        "debug_port": debug_port,
        "runtime_profile": runtime_profile,
        "runtime_config": runtime_config,
        "runtime_active": bool(runtime_active),
        "selector_mode": str(shared.get("selector_mode", "") or "Auto (platform selectors)").strip()
        or "Auto (platform selectors)",
        "buy_selector": str((buy_info or {}).get("selector", "") or "").strip(),
        "sell_selector": str((sell_info or {}).get("selector", "") or "").strip(),
        "flatten_selector": str((flat_info or {}).get("selector", "") or "").strip(),
        "expected_account_token": ",".join(str(token).strip() for token in expected_tokens if str(token).strip()),
    }


def _maybe_self_heal_auto_runtime(shared: dict[str, Any]) -> dict[str, Any] | None:
    if not bool(shared.get("auto_requested", False)) or bool(shared.get("halted", False)):
        return None

    now = time_module.monotonic()
    last_attempt = float(shared.get("last_auto_heal_attempt_at", 0.0) or 0.0)
    if now - last_attempt < ENGINE_AUTO_HEAL_INTERVAL_SECONDS:
        return None
    shared["last_auto_heal_attempt_at"] = now

    adapter = shared.get("adapter")
    if not isinstance(adapter, engine_core.CDPHumanAdapter) or not bool(adapter.is_connected):
        return None

    shared["connected"] = True
    payload = _build_auto_rearm_payload(shared)
    needs_runtime = not bool(shared.get("running", False))
    needs_targets = not (shared.get("buy_info") or shared.get("sell_info"))
    preflight_snapshot = shared.get("last_dom_preflight")
    preflight_ok = bool(isinstance(preflight_snapshot, dict) and preflight_snapshot.get("success", False))
    needs_preflight = not preflight_ok
    observer_cfg_missing = not isinstance(shared.get("live_observer_cfg"), dict)
    loops_degraded = bool(shared.get("running", False)) and (
        not bool(shared.get("tradovate_snapshot_running", False))
        or (
            isinstance(shared.get("live_observer_cfg"), dict)
            and bool(shared["live_observer_cfg"].get("enabled", False))
            and not bool(shared.get("live_observer_running", False))
        )
    )

    if not any((needs_runtime, needs_targets, needs_preflight, observer_cfg_missing, loops_degraded)):
        return None

    if needs_runtime:
        result = _attempt_auto_rearm_after_connect(shared, payload, adapter)
        shared["last_auto_heal_result"] = result
        return result

    state = _refresh_shared_context(shared, payload, include_live_observer=True)
    prepared: dict[str, Any] | None = None
    if needs_targets or needs_preflight:
        prepared = engine_core._prepare_custom_human_auto_targets(adapter, shared, session_state=state)
        if not bool(prepared.get("ok", False)):
            result = {
                "ok": False,
                "status": str(prepared.get("status", "prepare_failed") or "prepare_failed"),
                "message": str(prepared.get("message", "Auto targets kunne ikke opdateres.")).strip()
                or "Auto targets kunne ikke opdateres.",
                "prepared": prepared,
            }
            shared["last_auto_heal_result"] = result
            return result

    if observer_cfg_missing or loops_degraded:
        engine_core._ensure_custom_human_supervisor_running(shared)
        supervisor = engine_core._custom_human_supervisor_tick(shared)
        if bool(supervisor.get("ok", False)):
            engine_core._persist_custom_human_runtime_state(shared)
        result = {
            "ok": bool(supervisor.get("ok", False)),
            "status": str(supervisor.get("reason", "healthy") or "healthy"),
            "message": str(supervisor.get("detail", "") or "").strip(),
            "prepared": prepared,
            "supervisor": supervisor,
        }
        shared["last_auto_heal_result"] = result
        return result

    if prepared is not None and bool(prepared.get("ok", False)):
        engine_core._persist_custom_human_runtime_state(shared)
        result = {
            "ok": True,
            "status": "targets_refreshed",
            "message": "Auto-targets og preflight opdateret.",
            "prepared": prepared,
        }
        shared["last_auto_heal_result"] = result
        return result

    return None


def _ensure_adapter(
    shared: dict[str, Any],
    payload: dict[str, Any],
    *,
    ensure_watchdog: bool = True,
) -> engine_core.CDPHumanAdapter:
    platform = str(payload.get("platform", "tradovate") or "tradovate").strip().lower()
    debug_port = resolve_cdp_port(payload.get("debug_port", CDP_PORT))
    adapter = shared.get("adapter")
    current_port = int(getattr(adapter, "cdp_port", -1)) if adapter is not None else -1
    current_platform = str(getattr(adapter, "platform", "") or "").strip().lower()
    if (
        not isinstance(adapter, engine_core.CDPHumanAdapter)
        or current_port != debug_port
        or current_platform != platform
    ):
        if isinstance(adapter, engine_core.CDPHumanAdapter):
            try:
                adapter.shutdown_sync()
            except Exception:
                pass
        adapter = engine_core.CDPHumanAdapter(
            platform=platform,
            username="",
            password="",
            cdp_port=debug_port,
        )
        shared["adapter"] = adapter
    if not bool(adapter.is_connected):
        if ensure_watchdog:
            ensure_chrome_running(debug_port)
        LOGGER.info(
            "Engine: Starting adapter on port %s for platform %s.",
            debug_port,
            platform,
        )
        engine_core._run_cdp_adapter_task(adapter, adapter.connect)
    shared["debug_port"] = debug_port
    shared["connected"] = bool(adapter.is_connected)
    return adapter


def _disconnect_adapter(shared: dict[str, Any]) -> None:
    adapter = shared.get("adapter")
    if isinstance(adapter, engine_core.CDPHumanAdapter):
        try:
            adapter.shutdown_sync()
        except Exception:
            pass
    shared["adapter"] = None
    shared["connected"] = False


def _command_result(message: str, *, ok: bool = True, status: str = "ok", **extra: Any) -> dict[str, Any]:
    payload = {"ok": bool(ok), "status": str(status), "message": str(message)}
    payload.update(extra)
    return payload


def _engine_status(shared: dict[str, Any]) -> str:
    override = str(shared.get("engine_status_override", "") or "").strip().upper()
    if override:
        return override
    if bool(shared.get("halted", False)):
        return "HALTED"
    if str(shared.get("last_error", "") or "").strip():
        return "error"
    if bool(shared.get("connected", False)):
        return "connected"
    if bool(shared.get("running", False)):
        return "running"
    return "idle"


def _publish_engine_status(
    bridge_path: str | Path,
    shared: dict[str, Any],
    *,
    log_message: str | None = None,
) -> None:
    publish_status(bridge_path, _status_payload(shared))
    if log_message:
        LOGGER.info("%s", log_message)


def _log_pending_buffer_states(shared: dict[str, Any]) -> None:
    """Log DEBUG messages for any Mistrust Factor buffers that are mid-confirmation.

    This is called on every heartbeat so operators can see exactly when the
    engine is waiting for extra browser readings before trusting a state change.
    """
    for key, label in (("_position_buffer", "position"), ("_account_buffer", "account")):
        buf = shared.get(key)
        if not isinstance(buf, StateVerificationBuffer):
            continue
        info = buf.get_pending_info()
        pending = info["pending"]
        confirmed = info["confirmed"]
        count = info["count"]
        threshold = info["threshold"]
        if pending is _STATE_VERIFICATION_SENTINEL or pending == confirmed:
            continue  # nothing in flight
        LOGGER.debug(
            "Mistrust Factor [%s]: pending '%s' (confirmed '%s'), waiting for confirmation (%d/%d)",
            label,
            pending,
            confirmed if confirmed is not _STATE_VERIFICATION_SENTINEL else "none",
            count,
            threshold,
        )


def _handle_manual_click(shared: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    adapter = _ensure_adapter(shared, payload)
    signal = str(payload.get("signal", "") or "").strip().upper()
    selector_mode = str(payload.get("selector_mode", "Auto (platform selectors)") or "").strip()
    quantity = int(max(1, int(payload.get("quantity", 1) or 1)))
    if signal in {"BUY", "SELL"}:
        try:
            engine_core._run_cdp_adapter_task(adapter, lambda q=quantity: adapter.sync_order_quantity(q))
        except Exception:
            pass
    if selector_mode == "Manuelle koordinater":
        cx = float(payload.get("click_x", 0) or 0)
        cy = float(payload.get("click_y", 0) or 0)
        margin_pct = float(payload.get("offset_margin", 0.0) or 0.0) / 100.0
        engine_core._run_cdp_adapter_task(
            adapter,
            lambda: adapter.human_click_at(cx, cy, margin_pct=margin_pct),
        )
        shared["last_result"] = f"Manual {signal or 'CLICK'} udført på ({cx:.0f}, {cy:.0f})."
        return _command_result(shared["last_result"])

    if selector_mode == "CSS Selector":
        selector_key = {
            "BUY": "buy_selector",
            "SELL": "sell_selector",
            "FLAT": "flatten_selector",
        }.get(signal, "")
        selector = str(payload.get(selector_key, "") or "").strip()
        if not selector:
            raise RuntimeError(f"Mangler selector for {signal or 'manual click'}.")
        found = engine_core._run_cdp_adapter_task(
            adapter,
            lambda s=selector: adapter.click_element(s, jitter_px=2.0),
        )
        if not found:
            raise RuntimeError(f"Element ikke fundet: `{selector}`.")
        shared["last_result"] = f"Manual {signal} udført via selector `{selector}`."
        return _command_result(shared["last_result"])

    if signal == "FLAT":
        engine_core._run_cdp_adapter_task(adapter, adapter.close_all_positions)
        shared["last_result"] = "Manual FLAT udført via platform close selectors."
        return _command_result(shared["last_result"])

    # Use the confirmed Tradovate CSS class selectors (user-verified):
    #   BUY / ADD  → div.btn.btn-success
    #   SELL       → div.btn.btn-danger  (exclude panic button)
    #   EXIT/FLAT  → button.btn.btn-default (non-dropdown)
    # These are registered in _TRADOVATE_SELECTOR_BUNDLES so click_element
    # uses the full fallback chain automatically.
    auto_selectors = (
        [
            "div.btn.btn-success",
            "div.btn-success",
        ]
        if signal == "BUY"
        else [
            "div.btn.btn-danger:not(.panic-button)",
            "div.btn.btn-danger",
        ]
    )
    for selector in auto_selectors:
        try:
            found = engine_core._run_cdp_adapter_task(
                adapter,
                lambda s=selector: adapter.click_element(s, jitter_px=4.0),
            )
        except Exception:
            found = False
        if found:
            shared["last_result"] = f"Manual {signal} udført via selector `{selector}`."
            return _command_result(shared["last_result"])
    raise RuntimeError(f"Kunne ikke finde knap for {signal} — tjek at Tradovate-chartet er synligt.")


def _handle_start(shared: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    state = _refresh_shared_context(shared, payload, include_live_observer=True)
    runtime_profile = state.get(engine_core._custom_human_store_key("runtime_profile"), {})
    runtime_active = bool(state.get(engine_core._custom_human_store_key("active"), False)) and engine_core._custom_human_runtime_profile_is_valid(
        runtime_profile
    )
    if not runtime_active:
        raise RuntimeError("Ingen aktiv Custom Human runtime-profil er armed.")

    adapter = _ensure_adapter(shared, payload)
    prepared = engine_core._prepare_custom_human_auto_targets(adapter, shared, session_state=state)
    if not bool(prepared.get("ok", False)):
        raise RuntimeError(
            str(prepared.get("message", "Auto-start preflight fejlede.")).strip()
            or "Auto-start preflight fejlede."
        )

    shared["adapter"] = adapter
    shared["connected"] = bool(adapter.is_connected)
    shared["auto_requested"] = True
    shared["last_stop_reason"] = ""
    shared["auto_restart_attempts"] = 0
    start_payload = engine_core._start_custom_human_runtime_components(
        adapter,
        shared,
        reset_signal_queue=True,
    )
    engine_core._ensure_custom_human_supervisor_running(shared)
    engine_core._persist_custom_human_runtime_state(shared)
    _clear_engine_fault(shared)
    message = (
        "Auto-trading startet"
        f" | Observer: {start_payload.get('observer_label', 'Klik-worker only')}"
        f" | Snapshot: {start_payload.get('snapshot_label', 'Tradovate snapshot manuel')}"
    )
    shared["last_result"] = message
    return _command_result(message, prepared=prepared, start_payload=start_payload)


def _attempt_auto_rearm_after_connect(
    shared: dict[str, Any],
    payload: dict[str, Any],
    adapter: engine_core.CDPHumanAdapter,
) -> dict[str, Any]:
    if not bool(shared.get("auto_requested", False)):
        return {"ok": False, "status": "not_requested", "message": "Auto er ikke requested."}

    state = _refresh_shared_context(shared, payload, include_live_observer=True)
    runtime_profile = state.get(engine_core._custom_human_store_key("runtime_profile"), {})
    runtime_active = bool(state.get(engine_core._custom_human_store_key("active"), False)) and engine_core._custom_human_runtime_profile_is_valid(
        runtime_profile
    )
    if not runtime_active:
        engine_core._disable_invalid_custom_human_auto_request(
            shared,
            reason="Ingen aktiv Custom Human runtime-profil er armed.",
        )
        return {
            "ok": False,
            "status": "invalid_runtime",
            "message": "Ingen aktiv Custom Human runtime-profil er armed.",
        }

    if bool(shared.get("running", False)):
        engine_core._ensure_custom_human_supervisor_running(shared)
        return {"ok": True, "status": "already_running", "message": "Auto kører allerede."}

    prepared = engine_core._prepare_custom_human_auto_targets(adapter, shared, session_state=state)
    if not bool(prepared.get("ok", False)):
        return {
            "ok": False,
            "status": str(prepared.get("status", "prepare_failed") or "prepare_failed"),
            "message": str(prepared.get("message", "Auto kunne ikke genoprettes.")).strip()
            or "Auto kunne ikke genoprettes.",
            "prepared": prepared,
        }

    shared["adapter"] = adapter
    shared["connected"] = bool(adapter.is_connected)
    engine_core._ensure_custom_human_supervisor_running(shared)
    result = engine_core._custom_human_supervisor_tick(shared)
    if not bool(result.get("ok", False)):
        return {
            "ok": False,
            "status": str(result.get("reason", "rearm_failed") or "rearm_failed"),
            "message": str(result.get("detail", "Auto kunne ikke genoprettes.")).strip()
            or "Auto kunne ikke genoprettes.",
            "prepared": prepared,
            "supervisor": result,
        }

    engine_core._persist_custom_human_runtime_state(shared)
    message = "Auto-mode genoprettet."
    detail = str(result.get("detail", "") or "").strip()
    if detail:
        message = f"{message} {detail}"
    return {
        "ok": True,
        "status": str(result.get("reason", "rearmed") or "rearmed"),
        "message": message,
        "prepared": prepared,
        "supervisor": result,
    }


def _handle_set_position_metadata(shared: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    """Inject trade metadata for an open position orphaned during a reconnect.

    Called via the ``SET_POSITION_METADATA`` bridge command when the engine
    detects a live position (manual_reconcile phase) but has no entry_price /
    stop_price / risk_pts because the snapshot-confirmation loop was interrupted.

    The handler writes the caller-supplied values directly into live_state,
    clears reconcile_required, transitions phase to ``open_position`` and
    persists the state so the snapshot-trail and add-candidate loops can
    resume immediately without a restart.
    """
    live_state = engine_core._coerce_custom_human_live_state(shared.get("live_state"))
    if not bool(live_state.get("position_open", False)):
        return _command_result(
            "Ingen åben position – metadata kan ikke injiceres.",
            ok=False,
            status="no_position",
        )
    try:
        entry_price = float(payload["entry_price"])
        stop_price = float(payload["stop_price"])
        risk_pts = float(payload["risk_pts"])
        direction = str(payload.get("direction", "")).strip().lower()
    except (KeyError, TypeError, ValueError) as exc:
        return _command_result(
            f"Ugyldig metadata-payload: {exc}",
            ok=False,
            status="invalid_payload",
        )
    if direction not in ("long", "short"):
        return _command_result(
            f"Ugyldig direction '{direction}' – brug 'long' eller 'short'.",
            ok=False,
            status="invalid_payload",
        )
    live_state["entry_price"] = entry_price
    live_state["stop_price"] = stop_price
    live_state["active_stop"] = stop_price          # start trail at initial stop
    live_state["risk_pts"] = risk_pts
    live_state["direction"] = direction
    live_state["reconcile_required"] = False
    live_state["phase"] = "open_position"
    live_state["break_even_armed"] = False
    live_state["add_count_sent"] = 0
    live_state["max_favorable_pts"] = 0.0
    live_state["max_adverse_pts"] = 0.0
    if live_state.get("entry_bar_index") is None:
        live_state["entry_bar_index"] = live_state.get("start_bar")
    live_state["last_note"] = (
        f"Trade-metadata injiceret: {direction} @ {entry_price:.1f}, "
        f"stop {stop_price:.1f}, risk {risk_pts:.1f} pts."
    )
    shared["live_state"] = live_state
    engine_core._persist_custom_human_runtime_state(shared)
    msg = f"Metadata sat: {direction} @ {entry_price:.1f} | stop={stop_price:.1f} | risk={risk_pts:.1f} pts."
    shared["last_result"] = f"✅ {msg}"
    _LOG.info("[SET_POSITION_METADATA] %s", msg)
    return _command_result(msg, ok=True, status="metadata_set")


def _build_manual_signal_payload(shared: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    signal = str(payload.get("signal", "") or "").strip().upper()
    if signal not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported manual signal '{signal}'.")

    runtime_profile = dict(shared.get("runtime_profile")) if isinstance(shared.get("runtime_profile"), dict) else {}
    tradovate_snapshot = (
        dict(shared.get("tradovate_snapshot")) if isinstance(shared.get("tradovate_snapshot"), dict) else {}
    )

    action = "buy" if signal == "BUY" else "sell"
    instrument = (
        str(payload.get("instrument", "") or "").strip().upper()
        or str(runtime_profile.get("ticker") or runtime_profile.get("contract_symbol") or "").strip().upper()
        or str(tradovate_snapshot.get("instrument_root", "") or "").strip().upper()
        or "MYM"
    )

    quantity = payload.get("quantity")
    if quantity is None:
        quantity = (
            engine_core._safe_float(tradovate_snapshot.get("order_quantity_value"))
            or engine_core._safe_float(runtime_profile.get("fixed_contracts"))
            or 1.0
        )
    quantity = max(1, int(round(float(quantity))))

    trade_date = str((shared.get("live_state") or {}).get("trade_date", "") or date.today().isoformat()).strip()
    signal_id = hashlib.sha256(
        f"manual_ui:{signal}:{instrument}:{quantity}:{time_module.time_ns()}".encode("utf-8")
    ).hexdigest()[:24]

    manual_payload = dict(payload)
    manual_payload.update(
        {
            "signal": signal,
            "action": action,
            "event": str(payload.get("event", "") or "manual_ui").strip().lower(),
            "signal_id": str(payload.get("signal_id", "") or signal_id).strip(),
            "instrument": instrument,
            "quantity": quantity,
            "trade_date": trade_date,
        }
    )
    return manual_payload


def _handle_clear_riskgate_cache(shared: dict[str, Any]) -> dict[str, Any]:
    """Bridge handler for CLEAR_RISKGATE_CACHE command.

    Delegates to ``engine_core._clear_custom_human_riskgate_cache`` which
    clears the live RiskGate idempotency key set, updates persisted state and
    resets any manual-reconcile flags in one atomic operation.
    """
    result = engine_core._clear_custom_human_riskgate_cache(shared)
    n = int(result.get("cleared_keys", 0))
    rec = bool(result.get("reconcile_cleared", False))
    parts: list[str] = [f"{n} idempotency-nøgle(r) renset fra RiskGate-hukommelsen."]
    if rec:
        parts.append("Manuel reconcile-blokering fjernet.")
    message = " ".join(parts)
    shared["last_result"] = f"🗑️ {message}"
    return _command_result(message, ok=True, status="riskgate_cleared", result=result)


def _handle_refresh(shared: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    include_live_observer = bool(shared.get("auto_requested", False) or shared.get("running", False))
    _refresh_shared_context(shared, payload, include_live_observer=include_live_observer)
    adapter = _ensure_adapter(shared, payload)
    snapshot = engine_core._refresh_custom_human_tradovate_snapshot_health(
        adapter,
        shared,
        observer_cfg=shared.get("tradovate_snapshot_cfg"),
    )
    detail = engine_core._custom_human_tradovate_snapshot_status(snapshot).get(
        "detail",
        "Tradovate snapshot opdateret.",
    )
    recovery = _maybe_self_heal_auto_runtime(shared)
    if isinstance(recovery, dict) and bool(recovery.get("ok", False)):
        recovery_message = str(recovery.get("message", "") or "").strip()
        if recovery_message:
            detail = f"{detail} | {recovery_message}"
    shared["last_result"] = str(detail)
    return _command_result(str(detail), snapshot=snapshot, auto_recovery=recovery)


def _handle_test_connection(shared: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    _refresh_shared_context(shared, payload, include_live_observer=False)
    adapter = _ensure_adapter(shared, payload)
    info = engine_core._run_cdp_adapter_task(adapter, adapter.test_connection)
    if info and info.get("ok"):
        message = (
            f"Forbindelse OK | URL: {info.get('url', 'N/A')} | Titel: {info.get('title', 'N/A')}"
        )
        shared["last_result"] = message
        shared["last_connection_test"] = dict(info)
        _clear_engine_fault(shared)
        return _command_result(message, info=info)
    message = str((info or {}).get("error", "ukendt fejl") or "ukendt fejl")
    shared["last_result"] = f"Test fejlede: {message}"
    shared["last_connection_test"] = dict(info or {})
    return _command_result(shared["last_result"], ok=False, status="connection_failed", info=info or {})


def _process_command(shared: dict[str, Any], command: dict[str, Any]) -> dict[str, Any]:
    action = str(command.get("command", "") or "").strip().upper()
    payload = dict(command.get("payload") or {})
    shared["last_command"] = action
    shared["last_command_id"] = int(command.get("id", 0) or 0)
    if action == "CONNECT":
        _refresh_shared_context(shared, payload, include_live_observer=False)
        requested_port = resolve_cdp_port(payload.get("debug_port", CDP_PORT))
        requested_platform = str(payload.get("platform", "tradovate") or "tradovate").strip().lower()
        existing_adapter = shared.get("adapter")
        already_connected = (
            isinstance(existing_adapter, engine_core.CDPHumanAdapter)
            and bool(existing_adapter.is_connected)
            and int(getattr(existing_adapter, "cdp_port", -1)) == requested_port
            and str(getattr(existing_adapter, "platform", "") or "").strip().lower() == requested_platform
        )
        LOGGER.info(
            "Engine: Claimed command CONNECT, starting adapter on port %s...",
            requested_port,
        )
        if not already_connected:
            # Watchdog: auto-spawn Chrome if the port is dark (and wait for bootstrap)
            ensure_chrome_running(requested_port)
            # Pitbull: retry probe with exponential back-off
            probe = _pitbull_probe_with_backoff(requested_port)
            if not bool(probe.get("ok", False)):
                reason = (
                    f"Chrome/CDP svarer ikke på port {requested_port} efter {PITBULL_MAX_ATTEMPTS} forsøg. "
                    f"Probe fejlede mod {probe.get('endpoint', 'ukendt endpoint')}: {probe.get('error', 'ukendt fejl')}"
                )
                _set_engine_fault(shared, "CHROME_ERROR", reason, halt=True)
                return _command_result(reason, ok=False, status="chrome_error")
        try:
            adapter = existing_adapter if already_connected else _ensure_adapter(
                shared,
                payload,
                ensure_watchdog=False,
            )
        except Exception as exc:  # noqa: BLE001
            if _is_cdp_failure(exc):
                reason = str(exc).strip() or f"Chrome/CDP svarer ikke på port {requested_port}."
                _set_engine_fault(shared, "CHROME_ERROR", reason, halt=True)
                return _command_result(reason, ok=False, status="chrome_error")
            raise
        message = (
            f"Allerede forbundet til Chrome på port {requested_port}."
            if already_connected
            else f"Forbundet til Chrome på port {requested_port}."
        )
        shared["adapter"] = adapter
        shared["connected"] = bool(adapter.is_connected)
        _run_startup_cleanup_if_safe(
            shared,
            adapter,
            reset_context="connect command / flat snapshot",
        )
        shared["last_result"] = message
        shared["debug_port"] = requested_port
        _clear_engine_fault(shared)
        rearm = _attempt_auto_rearm_after_connect(shared, payload, adapter)
        if bool(rearm.get("ok", False)):
            rearm_message = str(rearm.get("message", "") or "").strip()
            if rearm_message and rearm_message != "Auto kører allerede.":
                message = f"{message} | {rearm_message}"
                shared["last_result"] = message
        elif bool(shared.get("auto_requested", False)):
            rearm_message = str(rearm.get("message", "") or "").strip()
            if rearm_message:
                shared["last_result"] = f"{message} | Auto afventer: {rearm_message}"
        return _command_result(
            shared["last_result"],
            status="already_connected" if already_connected else "connected",
            auto_rearm=rearm,
        )
    if action == "START":
        return _handle_start(shared, payload)
    if action == "STOP":
        engine_core._stop_custom_human_auto_runtime(
            shared,
            user_initiated=True,
            reason="Stoppet af bruger.",
        )
        return _command_result("Auto-trading stoppet.", status="stopped")
    if action == "REFRESH":
        return _handle_refresh(shared, payload)
    if action == "TEST_CONNECTION":
        return _handle_test_connection(shared, payload)
    if action == "FLAT":
        signal_payload = {"signal": "FLAT", "action": "exit", "event": "manual_ui_flat"}
        queue_status, queue_message = engine_core._queue_cdp_signal_from_custom_human(signal_payload)
        shared["last_result"] = str(queue_message)
        return _command_result(queue_message, ok=queue_status == "queued_to_cdp", status=queue_status)
    if action == "MANUAL_SIGNAL":
        try:
            signal_payload = _build_manual_signal_payload(shared, payload)
        except Exception as exc:
            message = f"Manual signal payload ugyldig: {exc}"
            shared["last_result"] = message
            return _command_result(message, ok=False, status="invalid_manual_signal")
        queue_status, queue_message = engine_core._queue_cdp_signal_from_custom_human(signal_payload)
        shared["last_result"] = str(queue_message)
        return _command_result(queue_message, ok=queue_status == "queued_to_cdp", status=queue_status)
    if action == "SET_POSITION_METADATA":
        return _handle_set_position_metadata(shared, payload)
    if action == "CLEAR_RISKGATE_CACHE":
        return _handle_clear_riskgate_cache(shared)
    if action == "MANUAL_CLICK":
        return _handle_manual_click(shared, payload)
    if action == "DISCONNECT":
        _disconnect_adapter(shared)
        _clear_engine_fault(shared)
        return _command_result("CDP adapter frakoblet.", status="disconnected")
    return _command_result(f"Ukendt kommando: {action}", ok=False, status="unknown_command")


def _status_payload(shared: dict[str, Any]) -> dict[str, Any]:
    adapter = shared.get("adapter")
    if isinstance(adapter, engine_core.CDPHumanAdapter):
        shared["connected"] = bool(adapter.is_connected)
        shared["debug_port"] = int(getattr(adapter, "cdp_port", resolve_cdp_port(shared.get("debug_port", CDP_PORT))))
    else:
        shared["connected"] = False
    payload = engine_core._capture_custom_human_runtime_state(shared)
    payload.update(
        {
            "engine_status": _engine_status(shared),
            "running": bool(shared.get("running", False)),
            "auto_requested": bool(shared.get("auto_requested", False)),
            "last_stop_reason": str(shared.get("last_stop_reason", "") or ""),
            "auto_restart_attempts": int(shared.get("auto_restart_attempts", 0) or 0),
            "supervisor_running": bool(shared.get("supervisor_running", False)),
            "supervisor_last_reason": str(shared.get("supervisor_last_reason", "") or ""),
            "connected": bool(shared.get("connected", False)),
            "cdp_port": resolve_cdp_port(shared.get("debug_port", CDP_PORT)),
            "runtime_active": bool(shared.get("runtime_active", False)),
            "selector_mode": str(shared.get("selector_mode", "") or ""),
            "last_command": str(shared.get("last_command", "") or ""),
            "last_command_id": int(shared.get("last_command_id", 0) or 0),
            "last_error": str(shared.get("last_error", "") or ""),
            "halted": bool(shared.get("halted", False)),
            "halt_reason": str(shared.get("halt_reason", "") or ""),
            "halt_code": str(shared.get("halt_code", "") or ""),
            "buy_info": dict(shared.get("buy_info")) if isinstance(shared.get("buy_info"), dict) else None,
            "sell_info": dict(shared.get("sell_info")) if isinstance(shared.get("sell_info"), dict) else None,
            "flat_info": dict(shared.get("flat_info")) if isinstance(shared.get("flat_info"), dict) else None,
            "live_observer_running": bool(shared.get("live_observer_running", False)),
            "tradovate_snapshot_running": bool(shared.get("tradovate_snapshot_running", False)),
            "expected_account_tokens": list(shared.get("expected_account_tokens") or ()),
            "last_dom_preflight": dict(shared.get("last_dom_preflight"))
            if isinstance(shared.get("last_dom_preflight"), dict)
            else None,
            "ui_contract": dict(shared.get("ui_contract")) if isinstance(shared.get("ui_contract"), dict) else None,
            "last_connection_test": dict(shared.get("last_connection_test"))
            if isinstance(shared.get("last_connection_test"), dict)
            else None,
            "diagnostics": engine_core._custom_human_diagnostics_snapshot(shared),
            "engine_meta": {
                "pid": int(os.getpid()),
                "published_at": time_module.time(),
                "heartbeat_interval_seconds": ENGINE_HEARTBEAT_SECONDS,
            },
            "updated_at": time_module.time(),
        }
    )
    observer_cfg = shared.get("live_observer_cfg")
    if isinstance(observer_cfg, dict):
        payload["live_observer_cfg"] = {
            "enabled": bool(observer_cfg.get("enabled", False)),
            "message": str(observer_cfg.get("message", "") or ""),
        }
    else:
        payload["live_observer_cfg"] = None
    if not bool(shared.get("connected", False)):
        snapshot_payload = payload.get("tradovate_snapshot")
        if isinstance(snapshot_payload, dict):
            snapshot_payload = dict(snapshot_payload)
            snapshot_payload["connected"] = False
            payload["tradovate_snapshot"] = snapshot_payload
    return payload


def _try_auto_connect_on_startup(shared: dict[str, Any]) -> None:
    """One-click startup: if the Watchdog already has Chrome on the CDP port,
    connect automatically so the UI sees a live 'connected' status without the
    operator having to click the Connect button first.

    Uses default settings (Tradovate platform, persisted port).  The runtime
    workers are NOT started here – the user still needs to arm a strategy.
    If auto-connect fails for any reason we log and continue silently; the
    engine is fully functional via the normal UI command path.
    """
    port = resolve_cdp_port(shared.get("debug_port", CDP_PORT))
    if not _is_port_open("127.0.0.1", port):
        LOGGER.info(
            "[GHOST-V6.6] One-click startup: Chrome not yet on port %s – skipping auto-connect.", port
        )
        return
    if bool(shared.get("connected", False)) and isinstance(shared.get("adapter"), engine_core.CDPHumanAdapter):
        LOGGER.info("[GHOST-V6.6] One-click startup: adapter already connected – nothing to do.")
        return
    LOGGER.info("[GHOST-V6.6] One-click startup: Chrome detected on port %s – auto-connecting.", port)
    try:
        _minimal_payload: dict[str, Any] = {"debug_port": port, "platform": "tradovate"}
        _ensure_adapter(shared, _minimal_payload, ensure_watchdog=False)
        shared["connected"] = bool(
            isinstance(shared.get("adapter"), engine_core.CDPHumanAdapter)
            and shared["adapter"].is_connected
        )
        if shared["connected"]:
            LOGGER.info("[GHOST-V6.6] One-click startup: Auto-connected to Chrome on port %s.", port)
            _run_startup_cleanup_if_safe(
                shared,
                shared["adapter"],
                reset_context="startup auto-connect / flat snapshot",
            )
            # If the persisted state had auto_requested=True, attempt to re-arm
            # the trading workers immediately (no UI click required).
            if bool(shared.get("auto_requested", False)):
                try:
                    # Enrich the minimal payload with persisted runtime_profile /
                    # runtime_config / runtime_active so _command_state can build
                    # a valid state even without a Streamlit session.
                    _persisted_rt = engine_core._load_custom_human_runtime_state() or {}
                    _rt_profile = _persisted_rt.get("runtime_profile") or {}
                    _rt_config = _persisted_rt.get("config") or {}
                    _rt_active = bool(_persisted_rt.get("active", False))
                    _rearm_payload: dict[str, Any] = {
                        **_minimal_payload,
                        "runtime_profile": _rt_profile,
                        "runtime_config": _rt_config,
                        "runtime_active": _rt_active,
                        "selector_mode": "Auto (platform selectors)",
                    }
                    rearm = _attempt_auto_rearm_after_connect(shared, _rearm_payload, shared["adapter"])
                    if bool(rearm.get("ok", False)):
                        LOGGER.info(
                            "[GHOST-V6.6] One-click startup: Auto-rearm succeeded – %s",
                            rearm.get("message", ""),
                        )
                    else:
                        LOGGER.info(
                            "[GHOST-V6.6] One-click startup: Auto-rearm skipped – %s",
                            rearm.get("message", ""),
                        )
                except Exception as exc:
                    LOGGER.debug("[GHOST-V6.6] One-click startup: Auto-rearm raised: %s", exc)
        else:
            LOGGER.warning("[GHOST-V6.6] One-click startup: Adapter connected but is_connected=False.")
    except Exception as exc:
        LOGGER.info(
            "[GHOST-V6.6] One-click startup: Auto-connect attempt failed (%s) – will wait for UI command.",
            exc,
        )


def _run_startup_cleanup_if_safe(
    shared: dict[str, Any],
    adapter: engine_core.CDPHumanAdapter,
    *,
    reset_context: str,
) -> dict[str, Any] | None:
    try:
        snapshot = engine_core._refresh_custom_human_tradovate_snapshot_health(
            adapter,
            shared,
            observer_cfg=shared.get("tradovate_snapshot_cfg"),
        )
    except Exception as exc:
        LOGGER.debug("[STARTUP] Startup-cleanup snapshot refresh sprang over: %s", exc)
        return None
    result = engine_core._cleanup_stale_runtime_on_startup(
        shared,
        snapshot_raw=snapshot if isinstance(snapshot, dict) else None,
        reset_context=reset_context,
    )
    if bool(result.get("ok", False)):
        LOGGER.info("[STARTUP] %s", str(result.get("message", "") or "").strip())
        engine_core._persist_custom_human_runtime_state(shared)
    else:
        LOGGER.info(
            "[STARTUP] Cleanup sprang over (%s): %s",
            str(result.get("status", "skipped") or "skipped"),
            str(result.get("message", "") or "").strip(),
        )
    return result


def _backup_databases_if_needed(bridge_path: Path) -> None:
    """Create a dated daily backup of all critical SQLite databases.

    Runs at most once per calendar day — if a backup from today already exists
    it is silently skipped.  Protects against accidental file deletion or
    corruption of the audit trail and bridge DB between sessions.
    """
    today_tag = date.today().isoformat()  # e.g. "2026-03-17"
    # Databases to back up: bridge DB + audit DB (sibling in the output/ dir)
    candidates = [
        bridge_path,
        bridge_path.parent / "trading_audit.sqlite3",
    ]
    for src in candidates:
        if not src.exists():
            continue
        backup_path = src.with_suffix(f".{today_tag}.bak")
        if backup_path.exists():
            continue  # Already backed up today
        try:
            shutil.copy2(src, backup_path)
            LOGGER.info("[BACKUP] %s → %s", src.name, backup_path.name)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("[BACKUP] Backup af %s fejlede: %s", src.name, exc)


def _scrub_stale_signal_log_entries(repo_root: Path) -> None:
    """Clean up stale signals and position locks left from crashed/disconnected sessions.

    Three problems are fixed here:

    1. **Duplicate signal_id block**: Signal-IDs are deterministic SHA-256
       hashes.  If a ``queued_local_cdp`` signal was never executed (e.g.
       CDP disconnect), its ID stays in ``signal_log`` and blocks every
       future identical signal with "duplicate signal_id".

    2. **Position lock freeze**: The router acquires a ``position_lock`` when
       a signal is dispatched and releases it only on confirmed fill/exit.
       If the engine crashes mid-trade the lock stays at ``is_open=1``
       forever, blocking all new entries with "blocked_position_lock".

    3. **Duplicate trade intent after flat position**: ``seen_idempotency_keys``
       in the persisted runtime state blocks same-session re-entry even after a
       position has been confirmed closed.  When the persisted live state shows
       ``position_open=False`` at startup, we clear the key set so a new entry
       can fire without a manual "Nulstil RiskGate" click.

    All three are safe to clear at startup because the broker snapshot at
    ``_prepare_custom_human_auto_targets`` will re-verify the real position
    before any trade is attempted.
    """
    signal_db = repo_root / "output" / "signal_router.sqlite3"
    if not signal_db.exists():
        return
    try:
        import sqlite3 as _sqlite3
        import datetime as _dt
        conn = _sqlite3.connect(signal_db)
        now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat()

        # 1. Delete unconfirmed queued signals so dedup never blocks fresh signals
        sig_deleted = conn.execute(
            "DELETE FROM signal_log WHERE status = 'queued_local_cdp'"
        ).rowcount

        # 2. Release all open position locks — the broker snapshot will re-check
        #    the real position before any click, so this is safe.
        lock_released = conn.execute(
            "UPDATE position_lock SET is_open = 0, updated_at = ? WHERE is_open = 1",
            (now_iso,),
        ).rowcount

        conn.commit()
        conn.close()
        if sig_deleted:
            LOGGER.info(
                "[STARTUP] Ryddet %d stale 'queued_local_cdp' signal(er) fra signal_log.",
                sig_deleted,
            )
        if lock_released:
            LOGGER.info(
                "[STARTUP] Frigivet %d fastfrosne position_lock(s) — broker snapshot verificerer åben position.",
                lock_released,
            )
    except Exception as _exc:
        LOGGER.warning("[STARTUP] Kunne ikke rydde signal_log/position_lock: %s", _exc)

    # 3. Clear seen_idempotency_keys from persisted runtime state when position is flat.
    #    Without this, a same-day engine restart keeps the idempotency key from the
    #    previous trade in memory and blocks every new entry signal with
    #    "Duplicate trade intent" even though the position has already closed.
    #    seen_idempotency_keys lives inside risk_gate_state (not at the top level).
    try:
        _state = engine_core._load_custom_human_runtime_state()
        if isinstance(_state, dict):
            _live = _state.get("live_state") or {}
            _position_open = bool(_live.get("position_open", False))
            _rg_state = _state.get("risk_gate_state") or {}
            _keys_before = list(_rg_state.get("seen_idempotency_keys") or [])
            if not _position_open and _keys_before:
                _rg_state["seen_idempotency_keys"] = []
                _state["risk_gate_state"] = _rg_state
                engine_core._save_custom_human_runtime_state(_state)
                LOGGER.info(
                    "[STARTUP] Position er flat – %d idempotency-nøgle(r) renset fra RiskGate-hukommelsen.",
                    len(_keys_before),
                )
    except Exception as _exc:
        LOGGER.warning("[STARTUP] Kunne ikke rydde seen_idempotency_keys: %s", _exc)


def run_engine(*, bridge_db_path: str | Path | None = None) -> None:
    bridge_path = init_bridge(_bridge_path(bridge_db_path))
    # Housekeeper: discard all pending commands from any prior (possibly crashed) session
    scrub_stale_commands(bridge_path)
    # Housekeeper: delete unconfirmed queued_local_cdp signals so the deterministic
    # SHA-256 signal_id hash never permanently blocks a fresh entry signal.
    _scrub_stale_signal_log_entries(Path(__file__).resolve().parent)
    # Safety net: create a daily backup of the databases before the engine touches them
    _backup_databases_if_needed(bridge_path)
    shared = engine_core._cdp_auto_trade_shared
    shared["engine_started_at"] = datetime.now(engine_core.APP_TIMEZONE).isoformat()
    shared.setdefault("engine_status_override", "")
    shared.setdefault("halted", False)
    shared.setdefault("halt_reason", "")
    shared.setdefault("halt_code", "")

    # Restore persisted auto_requested flag so the one-click auto-rearm
    # works after an engine restart without any UI interaction.
    # Without this, shared["auto_requested"] is always False on startup
    # and the auto-rearm is permanently skipped.
    try:
        _persisted = engine_core._load_custom_human_runtime_state()
        if isinstance(_persisted, dict) and bool(_persisted.get("auto_requested", False)):
            shared.setdefault("auto_requested", True)
            LOGGER.info(
                "[GHOST-V6.6] Startup: Restored auto_requested=True from persisted state."
            )
    except Exception as _restore_exc:
        LOGGER.debug("[GHOST-V6.6] Startup: Could not restore auto_requested: %s", _restore_exc)

    # Publish a bootstrap heartbeat before any auto-connect/auto-rearm work.
    # Startup recovery can legitimately spend multiple seconds in CDP/browser
    # calls, and the supervisor must not misread that as a dead child process.
    _publish_engine_status(bridge_path, shared)

    # One-click startup: hook into Chrome automatically if the Watchdog already
    # has it running, so the operator does not need to click 'Connect' in the UI.
    _try_auto_connect_on_startup(shared)
    _publish_engine_status(bridge_path, shared)
    LOGGER.info("Trading engine started. bridge=%s pid=%s", bridge_path, os.getpid())
    next_heartbeat_at = time_module.monotonic() + ENGINE_HEARTBEAT_SECONDS
    try:
        while True:
            for command in claim_commands(bridge_path):
                # Renew the supervisor heartbeat before potentially long-running
                # command handling so a valid browser action is never mistaken
                # for a dead engine.
                claimed_action = str(command.get("command", "") or "").strip().upper()
                if claimed_action:
                    shared["last_command"] = claimed_action
                claimed_id = int(command.get("id", 0) or 0)
                if claimed_id:
                    shared["last_command_id"] = claimed_id
                _publish_engine_status(bridge_path, shared)
                try:
                    result = _process_command(shared, command)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception("Command failed: %s", command.get("command"))
                    shared["last_result"] = f"Kommando {command.get('command', 'UNKNOWN')} fejlede: {exc}"
                    shared["last_command"] = str(command.get("command", "UNKNOWN") or "UNKNOWN").strip().upper()
                    if _is_cdp_failure(exc):
                        _set_engine_fault(shared, "CHROME_ERROR", str(exc), halt=True)
                        result = _command_result(shared["last_result"], ok=False, status="chrome_error")
                    else:
                        shared["last_error"] = str(exc)
                        result = _command_result(shared["last_result"], ok=False, status="command_failed")
                shared["last_command_result"] = result
                _publish_engine_status(
                    bridge_path,
                    shared,
                    log_message="Engine: Status published to bridge.",
                )
                next_heartbeat_at = time_module.monotonic() + ENGINE_HEARTBEAT_SECONDS
            if time_module.monotonic() >= next_heartbeat_at:
                # Renew the heartbeat before any self-heal/browser work.
                _publish_engine_status(bridge_path, shared)
                recovery = _maybe_self_heal_auto_runtime(shared)
                if isinstance(recovery, dict) and bool(recovery.get("ok", False)):
                    LOGGER.info(
                        "Engine: Auto-heal %s%s",
                        str(recovery.get("status", "ok") or "ok"),
                        f" – {str(recovery.get('message', '') or '').strip()}" if str(recovery.get("message", "") or "").strip() else "",
                    )
                _log_pending_buffer_states(shared)
                _publish_engine_status(bridge_path, shared)
                next_heartbeat_at = time_module.monotonic() + ENGINE_HEARTBEAT_SECONDS
            # Stealth Jitter: randomise the poll interval slightly so consecutive
            # CDP reads never form a perfect machine-like sawtooth pattern.
            time_module.sleep(ENGINE_POLL_SECONDS + random.uniform(ENGINE_POLL_JITTER_MIN, ENGINE_POLL_JITTER_MAX))
    except KeyboardInterrupt:
        LOGGER.info("Trading engine stopping due to keyboard interrupt.")
    finally:
        if bool(shared.get("running", False)):
            try:
                engine_core._stop_custom_human_auto_runtime(
                    shared,
                    user_initiated=False,
                    reason="Trading engine shutting down.",
                )
            except Exception:
                pass
        _disconnect_adapter(shared)
        _publish_engine_status(bridge_path, shared)


def main() -> None:
    parser = argparse.ArgumentParser(description="Final Fantasy trading engine process")
    parser.add_argument(
        "--bridge-db",
        default=str(BRIDGE_DB_PATH),
        help="Path to the SQLite bridge used between Streamlit UI and the engine.",
    )
    args = parser.parse_args()
    run_engine(bridge_db_path=args.bridge_db)


if __name__ == "__main__":
    main()
