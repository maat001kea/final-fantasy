from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.trading.cdp_adapter import CDP_PORT, resolve_cdp_port
from src.trading_engine_bridge import fetch_status, init_bridge, publish_neutral_status


ENGINE_SERVICE_HEARTBEAT_TIMEOUT_SECONDS = 15.0
ENGINE_SERVICE_START_TIMEOUT_SECONDS = 6.0
CHROME_READY_TIMEOUT_SECONDS = 15.0
CHROME_REUSE_HEARTBEAT_SECONDS = 5.0

_GOOGLE_CHROME_BIN_ENV = "GOOGLE_CHROME_BIN"
_DEFAULT_CHROME_START_URL = "about:blank"
_ENGINE_SERVICE_DIRNAME = "runtime"
_ENGINE_SERVICE_STATUS_FILENAME = "trading_engine_service_status.json"
_ENGINE_SERVICE_PID_FILENAME = "trading_engine_service.pid"
_ENGINE_SERVICE_LOG_FILENAME = "trading_engine_service.log"
_CHROME_STATUS_TEMPLATE = "cdp_chrome_{port}_status.json"
_CHROME_PID_TEMPLATE = "cdp_chrome_{port}.pid"
_CHROME_LOG_TEMPLATE = "cdp_chrome_{port}.log"
_CHROME_PROFILE_TEMPLATE = "chrome-cdp-profile-{port}"


def repo_root(explicit: str | Path | None = None) -> Path:
    if explicit is not None:
        return Path(explicit).resolve()
    return Path(__file__).resolve().parents[2]


def runtime_dir(explicit_repo_root: str | Path | None = None) -> Path:
    path = repo_root(explicit_repo_root) / "output" / _ENGINE_SERVICE_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def engine_service_paths(explicit_repo_root: str | Path | None = None) -> dict[str, Path]:
    base = runtime_dir(explicit_repo_root)
    return {
        "runtime_dir": base,
        "status_file": base / _ENGINE_SERVICE_STATUS_FILENAME,
        "pid_file": base / _ENGINE_SERVICE_PID_FILENAME,
        "log_file": base / _ENGINE_SERVICE_LOG_FILENAME,
    }


def chrome_runtime_paths(
    *,
    port: int = CDP_PORT,
    explicit_repo_root: str | Path | None = None,
) -> dict[str, Path]:
    base = runtime_dir(explicit_repo_root)
    port_value = resolve_cdp_port(port)
    return {
        "runtime_dir": base,
        "status_file": base / _CHROME_STATUS_TEMPLATE.format(port=port_value),
        "pid_file": base / _CHROME_PID_TEMPLATE.format(port=port_value),
        "log_file": base / _CHROME_LOG_TEMPLATE.format(port=port_value),
        "profile_dir": base / _CHROME_PROFILE_TEMPLATE.format(port=port_value),
    }


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def process_is_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    try:
        result = subprocess.run(  # noqa: S603
            ["ps", "-o", "stat=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
        )
        stat = str(result.stdout or "").strip().upper()
        if stat.startswith("Z"):
            return False
    except OSError:
        pass
    return True


def _write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    os.replace(tmp_path, path)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _write_text(path, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))


def read_json(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_pid_file(path: Path, pid: int) -> None:
    _write_text(path, f"{int(pid)}\n")


def _remove_file(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def bridge_status_age_seconds(status: dict[str, Any]) -> float | None:
    bridge_updated_at = str(status.get("_bridge_updated_at", "") or "").strip()
    if bridge_updated_at:
        try:
            observed_at = datetime.fromisoformat(bridge_updated_at)
            if observed_at.tzinfo is None:
                observed_at = observed_at.replace(tzinfo=timezone.utc)
            return max(
                0.0,
                (datetime.now(tz=timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds(),
            )
        except ValueError:
            pass
    for candidate in (status.get("updated_at"), (status.get("engine_meta") or {}).get("published_at")):
        try:
            return max(0.0, time.time() - float(candidate))
        except (TypeError, ValueError):
            continue
    return None


def probe_cdp_endpoint(port: int, *, timeout: float = 1.5) -> dict[str, Any]:
    port_value = resolve_cdp_port(port)
    last_error: Exception | None = None
    endpoint = f"http://127.0.0.1:{port_value}/json"
    for endpoint in (
        f"http://127.0.0.1:{port_value}/json/version",
        f"http://127.0.0.1:{port_value}/json",
    ):
        try:
            with urllib.request.urlopen(endpoint, timeout=timeout) as response:
                body = response.read().decode("utf-8", errors="replace")
            return {"ok": True, "endpoint": endpoint, "sample": body[:200], "port": port_value}
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    return {
        "ok": False,
        "endpoint": endpoint,
        "error": str(last_error or "ukendt fejl"),
        "port": port_value,
    }


def find_google_chrome_binary() -> str | None:
    candidates = [
        os.getenv(_GOOGLE_CHROME_BIN_ENV, ""),
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    for candidate in candidates:
        token = str(candidate or "").strip()
        if token and Path(token).exists():
            return token
    return None


def build_cdp_chrome_command(
    *,
    chrome_binary: str,
    port: int = CDP_PORT,
    profile_dir: str | Path,
    start_url: str = _DEFAULT_CHROME_START_URL,
    headless: bool = False,
) -> list[str]:
    port_value = resolve_cdp_port(port)
    command = [
        chrome_binary,
        f"--remote-debugging-port={port_value}",
        f"--user-data-dir={Path(profile_dir).resolve()}",
        # ── Identity & first-run ─────────────────────────────────────────────
        "--no-first-run",
        "--no-default-browser-check",
        # ── Pop-ups, notifications & info-bars ───────────────────────────────
        # Prevents Chrome dialogs/permission prompts from stealing focus and
        # breaking CDP WebSocket message delivery during live trades.
        "--disable-popup-blocking",
        "--disable-notifications",
        "--disable-infobars",
        # ── Background throttling ─────────────────────────────────────────────
        # Chrome aggressively throttles timers and paints for background tabs/
        # windows. These flags ensure the CDP observer loop runs at full speed
        # even when the Chrome window is not in the foreground.
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        # ── Stability & noise reduction ───────────────────────────────────────
        "--disable-crash-reporter",
        "--disable-features=DialMediaRouteProvider",
    ]
    if headless:
        command.extend(["--headless=new", "--disable-gpu"])
    else:
        command.append("--new-window")
    command.append(str(start_url or _DEFAULT_CHROME_START_URL))
    return command


def _signal_process(pid: int | None, sig: int, *, timeout_seconds: float = 5.0) -> bool:
    if not process_is_alive(pid):
        return True
    assert pid is not None
    try:
        os.kill(pid, sig)
    except OSError:
        return not process_is_alive(pid)
    deadline = time.time() + max(0.2, timeout_seconds)
    while time.time() < deadline:
        if not process_is_alive(pid):
            return True
        time.sleep(0.1)
    return not process_is_alive(pid)


def read_engine_service_status(
    explicit_repo_root: str | Path | None = None,
    *,
    bridge_db_path: str | Path | None = None,
) -> dict[str, Any]:
    root = repo_root(explicit_repo_root)
    paths = engine_service_paths(root)
    payload = read_json(paths["status_file"])
    supervisor_pid = _safe_int(payload.get("supervisor_pid"))
    engine_pid = _safe_int(payload.get("engine_pid"))
    supervisor_alive = process_is_alive(supervisor_pid)
    engine_alive = process_is_alive(engine_pid)
    bridge_path = Path(bridge_db_path) if bridge_db_path is not None else root / "output" / "trading_engine_bridge.sqlite3"
    bridge_payload = fetch_status(bridge_path) if bridge_path.exists() else {}
    heartbeat_age = bridge_status_age_seconds(bridge_payload) if isinstance(bridge_payload, dict) else None
    bridge_engine_pid = _safe_int((bridge_payload.get("engine_meta") or {}).get("pid")) if isinstance(bridge_payload, dict) else None
    bridge_engine_alive = process_is_alive(bridge_engine_pid) if bridge_engine_pid is not None else True
    non_bridge_payload = (
        {
            key: value
            for key, value in bridge_payload.items()
            if key != "_bridge_updated_at"
        }
        if isinstance(bridge_payload, dict)
        else {}
    )
    engine_online = (
        bool(non_bridge_payload)
        and heartbeat_age is not None
        and heartbeat_age <= CHROME_REUSE_HEARTBEAT_SECONDS
        and bridge_engine_alive
        and not bool(bridge_payload.get("halted", False))
        and bool(bridge_payload.get("active", False) or bridge_payload.get("running", False))
    )
    state = str(payload.get("state", "") or "").strip().lower()
    if supervisor_alive:
        running_state = state or "running"
    elif engine_online:
        running_state = "external_engine"
    elif state:
        running_state = state
    else:
        running_state = "stopped"
    normalized = {
        "state": running_state,
        "managed": bool(supervisor_alive),
        "running": bool(supervisor_alive or engine_online),
        "supervisor_pid": supervisor_pid,
        "engine_pid": engine_pid,
        "supervisor_alive": supervisor_alive,
        "engine_alive": engine_alive,
        "heartbeat_age_seconds": heartbeat_age,
        "engine_online": engine_online,
        "bridge_path": str(bridge_path),
        "status_file": str(paths["status_file"]),
        "pid_file": str(paths["pid_file"]),
        "log_file": str(paths["log_file"]),
        "updated_at": str(payload.get("updated_at", "") or ""),
        "restart_count": int(payload.get("restart_count", 0) or 0),
        "last_error": str(payload.get("last_error", "") or ""),
        "last_exit_code": payload.get("last_exit_code"),
        "engine_status": str(bridge_payload.get("engine_status", "") or ""),
        "connected": bool(bridge_payload.get("connected", False)),
        "halted": bool(bridge_payload.get("halted", False)),
    }
    return normalized


def start_engine_service(
    explicit_repo_root: str | Path | None = None,
    *,
    bridge_db_path: str | Path | None = None,
    env_overrides: dict[str, str] | None = None,
    start_timeout_seconds: float = ENGINE_SERVICE_START_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = repo_root(explicit_repo_root)
    bridge_path = init_bridge(Path(bridge_db_path) if bridge_db_path is not None else root / "output" / "trading_engine_bridge.sqlite3")
    status = read_engine_service_status(root, bridge_db_path=bridge_path)
    if status["managed"]:
        return {"ok": True, "status": "already_running", **status}
    if status["engine_online"]:
        return {"ok": True, "status": "external_engine_active", **status}
    paths = engine_service_paths(root)
    command = [
        sys.executable,
        str(root / "trading_engine_service.py"),
        "--bridge-db",
        str(bridge_path),
        "--status-file",
        str(paths["status_file"]),
        "--pid-file",
        str(paths["pid_file"]),
        "--log-file",
        str(paths["log_file"]),
    ]
    env = os.environ.copy()
    if env_overrides:
        env.update({str(key): str(value) for key, value in env_overrides.items()})
    log_handle = paths["log_file"].open("ab")
    try:
        subprocess.Popen(  # noqa: S603
            command,
            cwd=root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        log_handle.close()
    deadline = time.time() + max(1.0, start_timeout_seconds)
    latest_status = status
    while time.time() < deadline:
        latest_status = read_engine_service_status(root, bridge_db_path=bridge_path)
        if latest_status["managed"]:
            return {"ok": True, "status": "started", "command": command, **latest_status}
        time.sleep(0.2)
    return {
        "ok": False,
        "status": "start_timeout",
        "command": command,
        **latest_status,
    }


def stop_engine_service(
    explicit_repo_root: str | Path | None = None,
    *,
    bridge_db_path: str | Path | None = None,
    timeout_seconds: float = 6.0,
) -> dict[str, Any]:
    root = repo_root(explicit_repo_root)
    status = read_engine_service_status(root, bridge_db_path=bridge_db_path)
    supervisor_pid = _safe_int(status.get("supervisor_pid"))
    engine_pid = _safe_int(status.get("engine_pid"))
    bridge_path = Path(bridge_db_path) if bridge_db_path is not None else root / "output" / "trading_engine_bridge.sqlite3"
    bridge_payload = fetch_status(bridge_path) if bridge_path.exists() else {}
    bridge_engine_pid = _safe_int((bridge_payload.get("engine_meta") or {}).get("pid")) if isinstance(bridge_payload, dict) else None
    if not bool(status.get("managed", False)) and bridge_engine_pid not in {None, supervisor_pid, engine_pid}:
        engine_pid = bridge_engine_pid
    stopped_supervisor = _signal_process(supervisor_pid, signal.SIGTERM, timeout_seconds=timeout_seconds)
    stopped_engine = _signal_process(engine_pid, signal.SIGTERM, timeout_seconds=timeout_seconds / 2)
    if not stopped_supervisor and supervisor_pid is not None:
        stopped_supervisor = _signal_process(supervisor_pid, signal.SIGKILL, timeout_seconds=1.5)
    if not stopped_engine and engine_pid is not None:
        stopped_engine = _signal_process(engine_pid, signal.SIGKILL, timeout_seconds=1.5)
    paths = engine_service_paths(root)
    if stopped_supervisor and stopped_engine:
        _remove_file(paths["pid_file"])
        payload = read_json(paths["status_file"])
        payload.update(
            {
                "state": "stopped",
                "updated_at": utc_now_iso(),
                "engine_pid": None,
                "supervisor_pid": None,
            }
        )
        _write_json(paths["status_file"], payload)
        publish_neutral_status(bridge_path, last_stop_reason="Engine service stoppet.")
    latest_status = read_engine_service_status(root, bridge_db_path=bridge_db_path)
    return {
        "ok": bool(stopped_supervisor and stopped_engine),
        "status": "stopped" if stopped_supervisor and stopped_engine else "stop_failed",
        **latest_status,
    }


def read_cdp_chrome_status(
    explicit_repo_root: str | Path | None = None,
    *,
    port: int = CDP_PORT,
) -> dict[str, Any]:
    root = repo_root(explicit_repo_root)
    port_value = resolve_cdp_port(port)
    paths = chrome_runtime_paths(port=port_value, explicit_repo_root=root)
    payload = read_json(paths["status_file"])
    pid = _safe_int(payload.get("pid"))
    probe = probe_cdp_endpoint(port_value, timeout=0.7)
    managed = bool(payload.get("managed", False))
    ready = bool(probe.get("ok", False))
    running = ready or process_is_alive(pid)
    return {
        "state": "ready" if ready else ("running" if running else "stopped"),
        "managed": managed,
        "running": running,
        "ready": ready,
        "pid": pid,
        "port": port_value,
        "profile_dir": str(paths["profile_dir"]),
        "status_file": str(paths["status_file"]),
        "pid_file": str(paths["pid_file"]),
        "log_file": str(paths["log_file"]),
        "chrome_binary": str(payload.get("chrome_binary", "") or ""),
        "last_error": "" if ready else str(payload.get("last_error", "") or probe.get("error", "")),
        "updated_at": str(payload.get("updated_at", "") or ""),
        "probe_endpoint": str(probe.get("endpoint", "") or ""),
    }


def launch_cdp_chrome(
    explicit_repo_root: str | Path | None = None,
    *,
    port: int = CDP_PORT,
    chrome_binary: str | None = None,
    profile_dir: str | Path | None = None,
    start_url: str = _DEFAULT_CHROME_START_URL,
    headless: bool = False,
    ready_timeout_seconds: float = CHROME_READY_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    root = repo_root(explicit_repo_root)
    port_value = resolve_cdp_port(port)
    existing_probe = probe_cdp_endpoint(port_value)
    paths = chrome_runtime_paths(port=port_value, explicit_repo_root=root)
    if bool(existing_probe.get("ok", False)):
        payload = {
            "managed": False,
            "pid": None,
            "port": port_value,
            "profile_dir": str(paths["profile_dir"]),
            "chrome_binary": "",
            "updated_at": utc_now_iso(),
            "last_error": "",
        }
        _write_json(paths["status_file"], payload)
        _remove_file(paths["pid_file"])
        return {"ok": True, "status": "reused_existing", **read_cdp_chrome_status(root, port=port_value)}

    binary = str(chrome_binary or find_google_chrome_binary() or "").strip()
    if not binary:
        payload = {
            "managed": False,
            "pid": None,
            "port": port_value,
            "profile_dir": str(paths["profile_dir"]),
            "chrome_binary": "",
            "updated_at": utc_now_iso(),
            "last_error": "Google Chrome binary blev ikke fundet.",
        }
        _write_json(paths["status_file"], payload)
        return {"ok": False, "status": "chrome_binary_missing", **read_cdp_chrome_status(root, port=port_value)}

    target_profile_dir = Path(profile_dir) if profile_dir is not None else paths["profile_dir"]
    target_profile_dir.mkdir(parents=True, exist_ok=True)
    command = build_cdp_chrome_command(
        chrome_binary=binary,
        port=port_value,
        profile_dir=target_profile_dir,
        start_url=start_url,
        headless=headless,
    )
    log_handle = paths["log_file"].open("ab")
    try:
        proc = subprocess.Popen(  # noqa: S603
            command,
            cwd=root,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        log_handle.close()
    _write_pid_file(paths["pid_file"], proc.pid)
    deadline = time.time() + max(2.0, ready_timeout_seconds)
    while time.time() < deadline:
        probe = probe_cdp_endpoint(port_value)
        if bool(probe.get("ok", False)):
            payload = {
                "managed": True,
                "pid": int(proc.pid),
                "port": port_value,
                "profile_dir": str(target_profile_dir),
                "chrome_binary": binary,
                "updated_at": utc_now_iso(),
                "last_error": "",
            }
            _write_json(paths["status_file"], payload)
            return {"ok": True, "status": "started", **read_cdp_chrome_status(root, port=port_value)}
        if proc.poll() is not None:
            break
        time.sleep(0.25)
    _signal_process(proc.pid, signal.SIGTERM, timeout_seconds=1.0)
    payload = {
        "managed": True,
        "pid": int(proc.pid),
        "port": port_value,
        "profile_dir": str(target_profile_dir),
        "chrome_binary": binary,
        "updated_at": utc_now_iso(),
        "last_error": f"Chrome/CDP blev ikke klar på port {port_value} inden for timeout.",
    }
    _write_json(paths["status_file"], payload)
    return {"ok": False, "status": "start_timeout", **read_cdp_chrome_status(root, port=port_value)}


def stop_cdp_chrome(
    explicit_repo_root: str | Path | None = None,
    *,
    port: int = CDP_PORT,
    timeout_seconds: float = 6.0,
) -> dict[str, Any]:
    root = repo_root(explicit_repo_root)
    port_value = resolve_cdp_port(port)
    status = read_cdp_chrome_status(root, port=port_value)
    if not bool(status.get("managed", False)):
        return {"ok": False, "status": "not_managed", **status}
    pid = _safe_int(status.get("pid"))
    stopped = _signal_process(pid, signal.SIGTERM, timeout_seconds=timeout_seconds)
    if not stopped and pid is not None:
        stopped = _signal_process(pid, signal.SIGKILL, timeout_seconds=1.5)
    paths = chrome_runtime_paths(port=port_value, explicit_repo_root=root)
    if stopped:
        _remove_file(paths["pid_file"])
        payload = read_json(paths["status_file"])
        payload.update(
            {
                "updated_at": utc_now_iso(),
                "last_error": "",
                "pid": None,
            }
        )
        _write_json(paths["status_file"], payload)
    return {"ok": bool(stopped), "status": "stopped" if stopped else "stop_failed", **read_cdp_chrome_status(root, port=port_value)}
