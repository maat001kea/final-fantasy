from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from src.trading.runtime_control import (
    ENGINE_SERVICE_HEARTBEAT_TIMEOUT_SECONDS,
    bridge_status_age_seconds,
    fetch_status,
    process_is_alive,
    read_json,
    utc_now_iso,
    _remove_file,
    _signal_process,
    _write_json,
    _write_pid_file,
)


LOGGER = logging.getLogger("final_fantasy.trading_engine_service")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)

SUPERVISOR_POLL_SECONDS = 1.0
ENGINE_RESTART_DELAY_SECONDS = 1.5
ENGINE_RESTART_MAX_DELAY_SECONDS = 10.0
ENGINE_STARTUP_GRACE_SECONDS = 10.0


def _write_status(
    status_file: Path,
    *,
    bridge_path: Path,
    supervisor_pid: int,
    engine_pid: int | None,
    state: str,
    restart_count: int,
    last_exit_code: int | None = None,
    last_error: str = "",
    heartbeat_age_seconds: float | None = None,
) -> None:
    payload = read_json(status_file)
    payload.update(
        {
            "state": str(state),
            "bridge_path": str(bridge_path),
            "supervisor_pid": int(supervisor_pid),
            "engine_pid": int(engine_pid) if engine_pid is not None else None,
            "restart_count": int(restart_count),
            "last_exit_code": last_exit_code,
            "last_error": str(last_error or ""),
            "heartbeat_age_seconds": heartbeat_age_seconds,
            "updated_at": utc_now_iso(),
        }
    )
    _write_json(status_file, payload)


def _launch_engine_process(repo_root: Path, bridge_path: Path, log_file: Path) -> subprocess.Popen[bytes]:
    log_handle = log_file.open("ab")
    try:
        return subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                str(repo_root / "trading_engine.py"),
                "--bridge-db",
                str(bridge_path),
            ],
            cwd=repo_root,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
    finally:
        log_handle.close()


def run_supervisor(
    *,
    repo_root: Path,
    bridge_path: Path,
    status_file: Path,
    pid_file: Path,
    log_file: Path,
    heartbeat_timeout_seconds: float = ENGINE_SERVICE_HEARTBEAT_TIMEOUT_SECONDS,
) -> int:
    stop_requested = False

    def _handle_stop(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    supervisor_pid = os.getpid()
    _write_pid_file(pid_file, supervisor_pid)
    restart_count = 0
    restart_delay_seconds = ENGINE_RESTART_DELAY_SECONDS
    child: subprocess.Popen[bytes] | None = None
    child_started_at_monotonic: float | None = None
    last_exit_code: int | None = None
    last_error = ""

    LOGGER.info("Trading engine supervisor started. bridge=%s pid=%s", bridge_path, supervisor_pid)
    try:
        while not stop_requested:
            if child is None or child.poll() is not None:
                if child is not None:
                    last_exit_code = child.poll()
                    last_error = f"Engine exited unexpectedly with code {last_exit_code}."
                    LOGGER.warning("%s Restarting in %.1fs.", last_error, restart_delay_seconds)
                    _write_status(
                        status_file,
                        bridge_path=bridge_path,
                        supervisor_pid=supervisor_pid,
                        engine_pid=None,
                        state="restarting",
                        restart_count=restart_count,
                        last_exit_code=last_exit_code,
                        last_error=last_error,
                    )
                    deadline = time.time() + restart_delay_seconds
                    while time.time() < deadline and not stop_requested:
                        time.sleep(0.1)
                    restart_count += 1
                    restart_delay_seconds = min(
                        ENGINE_RESTART_MAX_DELAY_SECONDS,
                        restart_delay_seconds * 2,
                    )
                child = _launch_engine_process(repo_root, bridge_path, log_file)
                last_error = ""
                _write_status(
                    status_file,
                    bridge_path=bridge_path,
                    supervisor_pid=supervisor_pid,
                    engine_pid=child.pid,
                    state="starting",
                    restart_count=restart_count,
                    last_exit_code=last_exit_code,
                    last_error=last_error,
                    heartbeat_age_seconds=None,
                )
                child_started_at_monotonic = time.monotonic()
                LOGGER.info("Trading engine child started. child_pid=%s", child.pid)

            bridge_payload = fetch_status(bridge_path)
            heartbeat_age = bridge_status_age_seconds(bridge_payload) if isinstance(bridge_payload, dict) else None
            startup_grace_elapsed = (
                child_started_at_monotonic is not None
                and (time.monotonic() - child_started_at_monotonic) >= max(
                    heartbeat_timeout_seconds,
                    ENGINE_STARTUP_GRACE_SECONDS,
                )
            )
            _write_status(
                status_file,
                bridge_path=bridge_path,
                supervisor_pid=supervisor_pid,
                engine_pid=child.pid if child is not None and child.poll() is None else None,
                state="running" if startup_grace_elapsed else "starting",
                restart_count=restart_count,
                last_exit_code=last_exit_code,
                last_error=last_error,
                heartbeat_age_seconds=heartbeat_age,
            )
            if (
                child is not None
                and child.poll() is None
                and bool(startup_grace_elapsed)
                and heartbeat_age is not None
                and heartbeat_age > max(heartbeat_timeout_seconds, 2.0)
            ):
                last_error = (
                    f"Engine heartbeat er stale ({heartbeat_age:.1f}s). "
                    "Supervisor genstarter child-processen."
                )
                LOGGER.warning("%s", last_error)
                _write_status(
                    status_file,
                    bridge_path=bridge_path,
                    supervisor_pid=supervisor_pid,
                    engine_pid=child.pid,
                    state="restarting",
                    restart_count=restart_count,
                    last_exit_code=last_exit_code,
                    last_error=last_error,
                    heartbeat_age_seconds=heartbeat_age,
                )
                if not _signal_process(child.pid, signal.SIGTERM, timeout_seconds=3.0):
                    _signal_process(child.pid, signal.SIGKILL, timeout_seconds=1.5)
                child = None
                child_started_at_monotonic = None
                continue
            time.sleep(SUPERVISOR_POLL_SECONDS)
    finally:
        child_pid = child.pid if child is not None and child.poll() is None else None
        if child_pid is not None:
            LOGGER.info("Stopping trading engine child pid=%s", child_pid)
            if not _signal_process(child_pid, signal.SIGTERM, timeout_seconds=4.0):
                _signal_process(child_pid, signal.SIGKILL, timeout_seconds=1.5)
        _write_status(
            status_file,
            bridge_path=bridge_path,
            supervisor_pid=supervisor_pid,
            engine_pid=None,
            state="stopped",
            restart_count=restart_count,
            last_exit_code=last_exit_code,
            last_error=last_error,
            heartbeat_age_seconds=None,
        )
        _remove_file(pid_file)
        LOGGER.info("Trading engine supervisor stopped.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Trading engine supervisor with auto-restart")
    parser.add_argument("--bridge-db", required=True, help="SQLite bridge path used by the engine.")
    parser.add_argument("--status-file", required=True, help="Supervisor status JSON file.")
    parser.add_argument("--pid-file", required=True, help="Supervisor pid file.")
    parser.add_argument("--log-file", required=True, help="Combined supervisor/engine log file.")
    parser.add_argument(
        "--heartbeat-timeout",
        type=float,
        default=ENGINE_SERVICE_HEARTBEAT_TIMEOUT_SECONDS,
        help="Restart the engine if bridge heartbeat is older than this many seconds.",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    raise SystemExit(
        run_supervisor(
            repo_root=root,
            bridge_path=Path(args.bridge_db).resolve(),
            status_file=Path(args.status_file).resolve(),
            pid_file=Path(args.pid_file).resolve(),
            log_file=Path(args.log_file).resolve(),
            heartbeat_timeout_seconds=float(args.heartbeat_timeout),
        )
    )


if __name__ == "__main__":
    main()
