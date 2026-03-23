"""Reusable reconnection logic for long-running CDP (Pitbull) adapters.

This module provides a single helper that wraps an adapter's `run_sync` call and
retries once in case of a transient CDP/Chrome connection drop. The retry logic
is intentionally conservative to avoid masking real errors.

"""

from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any, Awaitable, Callable, TypeVar

from .cdp_adapter import CDPConnectionError

_T = TypeVar("_T")


def _is_reconnectable_exception(exc: BaseException) -> bool:
    """Return True if the exception is likely a transient CDP disconnect."""
    message = str(exc or "").strip().lower()
    if not message:
        return False

    # These are the common messages thrown by the underlying websockets / CDP
    # stack on a dropped connection.
    reconnect_triggers = (
        "reconnect to chrome",
        "connection was closed",
        "cdp connection was closed",
        "websocket is not connected",
        "websocket connection is closed",
    )

    if isinstance(exc, CDPConnectionError):
        return any(trigger in message for trigger in reconnect_triggers)

    return any(trigger in message for trigger in reconnect_triggers)


def run_with_reconnect(
    adapter: Any,
    task_factory: Callable[[], Awaitable[_T]],
    *,
    max_attempts: int = 2,
    sleep_between: float = 0.75,
    timeout: float = 10.0,
) -> _T:
    """Run an async adapter task safely with an optional reconnect retry.

    If the adapter exposes `run_sync` and `reconnect_sync`, the task is executed
    on the adapter's persistent event loop. If a transient disconnect is detected,
    the adapter is reconnected once before retrying.

    If the adapter does not implement `run_sync`, the task is run via
    ``asyncio.run``.
    """

    if not hasattr(adapter, "run_sync"):
        return asyncio.run(task_factory())

    # Determine whether we can pass a timeout argument to run_sync.
    use_timeout = False
    try:
        sig = inspect.signature(adapter.run_sync)
        use_timeout = "timeout" in sig.parameters
    except Exception:
        use_timeout = False

    attempts = 0
    while True:
        try:
            if use_timeout:
                return adapter.run_sync(task_factory, timeout=timeout)
            return adapter.run_sync(task_factory)
        except BaseException as exc:
            reconnectable = _is_reconnectable_exception(exc)
            if not reconnectable and hasattr(adapter, "is_connected"):
                reconnectable = not bool(getattr(adapter, "is_connected"))

            attempts += 1
            if attempts >= max_attempts or not reconnectable or not hasattr(adapter, "reconnect_sync"):
                raise

            time.sleep(float(sleep_between))
            adapter.reconnect_sync(timeout=timeout)
