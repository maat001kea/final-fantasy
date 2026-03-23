"""Diagnostics event helpers for the Custom Human trading engine.

This module owns the five functions that write to and read from the
``diagnostics_events`` list inside the shared-state dict:

  _coerce_custom_human_diagnostics_events  – sanitise raw event list
  _append_custom_human_diagnostic_event    – append / deduplicate an event
  _log_custom_human_runtime_event          – log + append in one call
  _record_custom_human_watchdog_diagnostic – write a watchdog event only when
                                             the watchdog state has changed
  _record_custom_human_snapshot_diagnostic – same, for snapshot state

All five were previously defined in engine_core.py.  They were extracted here
as Phase 2 of the engine_core split.

Dependency note
───────────────
``_record_custom_human_watchdog_diagnostic`` calls ``_custom_human_watchdog_snapshot``
which now lives in ``snapshot_manager``.  A *lazy* import is still used inside
the function body because ``snapshot_manager`` imports ``APP_TIMEZONE`` and
``_coerce_custom_human_diagnostics_events`` from this module, creating a
mutual-import cycle at module-load time.  The lazy import resolves the cycle:
both modules are fully initialised before any function call executes.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

APP_TIMEZONE = ZoneInfo("Europe/Copenhagen")
CUSTOM_HUMAN_DIAGNOSTIC_EVENT_LIMIT = 50

_LOG = logging.getLogger("final_fantasy.engine_core")


# ---------------------------------------------------------------------------
# Coerce raw diagnostics list
# ---------------------------------------------------------------------------

def _coerce_custom_human_diagnostics_events(raw: Any) -> list[dict[str, str]]:
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


# ---------------------------------------------------------------------------
# Append / deduplicate a diagnostics event
# ---------------------------------------------------------------------------

def _append_custom_human_diagnostic_event(
    shared: dict[str, Any],
    *,
    kind: str,
    headline: str,
    detail: str = "",
    tone: str = "info",
    at: datetime | None = None,
) -> dict[str, str]:
    event = {
        "ts": (at or datetime.now(tz=APP_TIMEZONE)).isoformat(),
        "kind": str(kind or "").strip().lower(),
        "tone": str(tone or "info").strip().lower(),
        "headline": str(headline or "").strip(),
        "detail": str(detail or "").strip(),
    }
    events = _coerce_custom_human_diagnostics_events(shared.get("diagnostics_events"))
    comparable = ("kind", "tone", "headline", "detail")
    if events and all(str(events[-1].get(key, "")) == event[key] for key in comparable):
        events[-1]["ts"] = event["ts"]
    else:
        events.append(event)
    shared["diagnostics_events"] = events[-CUSTOM_HUMAN_DIAGNOSTIC_EVENT_LIMIT:]
    return event


# ---------------------------------------------------------------------------
# Log + append in one call
# ---------------------------------------------------------------------------

def _log_custom_human_runtime_event(
    shared: dict[str, Any] | None,
    *,
    headline: str,
    detail: str = "",
    tone: str = "info",
    kind: str = "system",
    level: int = logging.INFO,
) -> None:
    message = str(headline or "").strip() or "CUSTOM HUMAN"
    detail_text = str(detail or "").strip()
    if detail_text:
        _LOG.log(level, "[CUSTOM HUMAN] %s | %s", message, detail_text)
    else:
        _LOG.log(level, "[CUSTOM HUMAN] %s", message)
    if isinstance(shared, dict):
        _append_custom_human_diagnostic_event(
            shared,
            kind=kind,
            headline=message,
            detail=detail_text,
            tone=tone,
        )


# ---------------------------------------------------------------------------
# Record watchdog diagnostic (deduplicates by signature)
# ---------------------------------------------------------------------------

def _record_custom_human_watchdog_diagnostic(shared: dict[str, Any]) -> None:
    # Lazy import: snapshot_manager imports diagnostics_manager at module level,
    # so a top-level import here would create a circular dependency.
    from src.trading.snapshot_manager import _custom_human_watchdog_snapshot  # noqa: PLC0415

    watchdog = _custom_human_watchdog_snapshot(shared)
    signature = f"{watchdog.get('headline', '')}|{watchdog.get('detail', '')}|{watchdog.get('tone', '')}"
    markers = shared.setdefault("diagnostic_markers", {})
    if not isinstance(markers, dict):
        markers = {}
        shared["diagnostic_markers"] = markers
    if signature == str(markers.get("watchdog", "")):
        return
    markers["watchdog"] = signature
    _append_custom_human_diagnostic_event(
        shared,
        kind="watchdog",
        headline=str(watchdog.get("headline", "WATCHDOG")),
        detail=str(watchdog.get("detail", "")),
        tone=str(watchdog.get("tone", "info")),
    )


# ---------------------------------------------------------------------------
# Record snapshot diagnostic (deduplicates by signature)
# ---------------------------------------------------------------------------

def _record_custom_human_snapshot_diagnostic(
    shared: dict[str, Any],
    snapshot_status: dict[str, Any] | None,
) -> None:
    payload = snapshot_status if isinstance(snapshot_status, dict) else {}
    signature = f"{payload.get('headline', '')}|{payload.get('detail', '')}|{payload.get('tone', '')}"
    markers = shared.setdefault("diagnostic_markers", {})
    if not isinstance(markers, dict):
        markers = {}
        shared["diagnostic_markers"] = markers
    if signature == str(markers.get("snapshot", "")):
        return
    markers["snapshot"] = signature
    _append_custom_human_diagnostic_event(
        shared,
        kind="snapshot",
        headline=str(payload.get("headline", "TRADOVATE SNAPSHOT")),
        detail=str(payload.get("detail", "")),
        tone=str(payload.get("tone", "info")),
    )
