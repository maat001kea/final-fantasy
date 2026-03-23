"""Robust TradersPost signal router with dedupe, rate limits, and position locks."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
ENTRY_ACTIONS = {"buy", "sell"}


@dataclass(frozen=True)
class SignalDispatchResult:
    signal_id: str
    action: str
    status: str
    response_code: int | None
    retries: int
    message: str
    position_key: str | None = None


class TradingSignalRouter:
    """Persisted signal router to protect against duplicates and race conditions."""

    def __init__(
        self,
        *,
        db_path: Path,
        webhook_url: str = "",
        rate_limit_per_min: int = 60,
        rate_limit_per_hour: int = 500,
        request_timeout_sec: float = 10.0,
        max_retries: int = 3,
        backoff_base_sec: float = 0.5,
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.webhook_url = str(webhook_url).strip()
        self.rate_limit_per_min = max(1, int(rate_limit_per_min))
        self.rate_limit_per_hour = max(self.rate_limit_per_min, int(rate_limit_per_hour))
        self.request_timeout_sec = max(1.0, float(request_timeout_sec))
        self.max_retries = max(0, int(max_retries))
        self.backoff_base_sec = max(0.05, float(backoff_base_sec))
        self._ensure_schema()

    def update_runtime_config(
        self,
        *,
        webhook_url: str | None = None,
        rate_limit_per_min: int | None = None,
        rate_limit_per_hour: int | None = None,
    ) -> None:
        if webhook_url is not None:
            self.webhook_url = str(webhook_url).strip()
        if rate_limit_per_min is not None:
            self.rate_limit_per_min = max(1, int(rate_limit_per_min))
        if rate_limit_per_hour is not None:
            self.rate_limit_per_hour = max(self.rate_limit_per_min, int(rate_limit_per_hour))

    def dispatch(
        self,
        payload: dict[str, Any],
        *,
        dry_run: bool = True,
        kill_switch: bool = False,
        max_positions_per_strategy: int = 1,
    ) -> SignalDispatchResult:
        action = str(payload.get("action", "")).strip().lower()
        extras = payload.get("extras", {})
        extras = extras if isinstance(extras, dict) else {}
        signal_id = str(extras.get("signalId", payload.get("signalId", ""))).strip()
        if not signal_id:
            signal_id = self._fallback_signal_id(payload=payload)
        position_key = str(extras.get("positionKey", payload.get("positionKey", ""))).strip() or None
        now_ts = time.time()
        now_iso = self._iso_utc(now_ts)

        if not self._insert_pending(
            signal_id=signal_id,
            position_key=position_key,
            action=action,
            payload=payload,
            created_ts=now_ts,
            created_at=now_iso,
        ):
            return SignalDispatchResult(
                signal_id=signal_id,
                action=action,
                status="duplicate",
                response_code=None,
                retries=0,
                message="Signal dropped: duplicate signal_id.",
                position_key=position_key,
            )

        if kill_switch and action in {"buy", "sell", "add"}:
            self._update_signal(
                signal_id=signal_id,
                status="blocked_kill_switch",
                response_code=None,
                retries=0,
                message="Kill switch active.",
            )
            return SignalDispatchResult(
                signal_id=signal_id,
                action=action,
                status="blocked_kill_switch",
                response_code=None,
                retries=0,
                message="Kill switch active.",
                position_key=position_key,
            )

        if action in {"buy", "sell", "add"} and self._rate_limited(now_ts=now_ts):
            self._update_signal(
                signal_id=signal_id,
                status="blocked_rate_limit",
                response_code=None,
                retries=0,
                message="Rate limit exceeded.",
            )
            return SignalDispatchResult(
                signal_id=signal_id,
                action=action,
                status="blocked_rate_limit",
                response_code=None,
                retries=0,
                message="Rate limit exceeded.",
                position_key=position_key,
            )

        if position_key:
            if not self._can_dispatch_for_position(
                position_key=position_key,
                action=action,
                max_positions=max(1, int(max_positions_per_strategy)),
            ):
                self._update_signal(
                    signal_id=signal_id,
                    status="blocked_position_lock",
                    response_code=None,
                    retries=0,
                    message="Position lock blocked action.",
                )
                return SignalDispatchResult(
                    signal_id=signal_id,
                    action=action,
                    status="blocked_position_lock",
                    response_code=None,
                    retries=0,
                    message="Position lock blocked action.",
                    position_key=position_key,
                )

        if dry_run:
            self._update_signal(
                signal_id=signal_id,
                status="dry_run",
                response_code=None,
                retries=0,
                message="Dry run only.",
            )
            self._apply_position_transition(position_key=position_key, action=action)
            return SignalDispatchResult(
                signal_id=signal_id,
                action=action,
                status="dry_run",
                response_code=None,
                retries=0,
                message="Dry run only.",
                position_key=position_key,
            )

        if not self.webhook_url:
            self._update_signal(
                signal_id=signal_id,
                status="failed_no_webhook",
                response_code=None,
                retries=0,
                message="Webhook URL missing.",
            )
            return SignalDispatchResult(
                signal_id=signal_id,
                action=action,
                status="failed_no_webhook",
                response_code=None,
                retries=0,
                message="Webhook URL missing.",
                position_key=position_key,
            )

        retries = 0
        response_code: int | None = None
        message = ""
        status = "failed_network"
        while True:
            try:
                response = requests.post(
                    self.webhook_url,
                    json=payload,
                    timeout=self.request_timeout_sec,
                )
                response_code = int(response.status_code)
                if 200 <= response_code < 300:
                    status = "sent"
                    message = "Signal sent."
                    break
                if response_code in RETRYABLE_STATUS_CODES and retries < self.max_retries:
                    retries += 1
                    status = "retrying"
                    message = f"Retryable HTTP {response_code}."
                    self._update_signal(
                        signal_id=signal_id,
                        status=status,
                        response_code=response_code,
                        retries=retries,
                        message=message,
                    )
                    time.sleep(self.backoff_base_sec * (2 ** (retries - 1)))
                    continue
                status = "failed_http"
                message = f"HTTP {response_code}"
                break
            except requests.RequestException as exc:
                if retries < self.max_retries:
                    retries += 1
                    status = "retrying"
                    message = str(exc)
                    self._update_signal(
                        signal_id=signal_id,
                        status=status,
                        response_code=response_code,
                        retries=retries,
                        message=message,
                    )
                    time.sleep(self.backoff_base_sec * (2 ** (retries - 1)))
                    continue
                status = "failed_network"
                message = str(exc)
                break

        self._update_signal(
            signal_id=signal_id,
            status=status,
            response_code=response_code,
            retries=retries,
            message=message,
        )
        if status == "sent":
            self._apply_position_transition(position_key=position_key, action=action)

        return SignalDispatchResult(
            signal_id=signal_id,
            action=action,
            status=status,
            response_code=response_code,
            retries=retries,
            message=message or status,
            position_key=position_key,
        )

    def dispatch_local(
        self,
        payload: dict[str, Any],
        *,
        kill_switch: bool = False,
        max_positions_per_strategy: int = 1,
        queued_status: str = "queued_local_cdp",
        queued_message: str = "Queued for Custom Human CDP worker.",
    ) -> SignalDispatchResult:
        """Run the same guardrails as dispatch(), but without sending a webhook."""
        result = self.dispatch(
            payload,
            dry_run=True,
            kill_switch=kill_switch,
            max_positions_per_strategy=max_positions_per_strategy,
        )
        if result.status != "dry_run":
            return result

        self._update_signal(
            signal_id=result.signal_id,
            status=str(queued_status),
            response_code=None,
            retries=0,
            message=str(queued_message),
        )
        return SignalDispatchResult(
            signal_id=result.signal_id,
            action=result.action,
            status=str(queued_status),
            response_code=None,
            retries=0,
            message=str(queued_message),
            position_key=result.position_key,
        )

    def status_summary(self) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT signal_id, action, status, response_code, retries, position_key
                FROM signal_log
                ORDER BY created_ts DESC
                LIMIT 1
                """
            ).fetchone()
            queue_count = conn.execute(
                "SELECT COUNT(*) FROM signal_log WHERE status IN ('pending', 'retrying')"
            ).fetchone()[0]
            retry_total = conn.execute("SELECT COALESCE(SUM(retries), 0) FROM signal_log").fetchone()[0]
        return {
            "last_signal_id": row[0] if row else "",
            "last_action": row[1] if row else "",
            "last_status": row[2] if row else "",
            "last_response_code": row[3] if row else None,
            "last_retries": int(row[4]) if row else 0,
            "last_position_key": row[5] if row else "",
            "queue_count": int(queue_count or 0),
            "retry_count_total": int(retry_total or 0),
        }

    def release_local_signals_for_flat_position(self, *, position_key: str) -> dict[str, int]:
        """Clear stale local CDP signals once broker state is confirmed flat."""
        token = str(position_key or "").strip()
        if not token:
            return {"signals_deleted": 0, "locks_released": 0}

        now_iso = self._iso_utc(time.time())
        with self._connect() as conn:
            signals_deleted = conn.execute(
                """
                DELETE FROM signal_log
                WHERE position_key = ?
                  AND status IN ('queued_local_cdp', 'dry_run')
                """,
                (token,),
            ).rowcount
            locks_released = conn.execute(
                """
                UPDATE position_lock
                SET is_open = 0, updated_at = ?
                WHERE position_key = ? AND is_open <> 0
                """,
                (now_iso, token),
            ).rowcount
            conn.commit()
        return {
            "signals_deleted": int(signals_deleted or 0),
            "locks_released": int(locks_released or 0),
        }

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_log (
                    signal_id TEXT PRIMARY KEY,
                    position_key TEXT,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_ts REAL NOT NULL,
                    sent_at TEXT,
                    response_code INTEGER,
                    retries INTEGER NOT NULL DEFAULT 0,
                    message TEXT,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS position_lock (
                    position_key TEXT PRIMARY KEY,
                    is_open INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_created_ts ON signal_log(created_ts)")
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _iso_utc(ts: float) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))

    @staticmethod
    def _fallback_signal_id(payload: dict[str, Any]) -> str:
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
        return f"sig_{abs(hash(encoded))}"

    def _insert_pending(
        self,
        *,
        signal_id: str,
        position_key: str | None,
        action: str,
        payload: dict[str, Any],
        created_ts: float,
        created_at: str,
    ) -> bool:
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO signal_log (
                        signal_id, position_key, action, status, created_at, created_ts, payload_json
                    ) VALUES (?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (
                        signal_id,
                        position_key,
                        action,
                        created_at,
                        float(created_ts),
                        json.dumps(payload, ensure_ascii=True, sort_keys=True),
                    ),
                )
                conn.commit()
                return True
        except sqlite3.IntegrityError:
            return False

    def _update_signal(
        self,
        *,
        signal_id: str,
        status: str,
        response_code: int | None,
        retries: int,
        message: str,
    ) -> None:
        now = self._iso_utc(time.time())
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE signal_log
                SET status = ?, response_code = ?, retries = ?, message = ?, sent_at = ?
                WHERE signal_id = ?
                """,
                (status, response_code, int(retries), str(message), now, signal_id),
            )
            conn.commit()

    def _rate_limited(self, *, now_ts: float) -> bool:
        min_since = now_ts - 60.0
        hour_since = now_ts - 3600.0
        with self._connect() as conn:
            per_min = conn.execute(
                """
                SELECT COUNT(*) FROM signal_log
                WHERE created_ts >= ? AND status NOT IN ('duplicate')
                """,
                (float(min_since),),
            ).fetchone()[0]
            per_hour = conn.execute(
                """
                SELECT COUNT(*) FROM signal_log
                WHERE created_ts >= ? AND status NOT IN ('duplicate')
                """,
                (float(hour_since),),
            ).fetchone()[0]
        return int(per_min) > int(self.rate_limit_per_min) or int(per_hour) > int(self.rate_limit_per_hour)

    def _can_dispatch_for_position(self, *, position_key: str, action: str, max_positions: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT is_open FROM position_lock WHERE position_key = ?",
                (position_key,),
            ).fetchone()
        is_open = int(row[0]) if row else 0
        if action in ENTRY_ACTIONS:
            return is_open < max(1, int(max_positions))
        if action == "add":
            return is_open > 0
        return True

    def _apply_position_transition(self, *, position_key: str | None, action: str) -> None:
        if not position_key:
            return
        now_iso = self._iso_utc(time.time())
        open_state = None
        if action in ENTRY_ACTIONS or action == "add":
            open_state = 1
        elif action in {"exit", "cancel"}:
            open_state = 0
        if open_state is None:
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO position_lock (position_key, is_open, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(position_key) DO UPDATE SET
                    is_open = excluded.is_open,
                    updated_at = excluded.updated_at
                """,
                (position_key, int(open_state), now_iso),
            )
            conn.commit()
