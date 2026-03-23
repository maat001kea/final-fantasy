from __future__ import annotations

import contextlib
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator


LOGGER = logging.getLogger("final_fantasy.trading_engine_bridge")


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@contextlib.contextmanager
def _open_db(db_path: str | Path) -> Generator[sqlite3.Connection, None, None]:
    """Open a SQLite connection and GUARANTEE it is closed on exit.

    Python's plain ``sqlite3.Connection`` context manager only
    commits/rolls-back — it never calls ``conn.close()``.  In a tight
    engine poll-loop that means one leaked FD per iteration, eventually
    exhausting the OS file-descriptor limit and stalling all writes.
    This wrapper closes the connection in the ``finally`` block so the
    FD is released immediately after every ``with _open_db(...):`` block.
    """
    conn = sqlite3.connect(Path(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    # WAL mode prevents the UI read-loop from blocking the engine write-loop.
    # NORMAL sync gives a good durability/speed balance (no fsync on every write).
    # These PRAGMAs are idempotent and take effect immediately.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Legacy alias kept so any external caller that uses _connect() directly
# still works.  New internal code should use _open_db() instead.
# ---------------------------------------------------------------------------
def _connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(Path(db_path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_bridge(db_path: str | Path) -> Path:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _open_db(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS engine_status (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ui_commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                command TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                claimed_at TEXT,
                completed_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                command TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                claimed_at TEXT,
                completed_at TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO engine_status (id, payload_json, updated_at)
            VALUES (1, '{}', ?)
            ON CONFLICT(id) DO NOTHING
            """,
            (_utc_now(),),
        )
        conn.commit()
    return path


def publish_status(db_path: str | Path, payload: dict[str, Any]) -> None:
    path = init_bridge(db_path)
    encoded = json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=True)
    updated_at = _utc_now()
    with _open_db(path) as conn:
        conn.execute(
            """
            INSERT INTO engine_status (id, payload_json, updated_at)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            (encoded, updated_at),
        )
        conn.commit()


def publish_neutral_status(
    db_path: str | Path,
    *,
    last_stop_reason: str = "",
) -> None:
    publish_status(
        db_path,
        {
            "active": False,
            "connected": False,
            "running": False,
            "runtime_active": False,
            "auto_requested": False,
            "halted": True,
            "last_stop_reason": str(last_stop_reason or ""),
            "live_observer_status": str(last_stop_reason or "Stoppet."),
            "tradovate_snapshot_status": str(last_stop_reason or "Stoppet."),
        },
    )


def fetch_status(db_path: str | Path) -> dict[str, Any]:
    path = init_bridge(db_path)
    with _open_db(path) as conn:
        row = conn.execute(
            "SELECT payload_json, updated_at FROM engine_status WHERE id = 1"
        ).fetchone()
    if row is None:
        return {}
    try:
        payload = json.loads(str(row["payload_json"] or "{}"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    payload["_bridge_updated_at"] = str(row["updated_at"] or "")
    return payload


def enqueue_command(
    db_path: str | Path,
    command: str,
    payload: dict[str, Any] | None = None,
) -> int:
    path = init_bridge(db_path)
    command_token = str(command or "").strip().upper()
    encoded = json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=True)
    created_at = _utc_now()
    with _open_db(path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO ui_commands (command, payload_json, status, created_at)
            VALUES (?, ?, 'pending', ?)
            """,
            (command_token, encoded, created_at),
        )
        conn.commit()
        return int(cursor.lastrowid)


def scrub_stale_commands(db_path: str | Path) -> int:
    """Mark all pending commands as 'stale' on engine startup.

    Prevents ghost orders from a prior (possibly crashed) session from being
    executed when the engine restarts. Returns the number of rows scrubbed.
    """
    path = init_bridge(db_path)
    updated_at = _utc_now()
    with _open_db(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        commands_count = 0
        ui_count = 0
        try:
            cursor = conn.execute(
                """
                UPDATE commands
                SET status = 'stale',
                    error = 'invalidated_by_reboot'
                WHERE status = 'pending'
                """
            )
            commands_count = int(cursor.rowcount)
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise
        try:
            cursor = conn.execute(
                """
                UPDATE ui_commands
                SET status = 'stale',
                    completed_at = ?
                WHERE status = 'pending'
                """,
                (updated_at,),
            )
            ui_count = int(cursor.rowcount)
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise
        conn.commit()
    total = commands_count + ui_count
    LOGGER.info(
        "[GHOST-V6.6] Housekeeper: Scrubbed %d stale command(s).",
        total,
    )
    return total


def claim_commands(db_path: str | Path, *, limit: int = 100) -> list[dict[str, Any]]:
    path = init_bridge(db_path)
    limit_value = max(1, int(limit))
    claimed_at = _utc_now()
    commands: list[dict[str, Any]] = []
    with _open_db(path) as conn:
        rows = conn.execute(
            """
            SELECT id, command, payload_json, created_at
            FROM ui_commands
            WHERE status = 'pending'
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit_value,),
        ).fetchall()
        if not rows:
            return []
        ids = [int(row["id"]) for row in rows]
        placeholders = ",".join("?" for _ in ids)
        conn.execute(
            f"""
            UPDATE ui_commands
            SET status = 'claimed',
                claimed_at = ?,
                completed_at = ?
            WHERE id IN ({placeholders})
            """,
            (claimed_at, claimed_at, *ids),
        )
        conn.commit()

    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except json.JSONDecodeError:
            payload = {}
        commands.append(
            {
                "id": int(row["id"]),
                "command": str(row["command"] or "").strip().upper(),
                "payload": payload if isinstance(payload, dict) else {},
                "created_at": str(row["created_at"] or ""),
                "claimed_at": claimed_at,
            }
        )
    return commands
