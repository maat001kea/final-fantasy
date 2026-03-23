"""SQLite audit database – INSERT-only audit trail for trade intents and decisions."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


class AuditDB:
    """Append-only SQLite audit database."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.db_path = str(db_path)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        # WAL mode: readers never block writers; safe for concurrent Streamlit + engine access.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        """Create audit tables if they don't exist."""
        with self._conn:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS trade_intents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry_price REAL,
                    stop_price REAL,
                    risk_pts REAL,
                    quantity REAL,
                    risk_amount REAL,
                    trade_date TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    context_json TEXT
                );

                CREATE TABLE IF NOT EXISTS gate_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT NOT NULL,
                    approved INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    checks_passed TEXT,
                    checks_failed TEXT,
                    state_snapshot TEXT,
                    decided_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS broker_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT NOT NULL,
                    order_id TEXT,
                    broker TEXT NOT NULL,
                    status TEXT NOT NULL,
                    fill_price REAL,
                    error_message TEXT,
                    raw_response TEXT,
                    placed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS daily_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_date TEXT NOT NULL,
                    daily_pnl REAL,
                    weekly_pnl REAL,
                    monthly_pnl REAL,
                    trades_today INTEGER,
                    account_balance REAL,
                    notes TEXT,
                    created_at TEXT NOT NULL
                );
            """)

    def log_intent(self, intent: dict[str, Any]) -> int:
        """Log a trade intent. Returns the row id."""
        with self._conn:
            cur = self._conn.execute(
                """INSERT INTO trade_intents
                   (idempotency_key, strategy_name, instrument, direction,
                    entry_price, stop_price, risk_pts, quantity, risk_amount,
                    trade_date, created_at, context_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    intent.get("idempotency_key", ""),
                    intent.get("strategy_name", ""),
                    intent.get("instrument", ""),
                    intent.get("direction", ""),
                    intent.get("entry_price"),
                    intent.get("stop_price"),
                    intent.get("risk_pts"),
                    intent.get("quantity"),
                    intent.get("risk_amount"),
                    intent.get("trade_date", ""),
                    intent.get("created_at", datetime.now(tz=timezone.utc).isoformat()),
                    json.dumps(intent.get("context_snapshot", {})),
                ),
            )
        return int(cur.lastrowid)

    def log_gate_decision(self, idempotency_key: str, decision: Any) -> int:
        """Log a gate decision. Returns the row id."""
        with self._conn:
            cur = self._conn.execute(
                """INSERT INTO gate_decisions
                   (idempotency_key, approved, reason, checks_passed, checks_failed,
                    state_snapshot, decided_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    idempotency_key,
                    1 if decision.approved else 0,
                    decision.reason,
                    json.dumps(decision.checks_passed),
                    json.dumps(decision.checks_failed),
                    json.dumps(decision.state_snapshot),
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
        return int(cur.lastrowid)

    def log_broker_order(self, idempotency_key: str, order_result: Any, broker: str = "") -> int:
        """Log a broker order result. Returns the row id."""
        with self._conn:
            cur = self._conn.execute(
                """INSERT INTO broker_orders
                   (idempotency_key, order_id, broker, status, fill_price,
                    error_message, raw_response, placed_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    idempotency_key,
                    getattr(order_result, "order_id", None),
                    broker,
                    str(getattr(order_result, "status", "")),
                    getattr(order_result, "fill_price", None),
                    getattr(order_result, "error_message", None),
                    json.dumps(getattr(order_result, "raw_response", {})),
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
        return int(cur.lastrowid)

    def log_daily_snapshot(
        self,
        *,
        snapshot_date: str,
        daily_pnl: float,
        weekly_pnl: float,
        monthly_pnl: float,
        trades_today: int,
        account_balance: float,
        notes: str = "",
    ) -> int:
        """Log a daily snapshot. Returns the row id."""
        with self._conn:
            cur = self._conn.execute(
                """INSERT INTO daily_snapshots
                   (snapshot_date, daily_pnl, weekly_pnl, monthly_pnl,
                    trades_today, account_balance, notes, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    snapshot_date,
                    daily_pnl,
                    weekly_pnl,
                    monthly_pnl,
                    trades_today,
                    account_balance,
                    notes,
                    datetime.now(tz=timezone.utc).isoformat(),
                ),
            )
        return int(cur.lastrowid)

    def get_todays_stats(self, trade_date: str | None = None) -> dict[str, Any]:
        """Return aggregate stats for today."""
        if trade_date is None:
            trade_date = date.today().isoformat()
        row = self._conn.execute(
            """SELECT COUNT(*) as total_intents FROM trade_intents WHERE trade_date = ?""",
            (trade_date,),
        ).fetchone()
        approved = self._conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM gate_decisions gd
            INNER JOIN trade_intents ti
                ON ti.idempotency_key = gd.idempotency_key
            WHERE ti.trade_date = ? AND gd.approved = 1
            """,
            (trade_date,),
        ).fetchone()
        rejected = self._conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM gate_decisions gd
            INNER JOIN trade_intents ti
                ON ti.idempotency_key = gd.idempotency_key
            WHERE ti.trade_date = ? AND gd.approved = 0
            """,
            (trade_date,),
        ).fetchone()
        return {
            "trade_date": trade_date,
            "total_intents": row["total_intents"] if row else 0,
            "approved": approved["cnt"] if approved else 0,
            "rejected": rejected["cnt"] if rejected else 0,
        }

    def get_recent_decisions(self, limit: int = 20) -> list[dict[str, Any]]:
        """Return the most recent gate decisions."""
        rows = self._conn.execute(
            """SELECT * FROM gate_decisions ORDER BY id DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
