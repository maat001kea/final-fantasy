"""Deterministic Risk & Execution Gate."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any


@dataclass
class RiskConfig:
    """Risk configuration loaded from account_config.yaml."""

    currency: str = "GBP"
    starting_balance: float = 1000.0
    max_daily_loss_abs: float = 30.0
    max_daily_loss_pct: float = 3.0
    kill_switch_at_loss_pct: float = 5.0
    max_risk_per_contract_abs: float | None = None
    max_weekly_loss_abs: float = 100.0
    max_monthly_loss_abs: float = 250.0
    daily_profit_target_abs: float = 50.0
    max_trades_per_day: int = 3
    max_trades_per_hour: int = 2
    loss_cooldown_seconds: int = 300
    consecutive_loss_cooldown_seconds: int = 1800
    consecutive_loss_threshold: int = 3

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> RiskConfig:
        """Create RiskConfig from a dictionary."""
        return cls(
            currency=str(d.get("currency", "GBP")),
            starting_balance=float(d.get("starting_balance", 1000.0)),
            max_daily_loss_abs=float(d.get("max_daily_loss_abs", 30.0)),
            max_daily_loss_pct=float(d.get("max_daily_loss_pct", 3.0)),
            kill_switch_at_loss_pct=float(d.get("kill_switch_at_loss_pct", 5.0)),
            max_risk_per_contract_abs=(
                float(d["max_risk_per_contract_abs"])
                if d.get("max_risk_per_contract_abs") is not None
                else None
            ),
            max_weekly_loss_abs=float(d.get("max_weekly_loss_abs", 100.0)),
            max_monthly_loss_abs=float(d.get("max_monthly_loss_abs", 250.0)),
            daily_profit_target_abs=float(d.get("daily_profit_target_abs", 50.0)),
            max_trades_per_day=int(d.get("max_trades_per_day", 3)),
            max_trades_per_hour=int(d.get("max_trades_per_hour", 2)),
            loss_cooldown_seconds=int(d.get("loss_cooldown_seconds", 300)),
            consecutive_loss_cooldown_seconds=int(d.get("consecutive_loss_cooldown_seconds", 1800)),
            consecutive_loss_threshold=int(d.get("consecutive_loss_threshold", 3)),
        )


@dataclass
class GateState:
    """Mutable runtime state; reset daily."""

    daily_pnl: float = 0.0
    weekly_pnl: float = 0.0
    monthly_pnl: float = 0.0
    trades_today: int = 0
    trades_this_hour: int = 0
    last_trade_time: datetime | None = None
    last_loss_time: datetime | None = None
    consecutive_losses: int = 0
    kill_switch_active: bool = False
    circuit_breaker_active: bool = False
    seen_idempotency_keys: set[str] = field(default_factory=set)
    current_date: date = field(default_factory=lambda: datetime.now(tz=timezone.utc).date())


@dataclass(frozen=True)
class GateDecision:
    """Immutable gate decision."""

    approved: bool
    reason: str
    checks_passed: list[str]
    checks_failed: list[str]
    state_snapshot: dict[str, Any]


def _compute_idempotency_key(
    strategy: str,
    instrument: str,
    direction: str,
    trade_date: str,
    suffix: str = "entry",
    bar_index: int | None = None,
) -> str:
    """Compute a deterministic SHA-256 idempotency key.

    ``bar_index`` is appended when provided so that two valid entry signals on
    different bars of the same trading day produce distinct keys, preventing
    the risk gate from blocking a legitimate second setup after an earlier
    same-day trade.
    """
    raw = f"{strategy}|{instrument}|{direction}|{trade_date}|{suffix}"
    if bar_index is not None:
        raw = f"{raw}|{bar_index}"
    return hashlib.sha256(raw.encode()).hexdigest()


class RiskGate:
    """Deterministic Risk & Execution Gate."""

    def __init__(self, config: RiskConfig, account_balance: float | None = None) -> None:
        self.config = config
        self.account_balance = account_balance or config.starting_balance
        self.state = GateState()

    def export_state(self) -> dict[str, Any]:
        """Serialize runtime gate state for persistence across restarts."""
        return {
            "daily_pnl": float(self.state.daily_pnl),
            "weekly_pnl": float(self.state.weekly_pnl),
            "monthly_pnl": float(self.state.monthly_pnl),
            "trades_today": int(self.state.trades_today),
            "trades_this_hour": int(self.state.trades_this_hour),
            "last_trade_time": self.state.last_trade_time.isoformat() if self.state.last_trade_time else "",
            "last_loss_time": self.state.last_loss_time.isoformat() if self.state.last_loss_time else "",
            "consecutive_losses": int(self.state.consecutive_losses),
            "kill_switch_active": bool(self.state.kill_switch_active),
            "circuit_breaker_active": bool(self.state.circuit_breaker_active),
            "seen_idempotency_keys": sorted(str(item) for item in self.state.seen_idempotency_keys),
            "current_date": self.state.current_date.isoformat(),
        }

    def restore_state(self, payload: dict[str, Any] | None) -> None:
        """Restore runtime gate state from a serialized payload."""
        if not isinstance(payload, dict):
            return

        def _parse_datetime(value: Any) -> datetime | None:
            token = str(value or "").strip()
            if not token:
                return None
            try:
                parsed = datetime.fromisoformat(token)
            except ValueError:
                return None
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)

        def _parse_date(value: Any) -> date:
            token = str(value or "").strip()
            if not token:
                return datetime.now(tz=timezone.utc).date()
            try:
                return date.fromisoformat(token)
            except ValueError:
                return datetime.now(tz=timezone.utc).date()

        state = self.state
        state.daily_pnl = float(payload.get("daily_pnl", 0.0) or 0.0)
        state.weekly_pnl = float(payload.get("weekly_pnl", 0.0) or 0.0)
        state.monthly_pnl = float(payload.get("monthly_pnl", 0.0) or 0.0)
        state.trades_today = int(payload.get("trades_today", 0) or 0)
        state.trades_this_hour = int(payload.get("trades_this_hour", 0) or 0)
        state.last_trade_time = _parse_datetime(payload.get("last_trade_time"))
        state.last_loss_time = _parse_datetime(payload.get("last_loss_time"))
        state.consecutive_losses = int(payload.get("consecutive_losses", 0) or 0)
        state.kill_switch_active = bool(payload.get("kill_switch_active", False))
        state.circuit_breaker_active = bool(payload.get("circuit_breaker_active", False))
        restored_date = _parse_date(payload.get("current_date"))
        today = datetime.now(tz=timezone.utc).date()
        if restored_date < today:
            # New trading day: always start with a clean slate so yesterday's
            # seen keys do not block today's first signal on restart.
            state.seen_idempotency_keys = set()
            state.current_date = today
        else:
            seen_keys = payload.get("seen_idempotency_keys", [])
            if isinstance(seen_keys, (list, tuple, set)):
                state.seen_idempotency_keys = {str(item).strip() for item in seen_keys if str(item).strip()}
            else:
                state.seen_idempotency_keys = set()
            state.current_date = restored_date

    def evaluate(
        self,
        *,
        strategy: str,
        instrument: str,
        direction: str,
        quantity: float,
        risk_pts: float,
        tick_value: float = 1.0,
        now: datetime | None = None,
        suffix: str = "entry",
        bar_index: int | None = None,
    ) -> GateDecision:
        """Run all risk checks and return a GateDecision."""
        if now is None:
            now = datetime.now(tz=timezone.utc)

        checks_passed: list[str] = []
        checks_failed: list[str] = []

        trade_date = now.date().isoformat()
        idempotency_key = _compute_idempotency_key(
            strategy,
            instrument,
            direction,
            trade_date,
            suffix=suffix,
            bar_index=bar_index,
        )
        risk_amount = quantity * risk_pts * tick_value

        def _state_snapshot() -> dict[str, Any]:
            return {
                "daily_pnl": self.state.daily_pnl,
                "weekly_pnl": self.state.weekly_pnl,
                "monthly_pnl": self.state.monthly_pnl,
                "trades_today": self.state.trades_today,
                "consecutive_losses": self.state.consecutive_losses,
                "kill_switch_active": self.state.kill_switch_active,
                "circuit_breaker_active": self.state.circuit_breaker_active,
            }

        def _reject(reason: str, check: str) -> GateDecision:
            checks_failed.append(check)
            return GateDecision(
                approved=False,
                reason=reason,
                checks_passed=list(checks_passed),
                checks_failed=list(checks_failed),
                state_snapshot=_state_snapshot(),
            )

        # 1. Kill switch
        if self.state.kill_switch_active:
            return _reject("Kill switch is active", "kill_switch")
        checks_passed.append("kill_switch")

        # 2. Circuit breaker
        if self.state.circuit_breaker_active:
            return _reject("Circuit breaker is active", "circuit_breaker")
        checks_passed.append("circuit_breaker")

        # 3. Idempotency / dedup
        if idempotency_key in self.state.seen_idempotency_keys:
            return _reject(f"Duplicate trade intent: {idempotency_key[:16]}…", "idempotency")
        checks_passed.append("idempotency")

        # 4. Daily loss abs limit
        if self.state.daily_pnl <= -abs(self.config.max_daily_loss_abs):
            return _reject(
                f"Daily loss limit reached: {self.state.daily_pnl:.2f} <= -{self.config.max_daily_loss_abs}",
                "daily_loss_abs",
            )
        checks_passed.append("daily_loss_abs")

        # 5. Daily loss pct limit
        max_loss_pct_abs = self.account_balance * (self.config.max_daily_loss_pct / 100.0)
        if self.state.daily_pnl <= -max_loss_pct_abs:
            return _reject(
                f"Daily loss pct limit reached: {self.state.daily_pnl:.2f}",
                "daily_loss_pct",
            )
        checks_passed.append("daily_loss_pct")

        # 6. Weekly loss limit
        if self.state.weekly_pnl <= -abs(self.config.max_weekly_loss_abs):
            return _reject(
                f"Weekly loss limit reached: {self.state.weekly_pnl:.2f}",
                "weekly_loss",
            )
        checks_passed.append("weekly_loss")

        # 7. Monthly loss limit
        if self.state.monthly_pnl <= -abs(self.config.max_monthly_loss_abs):
            return _reject(
                f"Monthly loss limit reached: {self.state.monthly_pnl:.2f}",
                "monthly_loss",
            )
        checks_passed.append("monthly_loss")

        # 8. Daily profit target (discipline stop)
        if self.state.daily_pnl >= self.config.daily_profit_target_abs:
            return _reject(
                f"Daily profit target reached: {self.state.daily_pnl:.2f}",
                "daily_profit_target",
            )
        checks_passed.append("daily_profit_target")

        # 9. Trades per day limit
        if self.state.trades_today >= self.config.max_trades_per_day:
            return _reject(
                f"Max trades per day reached: {self.state.trades_today}",
                "trades_per_day",
            )
        checks_passed.append("trades_per_day")

        # 10. Trades per hour limit
        if self.state.trades_this_hour >= self.config.max_trades_per_hour:
            return _reject(
                f"Max trades per hour reached: {self.state.trades_this_hour}",
                "trades_per_hour",
            )
        checks_passed.append("trades_per_hour")

        # 11. Consecutive loss cooldown
        if self.state.last_loss_time is not None:
            elapsed = (now - self.state.last_loss_time).total_seconds()
            if self.state.consecutive_losses >= self.config.consecutive_loss_threshold:
                cooldown = self.config.consecutive_loss_cooldown_seconds
                if elapsed < cooldown:
                    return _reject(
                        f"Extended cooldown active: {cooldown - elapsed:.0f}s remaining",
                        "consecutive_loss_cooldown",
                    )
            else:
                cooldown = self.config.loss_cooldown_seconds
                if elapsed < cooldown:
                    return _reject(
                        f"Loss cooldown active: {cooldown - elapsed:.0f}s remaining",
                        "loss_cooldown",
                    )
        checks_passed.append("cooldown")

        # 12. Position size check
        risk_per_contract = risk_pts * tick_value
        configured_contract_cap = self.config.max_risk_per_contract_abs
        if configured_contract_cap is not None and configured_contract_cap > 0:
            kill_threshold = float(quantity) * float(configured_contract_cap)
            if risk_per_contract > configured_contract_cap:
                return _reject(
                    (
                        f"Risk per contract {risk_per_contract:.2f} exceeds "
                        f"configured cap {configured_contract_cap:.2f}"
                    ),
                    "risk_per_contract",
                )
        else:
            kill_threshold = self.account_balance * (self.config.kill_switch_at_loss_pct / 100.0)
        if risk_amount > kill_threshold:
            return _reject(
                f"Position size risk {risk_amount:.2f} exceeds kill-switch threshold {kill_threshold:.2f}",
                "position_size",
            )
        checks_passed.append("position_size")

        # All checks passed – register idempotency key
        self.state.seen_idempotency_keys.add(idempotency_key)

        return GateDecision(
            approved=True,
            reason="All checks passed",
            checks_passed=checks_passed,
            checks_failed=[],
            state_snapshot=_state_snapshot(),
        )

    def release_idempotency_key(
        self,
        *,
        strategy: str,
        instrument: str,
        direction: str,
        trade_date: str,
        suffix: str = "entry",
        bar_index: int | None = None,
    ) -> bool:
        """Release a previously reserved idempotency key after downstream failure."""
        key = _compute_idempotency_key(
            strategy, instrument, direction, trade_date, suffix=suffix, bar_index=bar_index
        )
        if key not in self.state.seen_idempotency_keys:
            return False
        self.state.seen_idempotency_keys.discard(key)
        return True

    def record_fill(self, pnl: float, now: datetime | None = None) -> None:
        """Update state after a trade is filled."""
        if now is None:
            now = datetime.now(tz=timezone.utc)
        self.state.daily_pnl += pnl
        self.state.weekly_pnl += pnl
        self.state.monthly_pnl += pnl
        self.state.trades_today += 1
        self.state.trades_this_hour += 1
        self.state.last_trade_time = now

        if pnl < 0:
            self.state.consecutive_losses += 1
            self.state.last_loss_time = now
        else:
            self.state.consecutive_losses = 0
            self.state.last_loss_time = None  # <--- SIKKERHEDSVENTIL: Nulstiller loss timer ved overskud!

        # Trigger kill switch if loss exceeds kill_switch_at_loss_pct
        kill_threshold = self.account_balance * (self.config.kill_switch_at_loss_pct / 100.0)
        if self.state.daily_pnl <= -kill_threshold:
            self.state.kill_switch_active = True

    def reset_daily(self) -> None:
        """Reset daily counters (call at start of each trading day)."""
        self.state.daily_pnl = 0.0
        self.state.trades_today = 0
        self.state.trades_this_hour = 0
        self.state.last_trade_time = None
        self.state.last_loss_time = None
        self.state.consecutive_losses = 0
        self.state.seen_idempotency_keys = set()
        self.state.current_date = datetime.now(tz=timezone.utc).date()
        # Do NOT reset kill_switch_active or circuit_breaker_active on daily reset

    def trigger_circuit_breaker(self) -> None:
        """Manually trigger the circuit breaker."""
        self.state.circuit_breaker_active = True
