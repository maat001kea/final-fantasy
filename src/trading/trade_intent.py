"""Trade Intent generator – produces TradeIntent dicts from strategy + bar data."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Any

from .position_sizer import compute_position_size, load_sizing_config
from .strategy_loader import StrategyDSL


def _compute_idempotency_key(
    strategy: str,
    instrument: str,
    direction: str,
    trade_date: str,
    suffix: str = "entry",
    bar_index: int | None = None,
) -> str:
    """Compute a deterministic SHA-256 idempotency key including a suffix for Adds.

    ``bar_index`` is appended when provided so that two valid entry signals on
    different bars of the same trading day produce distinct keys.
    """
    raw = f"{strategy}|{instrument}|{direction}|{trade_date}|{suffix}"
    if bar_index is not None:
        raw = f"{raw}|{bar_index}"
    return hashlib.sha256(raw.encode()).hexdigest()


def create_trade_intent(
    strategy: StrategyDSL,
    bar_data: dict[str, Any],
    account_balance: float,
    sizing_config: dict[str, Any] | None = None,
    now: datetime | None = None,
    direction: str | None = None,
    order_type: str = "entry",
) -> dict[str, Any] | None:
    """Create a TradeIntent dict from a StrategyDSL + bar_data.

    Returns None if the strategy does not match the current context,
    or if required bar data fields are missing.
    """
    if not strategy.matches_context(bar_data):
        return None

    instrument = str(bar_data.get("instrument", ""))
    if instrument and strategy.instruments and instrument not in strategy.instruments:
        return None

    resolved_direction = str(direction or strategy.direction).strip().lower()
    if resolved_direction not in {"long", "short"}:
        return None

    entry_source = strategy.entry_source_for(resolved_direction)
    stop_source = strategy.stop_source_for(resolved_direction)
    entry_price = bar_data.get(entry_source)
    stop_price = bar_data.get(stop_source)

    if entry_price is None or stop_price is None:
        return None

    entry_price = float(entry_price)
    stop_price = float(stop_price)
    risk_pts = abs(entry_price - stop_price)

    if sizing_config is None:
        sizing_config = load_sizing_config()

    sizing = compute_position_size(
        account_balance=account_balance,
        risk_pts=risk_pts,
        instrument=instrument,
        config=sizing_config,
    )

    if now is None:
        now = datetime.now(tz=timezone.utc)
    trade_date = now.date().isoformat()
    _bar_idx = bar_data.get("bar_index")
    _bar_idx_int = int(_bar_idx) if _bar_idx is not None else None
    idempotency_key = _compute_idempotency_key(
        strategy.name, instrument, resolved_direction, trade_date,
        suffix=order_type, bar_index=_bar_idx_int,
    )

    return {
        "strategy_name": strategy.name,
        "instrument": instrument,
        "direction": resolved_direction,
        "entry_price": entry_price,
        "stop_price": stop_price,
        "risk_pts": risk_pts,
        "quantity": sizing.quantity,
        "risk_amount": sizing.risk_amount,
        "risk_pct": sizing.risk_pct,
        "idempotency_key": idempotency_key,
        "order_type": order_type,
        "trade_date": trade_date,
        "created_at": now.isoformat(),
        "context_snapshot": dict(bar_data),
        "sizing_model": sizing.model_used,
    }
