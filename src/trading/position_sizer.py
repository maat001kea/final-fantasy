"""Position sizing engine."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SizingResult:
    """Immutable position sizing result."""

    quantity: float
    risk_amount: float
    risk_pct: float
    model_used: str
    capped: bool
    reason: str


def load_sizing_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load position sizing config from YAML file."""
    if path is None:
        path = Path(__file__).parent.parent.parent / "config" / "position_sizing.yaml"
    path = Path(path)
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def compute_position_size(
    *,
    account_balance: float,
    risk_pts: float,
    instrument: str = "",
    config: dict[str, Any] | None = None,
) -> SizingResult:
    """Compute position size using fixed_risk_per_trade model.

    Quantity = (account_balance * risk_pct/100) / (risk_pts * tick_value)
    """
    if config is None:
        config = load_sizing_config()

    model = str(config.get("model", "fixed_risk_per_trade"))
    risk_pct = float(config.get("risk_per_trade_pct", 1.0))
    default_min = float(config.get("min_quantity", 0.10))
    default_max = float(config.get("max_quantity", 5.0))

    # Instrument overrides
    overrides: dict[str, Any] = config.get("instrument_overrides", {}) or {}
    inst_cfg = overrides.get(instrument, {})
    tick_value = float(inst_cfg.get("tick_value_gbp", 1.0))
    min_qty = float(inst_cfg.get("min_quantity", default_min))
    max_qty = float(inst_cfg.get("max_quantity", default_max))

    if risk_pts <= 0:
        return SizingResult(
            quantity=min_qty,
            risk_amount=0.0,
            risk_pct=0.0,
            model_used=model,
            capped=True,
            reason="risk_pts is zero or negative; returning minimum quantity",
        )

    risk_amount = account_balance * (risk_pct / 100.0)
    raw_quantity = risk_amount / (risk_pts * tick_value)

    capped = False
    quantity = raw_quantity
    reason = "computed"

    if quantity < min_qty:
        quantity = min_qty
        capped = True
        reason = f"capped to min_quantity ({min_qty})"
    elif quantity > max_qty:
        quantity = max_qty
        capped = True
        reason = f"capped to max_quantity ({max_qty})"

    actual_risk = quantity * risk_pts * tick_value
    actual_risk_pct = (actual_risk / account_balance) * 100.0 if account_balance > 0 else 0.0

    return SizingResult(
        quantity=round(quantity, 2),
        risk_amount=round(actual_risk, 4),
        risk_pct=round(actual_risk_pct, 4),
        model_used=model,
        capped=capped,
        reason=reason,
    )
