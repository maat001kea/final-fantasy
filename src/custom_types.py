"""Types and defaults for Custom Strategy Lab."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


SCHOOL_RUN_DEFAULT_TIMEFRAME = "15m"


@dataclass(frozen=True)
class CustomStrategyConfig:
    """Minimal School Run config for Custom overlay."""

    instrument: str = "DOW"
    timeframe: str = SCHOOL_RUN_DEFAULT_TIMEFRAME
    lookback_sessions: int = 1000
    execution_model: str = "Aggressiv"  # Systematisk | Dynamisk | Aggressiv
    bar1_start: str = "14:00"

    # Keep original School Run defaults locked.
    direction_mode: str = "dual_breakout"
    entry_offset_pts: float = 1.0
    max_trigger_bars: int = 8
    contract_symbol: str = "MYM"
    contract_quantity: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CustomScanConfig:
    """Runtime scan settings for School Run time-of-day scan."""

    start_time_from: str = "08:00"
    start_time_to: str = "11:00"
    start_time_step_minutes: int = 15
    top_k: int = 12
    max_evaluations: int = 800
    max_seconds: float = 45.0
    min_triggered_trades: int = 30


def _normalize_execution_model(value: Any) -> str:
    token = str(value or "").strip().lower()
    if token in {"systematisk", "simplified"}:
        return "Systematisk"
    if token in {"dynamisk", "tom_live", "live"}:
        return "Dynamisk"
    if token in {"aggressiv", "tom_aggressive", "aggressive"}:
        return "Aggressiv"
    return "Aggressiv"


def coerce_custom_strategy_config(raw: dict[str, Any] | CustomStrategyConfig | None) -> CustomStrategyConfig:
    """Best-effort coercion from dict/session payload to dataclass."""
    if isinstance(raw, CustomStrategyConfig):
        return raw
    data = raw if isinstance(raw, dict) else {}
    defaults = CustomStrategyConfig()
    out: dict[str, Any] = defaults.to_dict()
    for key in out:
        if key in data:
            out[key] = data[key]

    out["timeframe"] = SCHOOL_RUN_DEFAULT_TIMEFRAME
    out["execution_model"] = _normalize_execution_model(out.get("execution_model"))
    out["lookback_sessions"] = 1000
    out["direction_mode"] = "dual_breakout"
    out["entry_offset_pts"] = 1.0
    out["max_trigger_bars"] = 8
    out["contract_symbol"] = str(out.get("contract_symbol", "") or "")
    try:
        qty = float(out.get("contract_quantity", 1.0))
    except (TypeError, ValueError):
        qty = 1.0
    out["contract_quantity"] = max(1.0, qty)
    return CustomStrategyConfig(**out)


def coerce_custom_scan_config(raw: dict[str, Any] | CustomScanConfig | None) -> CustomScanConfig:
    """Best-effort coercion from dict/session payload to dataclass."""
    if isinstance(raw, CustomScanConfig):
        return raw
    data = raw if isinstance(raw, dict) else {}
    defaults = CustomScanConfig()
    out: dict[str, Any] = {
        "start_time_from": str(data.get("start_time_from", defaults.start_time_from)),
        "start_time_to": str(data.get("start_time_to", defaults.start_time_to)),
        "start_time_step_minutes": int(data.get("start_time_step_minutes", defaults.start_time_step_minutes)),
        "top_k": int(data.get("top_k", defaults.top_k)),
        "max_evaluations": int(data.get("max_evaluations", defaults.max_evaluations)),
        "max_seconds": float(data.get("max_seconds", defaults.max_seconds)),
        "min_triggered_trades": int(data.get("min_triggered_trades", defaults.min_triggered_trades)),
    }
    out["start_time_step_minutes"] = max(5, min(60, int(out["start_time_step_minutes"])))
    out["top_k"] = max(1, min(50, int(out["top_k"])))
    out["max_evaluations"] = max(20, min(5000, int(out["max_evaluations"])))
    out["max_seconds"] = max(5.0, min(600.0, float(out["max_seconds"])))
    out["min_triggered_trades"] = max(1, min(1000, int(out["min_triggered_trades"])))
    return CustomScanConfig(**out)
