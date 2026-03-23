"""Strategy DSL loader – parses YAML strategy files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_REQUIRED_FIELDS = ("meta", "signal", "filters", "risk")
_REQUIRED_META = ("name", "version", "evidence_level", "description")
_REQUIRED_SIGNAL_BASE = ("start_bar",)
_REQUIRED_SIGNAL_SINGLE = ("direction", "entry_source", "stop_source")
_REQUIRED_SIGNAL_DUAL = ("long_entry_source", "long_stop_source", "short_entry_source", "short_stop_source")


class StrategyDSL:
    """Parsed and validated strategy definition."""

    def __init__(self, data: dict[str, Any], source_path: Path | None = None) -> None:
        self._data = data
        self.source_path = source_path
        self._validate()

    def _validate(self) -> None:
        """Raise ValueError if required fields are missing."""
        for field in _REQUIRED_FIELDS:
            if field not in self._data:
                raise ValueError(self._with_source(f"Strategy YAML missing required section: '{field}'"))
        meta = self._data["meta"]
        for f in _REQUIRED_META:
            if f not in meta:
                raise ValueError(self._with_source(f"Strategy meta missing required field: '{f}'"))
        signal = self._data["signal"]
        for f in _REQUIRED_SIGNAL_BASE:
            if f not in signal:
                raise ValueError(self._with_source(f"Strategy signal missing required field: '{f}'"))

        mode = self._resolve_mode(signal)
        signal["mode"] = mode
        required_signal_fields = _REQUIRED_SIGNAL_DUAL if mode == "dual_breakout" else _REQUIRED_SIGNAL_SINGLE
        for f in required_signal_fields:
            if f not in signal:
                raise ValueError(
                    self._with_source(
                        f"Strategy signal missing required field: '{f}' (mode={mode})"
                    )
                )

    def _resolve_mode(self, signal: dict[str, Any]) -> str:
        raw_mode = str(signal.get("mode", "")).strip().lower()
        has_dual_fields = all(field in signal for field in _REQUIRED_SIGNAL_DUAL)
        if raw_mode == "dual_breakout":
            return "dual_breakout"
        if raw_mode in {"single_breakout", "single"}:
            if has_dual_fields and "direction" not in signal:
                return "dual_breakout"
            return "single_breakout"
        if has_dual_fields:
            return "dual_breakout"
        return "single_breakout"

    def _with_source(self, message: str) -> str:
        if self.source_path is None:
            return message
        return f"{message} | file={self.source_path}"

    @property
    def name(self) -> str:
        return str(self._data["meta"]["name"])

    @property
    def direction(self) -> str:
        signal = self._data["signal"]
        if self.is_dual_breakout:
            # Backward-compatible default for legacy callers; dual handlers should use directions().
            return "long"
        return str(signal["direction"])

    @property
    def mode(self) -> str:
        return self._resolve_mode(self._data.get("signal", {}))

    @property
    def is_dual_breakout(self) -> bool:
        return self.mode == "dual_breakout"

    @property
    def directions(self) -> tuple[str, ...]:
        return ("long", "short") if self.is_dual_breakout else (self.direction,)

    @property
    def entry_source(self) -> str:
        signal = self._data["signal"]
        if self.is_dual_breakout:
            return str(signal["long_entry_source"])
        return str(signal["entry_source"])

    @property
    def stop_source(self) -> str:
        signal = self._data["signal"]
        if self.is_dual_breakout:
            return str(signal["long_stop_source"])
        return str(signal["stop_source"])

    def entry_source_for(self, direction: str) -> str:
        signal = self._data["signal"]
        if self.is_dual_breakout:
            side = str(direction).strip().lower()
            if side == "short":
                return str(signal["short_entry_source"])
            return str(signal["long_entry_source"])
        return str(signal["entry_source"])

    def stop_source_for(self, direction: str) -> str:
        signal = self._data["signal"]
        if self.is_dual_breakout:
            side = str(direction).strip().lower()
            if side == "short":
                return str(signal["short_stop_source"])
            return str(signal["long_stop_source"])
        return str(signal["stop_source"])

    @property
    def start_bar(self) -> int:
        return int(self._data["signal"]["start_bar"])

    @property
    def instruments(self) -> list[str]:
        return list(self._data.get("filters", {}).get("instruments", []))

    @property
    def context_filters(self) -> dict[str, Any]:
        return dict(self._data.get("filters", {}).get("context_filters", {}))

    @property
    def max_risk_per_trade_pct(self) -> float:
        return float(self._data.get("risk", {}).get("max_risk_per_trade_pct", 1.0))

    def matches_context(self, context_dict: dict[str, Any]) -> bool:
        """Return True if all context_filters match the provided context dict."""
        for key, expected in self.context_filters.items():
            if expected == "any":
                continue
            actual = context_dict.get(key)
            if actual != expected:
                return False
        return True

    def as_dict(self) -> dict[str, Any]:
        """Return the raw data dictionary."""
        return dict(self._data)


def load_strategies(directory: str | Path | None = None) -> list[StrategyDSL]:
    """Load all YAML strategy files from the given directory.

    Defaults to the `strategies/` directory in the repository root.
    """
    if directory is None:
        directory = Path(__file__).parent.parent.parent / "strategies"
    directory = Path(directory)
    strategies: list[StrategyDSL] = []
    for yaml_path in sorted(directory.glob("*.yaml")):
        with yaml_path.open(encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ValueError(f"Strategy file {yaml_path} does not contain a YAML mapping.")
        strategies.append(StrategyDSL(data, source_path=yaml_path))
    return strategies
