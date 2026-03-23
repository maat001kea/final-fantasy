"""Spread-based fee model for backtest trade cost estimation."""

from __future__ import annotations

import json
from ast import literal_eval
from collections.abc import Iterable
from datetime import time
from typing import Any


class FeeModel:
    """Simple fixed-spread fee model (points) using DK-local trading windows."""

    SPREAD_SCHEDULE: dict[str, dict[str, float]] = {
        "DAX": {
            "08:00-16:30": 1.0,
            "16:30-21:00": 2.0,
            "21:00-23:02": 6.0,
            "default": 2.0,
        },
        "DOW": {
            "14:30-21:00": 1.9,
            "21:00-23:01": 4.0,
            "default": 2.5,
        },
        "NASDAQ 100": {
            "14:30-21:00": 0.9,
            "21:00-23:01": 1.8,
            "default": 1.6,
        },
        "FTSE": {
            "08:00-16:30": 0.8,
            "16:30-21:00": 1.0,
            "21:00-23:02": 5.0,
            "default": 1.0,
        },
    }
    DEFAULT_SPREAD = 2.0
    CONTRACT_SPECS: dict[str, dict[str, float]] = {
        "MYM": {"tick": 1.0, "point_value": 0.5, "fee_per_side_usd": 0.52},
        "MES": {"tick": 0.25, "point_value": 5.0},
        "MNQ": {"tick": 0.25, "point_value": 2.0},
        "M2K": {"tick": 0.1, "point_value": 5.0},
    }
    INSTRUMENT_TO_CONTRACT: dict[str, str] = {
        "DOW": "MYM",
        "S&P 500": "MES",
        "NASDAQ 100": "MNQ",
        "RUSSELL 2000": "M2K",
    }

    @classmethod
    def _parse_hhmm(cls, token: str) -> time:
        hour_str, minute_str = token.strip().split(":", 1)
        return time(int(hour_str), int(minute_str))

    @classmethod
    def _in_range(cls, trade_time: time, start: time, end: time) -> bool:
        if start <= end:
            return start <= trade_time <= end
        return trade_time >= start or trade_time <= end

    @classmethod
    def _normalize_instrument(cls, instrument: str) -> str:
        return str(instrument or "").strip().upper()

    @classmethod
    def _resolve_schedule(cls, instrument: str) -> dict[str, float] | None:
        normalized = cls._normalize_instrument(instrument)
        for key, schedule in cls.SPREAD_SCHEDULE.items():
            if cls._normalize_instrument(key) == normalized:
                return schedule
        return None

    @classmethod
    def get_spread(cls, instrument: str, trade_time: time | None) -> float:
        """Return spread cost in points for instrument/time; fallback to defaults."""
        schedule = cls._resolve_schedule(instrument)
        if schedule is None:
            return float(cls.DEFAULT_SPREAD)

        if trade_time is None:
            return float(schedule.get("default", cls.DEFAULT_SPREAD))

        for window, spread in schedule.items():
            if window == "default":
                continue
            if "-" not in window:
                continue
            start_token, end_token = window.split("-", 1)
            try:
                start = cls._parse_hhmm(start_token)
                end = cls._parse_hhmm(end_token)
            except Exception:
                continue
            if cls._in_range(trade_time, start, end):
                return float(spread)

        return float(schedule.get("default", cls.DEFAULT_SPREAD))

    @classmethod
    def contract_for_instrument(cls, instrument: str) -> str | None:
        token = str(instrument or "").strip().upper()
        for key, symbol in cls.INSTRUMENT_TO_CONTRACT.items():
            if str(key).strip().upper() == token:
                return str(symbol).strip().upper()
        return None

    @classmethod
    def point_value_for_contract(cls, contract_symbol: str | None) -> float | None:
        token = str(contract_symbol or "").strip().upper()
        if not token:
            return None
        spec = cls.CONTRACT_SPECS.get(token)
        if not spec:
            return None
        point_value = float(spec.get("point_value", 0.0))
        if point_value <= 0:
            return None
        return point_value

    @classmethod
    def fee_per_side_usd_for_contract(cls, contract_symbol: str | None) -> float | None:
        token = str(contract_symbol or "").strip().upper()
        if not token:
            return None
        spec = cls.CONTRACT_SPECS.get(token)
        if not spec:
            return None
        fee_side = cls._to_float(spec.get("fee_per_side_usd"))
        if fee_side is None or fee_side <= 0:
            return None
        return fee_side

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            out = float(value)
        except (TypeError, ValueError):
            return None
        if out != out:  # NaN check
            return None
        return out

    @classmethod
    def _to_float_list(cls, value: Any) -> list[float]:
        if value is None:
            return []
        if isinstance(value, str):
            token = value.strip()
            if not token:
                return []
            parsed: Any
            try:
                parsed = json.loads(token)
            except Exception:
                try:
                    parsed = literal_eval(token)
                except Exception:
                    parsed = token
            if isinstance(parsed, str):
                maybe_number = cls._to_float(parsed)
                return [maybe_number] if maybe_number is not None else []
            value = parsed

        if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
            out: list[float] = []
            for item in value:
                num = cls._to_float(item)
                if num is not None:
                    out.append(num)
            return out

        num = cls._to_float(value)
        return [num] if num is not None else []

    @classmethod
    def fees_pts_per_contract(cls, fees_usd_per_contract: float, contract_symbol: str | None) -> float:
        point_value = cls.point_value_for_contract(contract_symbol)
        usd = cls._to_float(fees_usd_per_contract)
        if point_value is None or usd is None or point_value <= 0:
            return 0.0
        return float(usd / point_value)

    @classmethod
    def fees_pts_trade(
        cls,
        *,
        fees_usd_fills: Any,
        contract_symbol: str | None,
        contracts_fills: Any = None,
    ) -> float:
        """
        Convert fill-level USD fees to points.

        Rule: fees_pts_trade = sum(fees_usd_fill / (point_value * contracts_fill))
        """
        point_value = cls.point_value_for_contract(contract_symbol)
        if point_value is None or point_value <= 0:
            return 0.0

        fee_vals = cls._to_float_list(fees_usd_fills)
        if not fee_vals:
            return 0.0

        contract_vals = cls._to_float_list(contracts_fills)
        if not contract_vals:
            contract_vals = [1.0]
        if len(contract_vals) == 1 and len(fee_vals) > 1:
            contract_vals = contract_vals * len(fee_vals)

        total_pts = 0.0
        for fee_usd, contracts in zip(fee_vals, contract_vals):
            contracts_float = cls._to_float(contracts)
            if contracts_float is None or contracts_float <= 0:
                continue
            total_pts += float(fee_usd) / (float(point_value) * float(contracts_float))
        return float(total_pts)

    @classmethod
    def fees_pts_from_contract_sides(cls, contract_symbol: str | None, contract_sides: float) -> float:
        """
        fees_pts = (fee_per_side_usd * contract_sides) / point_value
        Example (MYM): point_value=0.50, fee_per_side_usd=0.52.
        """
        point_value = cls.point_value_for_contract(contract_symbol)
        fee_per_side_usd = cls.fee_per_side_usd_for_contract(contract_symbol)
        sides = cls._to_float(contract_sides)
        if point_value is None or point_value <= 0:
            return 0.0
        if fee_per_side_usd is None or fee_per_side_usd <= 0:
            return 0.0
        if sides is None or sides <= 0:
            return 0.0
        fees_usd = float(fee_per_side_usd) * float(sides)
        return float(fees_usd / float(point_value))
