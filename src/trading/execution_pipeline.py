"""Main orchestrator – connects Strategy Loader → Intent Generator → Risk Gate → Broker → Audit DB."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .audit_db import AuditDB
from .broker_adapter_base import BrokerAdapter, OrderRequest, OrderSide, OrderType
from .position_sizer import load_sizing_config
from .risk_gate import RiskConfig, RiskGate
from .strategy_loader import StrategyDSL, load_strategies
from .trade_intent import create_trade_intent


@dataclass
class PipelineConfig:
    """Feature flags and settings for the execution pipeline."""

    live_execution: bool = False
    shadow_mode: bool = True
    strategies_dir: str | None = None
    account_config: dict[str, Any] = field(default_factory=dict)
    sizing_config: dict[str, Any] = field(default_factory=dict)


class ExecutionPipeline:
    """Main orchestrator for the trading engine."""

    def __init__(
        self,
        broker: BrokerAdapter | None = None,
        audit_db: AuditDB | None = None,
        config: PipelineConfig | None = None,
        account_balance: float = 1000.0,
    ) -> None:
        self.broker = broker
        self.audit_db = audit_db or AuditDB()
        self.config = config or PipelineConfig()
        self.account_balance = account_balance

        risk_config = RiskConfig.from_dict(self.config.account_config)
        self.risk_gate = RiskGate(config=risk_config, account_balance=account_balance)
        self.sizing_config = self.config.sizing_config or load_sizing_config()
        self._strategies: list[StrategyDSL] = []

    def load_strategies(self) -> None:
        """Load strategies from YAML files."""
        self._strategies = load_strategies(self.config.strategies_dir)

    def run_once(self, bar_data: dict[str, Any], now: datetime | None = None) -> list[dict[str, Any]]:
        """Single-pass evaluation: generate intents, run gate, optionally execute.

        Returns a list of processed intent results.
        """
        if now is None:
            now = datetime.now(tz=timezone.utc)

        results: list[dict[str, Any]] = []
        for strategy in self._strategies:
            for direction in strategy.directions:
                intent = create_trade_intent(
                    strategy=strategy,
                    bar_data=bar_data,
                    account_balance=self.account_balance,
                    sizing_config=self.sizing_config,
                    now=now,
                    direction=direction,
                )
                if intent is None:
                    continue
                result = self.process_intent(intent, now=now)
                results.append(result)
        return results

    def process_intent(
        self, intent: dict[str, Any], now: datetime | None = None
    ) -> dict[str, Any]:
        """Run gate check → broker execution → audit logging for one intent."""
        if now is None:
            now = datetime.now(tz=timezone.utc)

        # Log intent
        self.audit_db.log_intent(intent)

        context_snapshot = intent.get("context_snapshot", {})
        context_snapshot = dict(context_snapshot) if isinstance(context_snapshot, dict) else {}
        bar_index_raw = intent.get("entry_bar_index", context_snapshot.get("entry_bar_index"))
        bar_index = int(bar_index_raw) if bar_index_raw is not None else None

        # Run risk gate – pass order_type as suffix so Add orders get a distinct key
        decision = self.risk_gate.evaluate(
            strategy=intent.get("strategy_name", ""),
            instrument=intent.get("instrument", ""),
            direction=intent.get("direction", ""),
            quantity=float(intent.get("quantity", 0.0)),
            risk_pts=float(intent.get("risk_pts", 0.0)),
            tick_value=float(intent.get("tick_value", 1.0)),
            now=now,
            suffix=str(intent.get("order_type", "entry") or "entry"),
            bar_index=bar_index,
        )

        # Log gate decision
        self.audit_db.log_gate_decision(intent.get("idempotency_key", ""), decision)

        result: dict[str, Any] = {
            "intent": intent,
            "decision": decision,
            "order_result": None,
        }

        if not decision.approved:
            return result

        # Execute if live or shadow
        if self.broker is not None and (self.config.live_execution or self.config.shadow_mode):

            async def _place() -> None:
                direction = intent.get("direction", "long")
                side = OrderSide.BUY if direction == "long" else OrderSide.SELL
                metadata = intent.get("metadata", {})
                metadata = dict(metadata) if isinstance(metadata, dict) else {}
                order_req = OrderRequest(
                    instrument=intent.get("instrument", ""),
                    side=side,
                    order_type=OrderType.MARKET,
                    quantity=float(intent.get("quantity", 0.0)),
                    entry_price=intent.get("entry_price"),
                    stop_price=intent.get("stop_price"),
                    idempotency_key=intent.get("idempotency_key", ""),
                    strategy_name=intent.get("strategy_name", ""),
                    metadata=metadata,
                )
                order_result = await self.broker.place_order(order_req)
                broker_name = type(self.broker).__name__
                self.audit_db.log_broker_order(
                    intent.get("idempotency_key", ""), order_result, broker=broker_name
                )
                result["order_result"] = order_result

            try:
                asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(_place())
            else:
                error: list[BaseException] = []

                def _runner() -> None:
                    try:
                        asyncio.run(_place())
                    except BaseException as exc:  # pragma: no cover - defensive fallback
                        error.append(exc)

                thread = threading.Thread(target=_runner, daemon=True, name="execution-pipeline-broker")
                thread.start()
                thread.join()
                if error:
                    raise error[0]

        return result
