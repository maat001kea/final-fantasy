"""Abstract broker adapter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    ERROR = "ERROR"


@dataclass(frozen=True)
class OrderRequest:
    """Immutable order request."""

    instrument: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    entry_price: float | None
    stop_price: float | None
    idempotency_key: str
    strategy_name: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class OrderResult:
    """Immutable order result returned by broker."""

    order_id: str
    status: OrderStatus
    instrument: str
    side: OrderSide
    quantity: float
    fill_price: float | None
    error_message: str | None
    raw_response: dict[str, Any]


class BrokerAdapter(ABC):
    """Abstract base class for broker adapters."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to broker."""

    @abstractmethod
    async def place_order(self, request: OrderRequest) -> OrderResult:
        """Place an order with the broker."""

    @abstractmethod
    async def get_position(self, instrument: str) -> dict[str, Any] | None:
        """Get current open position for an instrument."""

    @abstractmethod
    async def get_account_balance(self) -> float:
        """Return current account equity/balance."""

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order. Returns True if successful."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to broker."""
