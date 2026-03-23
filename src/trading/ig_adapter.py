"""IG Markets broker adapter wrapping the existing src/live_ig.py."""

from __future__ import annotations

from typing import Any

from ..live_ig import IGCredentials, IGApiError, create_session, _base_url, _request_json
from .broker_adapter_base import BrokerAdapter, OrderRequest, OrderResult, OrderSide, OrderStatus


class IGBrokerAdapter(BrokerAdapter):
    """IG Markets adapter using the REST API v2."""

    def __init__(self, credentials: IGCredentials) -> None:
        self.credentials = credentials
        self._session_tokens: dict[str, str] = {}

    async def connect(self) -> None:
        """Create IG session and store tokens."""
        self._session_tokens = create_session(self.credentials)

    async def place_order(self, request: OrderRequest) -> OrderResult:
        """Place a deal via IG REST API v2."""
        if not self._session_tokens:
            return OrderResult(
                order_id="",
                status=OrderStatus.ERROR,
                instrument=request.instrument,
                side=request.side,
                quantity=request.quantity,
                fill_price=None,
                error_message="Not connected – call connect() first",
                raw_response={},
            )

        base_url = _base_url(self.credentials.environment)
        url = f"{base_url}/positions/otc"
        direction = "BUY" if request.side == OrderSide.BUY else "SELL"
        body: dict[str, Any] = {
            "epic": request.instrument,
            "direction": direction,
            "size": request.quantity,
            "orderType": request.order_type.value,
            "guaranteedStop": False,
            "forceOpen": True,
            "currencyCode": "GBP",
        }
        if request.stop_price is not None:
            body["stopLevel"] = request.stop_price
        if request.entry_price is not None and request.order_type.value != "MARKET":
            body["level"] = request.entry_price

        headers = {
            "X-IG-API-KEY": self.credentials.api_key,
            "CST": self._session_tokens.get("cst", ""),
            "X-SECURITY-TOKEN": self._session_tokens.get("x_security_token", ""),
            "Version": "2",
            "Content-Type": "application/json",
            "Accept": "application/json; charset=UTF-8",
        }

        try:
            payload, _ = _request_json("POST", url, headers=headers, json_body=body)
            deal_ref = payload.get("dealReference", "")
            return OrderResult(
                order_id=str(deal_ref),
                status=OrderStatus.PENDING,
                instrument=request.instrument,
                side=request.side,
                quantity=request.quantity,
                fill_price=None,
                error_message=None,
                raw_response=payload,
            )
        except IGApiError as exc:
            return OrderResult(
                order_id="",
                status=OrderStatus.REJECTED,
                instrument=request.instrument,
                side=request.side,
                quantity=request.quantity,
                fill_price=None,
                error_message=str(exc),
                raw_response={},
            )

    async def get_position(self, instrument: str) -> dict[str, Any] | None:
        """Fetch open position for an epic."""
        if not self._session_tokens:
            return None
        base_url = _base_url(self.credentials.environment)
        url = f"{base_url}/positions/otc"
        headers = {
            "X-IG-API-KEY": self.credentials.api_key,
            "CST": self._session_tokens.get("cst", ""),
            "X-SECURITY-TOKEN": self._session_tokens.get("x_security_token", ""),
            "Version": "2",
            "Accept": "application/json; charset=UTF-8",
        }
        try:
            payload, _ = _request_json("GET", url, headers=headers)
            positions = payload.get("positions", [])
            for pos in positions:
                mkt = pos.get("market", {})
                if mkt.get("epic") == instrument:
                    return pos
        except IGApiError:
            pass
        return None

    async def get_account_balance(self) -> float:
        """Return current account equity."""
        if not self._session_tokens:
            return 0.0
        base_url = _base_url(self.credentials.environment)
        url = f"{base_url}/accounts"
        headers = {
            "X-IG-API-KEY": self.credentials.api_key,
            "CST": self._session_tokens.get("cst", ""),
            "X-SECURITY-TOKEN": self._session_tokens.get("x_security_token", ""),
            "Version": "1",
            "Accept": "application/json; charset=UTF-8",
        }
        try:
            payload, _ = _request_json("GET", url, headers=headers)
            accounts = payload.get("accounts", [])
            if accounts:
                balance_obj = accounts[0].get("balance", {})
                return float(balance_obj.get("available", 0.0))
        except IGApiError:
            pass
        return 0.0

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a working order."""
        if not self._session_tokens:
            return False
        base_url = _base_url(self.credentials.environment)
        url = f"{base_url}/workingorders/otc/{order_id}"
        headers = {
            "X-IG-API-KEY": self.credentials.api_key,
            "CST": self._session_tokens.get("cst", ""),
            "X-SECURITY-TOKEN": self._session_tokens.get("x_security_token", ""),
            "_method": "DELETE",
            "Version": "2",
            "Accept": "application/json; charset=UTF-8",
        }
        try:
            _request_json("POST", url, headers=headers)
            return True
        except IGApiError:
            return False

    async def disconnect(self) -> None:
        """Clear session tokens."""
        self._session_tokens = {}
