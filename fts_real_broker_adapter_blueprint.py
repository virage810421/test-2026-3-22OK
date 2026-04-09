# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class RealBrokerAdapterBlueprint(ABC):
    """Broker-ready contract.
    This is the part that still needs the actual broker API, credentials,
    callbacks and settlement details.
    """

    @abstractmethod
    def connect(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def refresh_auth(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def place_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def replace_order(self, broker_order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_order_status(self, broker_order_id: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_fills(self, trading_date: str | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_positions(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_cash(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def disconnect(self) -> dict[str, Any]:
        raise NotImplementedError


def required_real_broker_fields() -> dict[str, list[str]]:
    return {
        'auth': ['api_key', 'api_secret', 'account_id', 'cert_or_token'],
        'order_payload': ['ticker', 'side', 'qty', 'price', 'order_type', 'time_in_force', 'session', 'idempotency_key'],
        'callback_payload': ['broker_order_id', 'order_status', 'fill_qty', 'fill_price', 'event_time', 'reject_reason'],
        'account_snapshot': ['cash_available', 'market_value', 'equity', 'buying_power'],
    }
