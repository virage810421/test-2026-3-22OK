# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class RealBrokerAdapterBlueprint(ABC):
    """Broker-ready contract.

    The abstract methods are the minimum true broker actions. The optional helper
    methods below are intentionally *not* abstract so old adapters do not become
    instantly unusable. A real-live closed loop can use them when the adapter
    supports richer broker evidence.
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

    def get_open_orders(self) -> list[dict[str, Any]]:
        raise NotImplementedError('adapter does not implement get_open_orders()')

    def poll_callbacks(self, cursor: str | None = None):
        raise NotImplementedError('adapter does not implement poll_callbacks()')

    def capability_report(self) -> dict[str, Any]:
        return {
            'broker_kind': self.__class__.__name__,
            'supports_open_orders': callable(getattr(self, 'get_open_orders', None)),
            'supports_callbacks': callable(getattr(self, 'poll_callbacks', None)),
            'true_broker_ready': False,
            'status': 'capability_report_default_unverified',
        }

    def export_broker_snapshot(self) -> dict[str, Any]:
        return {
            'status': 'broker_snapshot_export_not_implemented',
            'orders': self.get_open_orders() if callable(getattr(self, 'get_open_orders', None)) else [],
            'fills': self.get_fills() if callable(getattr(self, 'get_fills', None)) else [],
            'positions': self.get_positions() if callable(getattr(self, 'get_positions', None)) else [],
            'cash': self.get_cash() if callable(getattr(self, 'get_cash', None)) else {},
        }


def required_real_broker_fields() -> dict[str, list[str]]:
    return {
        'auth': ['api_key', 'api_secret', 'account_id', 'cert_or_token'],
        'order_payload': ['ticker', 'side', 'qty', 'price', 'order_type', 'time_in_force', 'session', 'idempotency_key'],
        'callback_payload': ['broker_order_id', 'order_status', 'fill_qty', 'fill_price', 'event_time', 'reject_reason'],
        'account_snapshot': ['cash_available', 'market_value', 'equity', 'buying_power'],
        'order_snapshot': ['broker_order_id', 'client_order_id', 'status', 'symbol', 'qty', 'filled_qty'],
    }
