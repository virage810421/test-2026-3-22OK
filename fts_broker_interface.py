# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Any


class BrokerBase(ABC):
    @abstractmethod
    def update_market_price(self, ticker: str, price: float): ...

    @abstractmethod
    def place_order(self, order): ...

    @abstractmethod
    def cancel_order(self, order): ...

    @abstractmethod
    def get_positions(self): ...

    @abstractmethod
    def get_account_snapshot(self): ...

    @abstractmethod
    def restore_state(self, cash, positions, last_prices=None): ...

    # ---- Optional broker contract methods for pre-live / real broker alignment ----
    def connect(self) -> dict[str, Any]:
        raise NotImplementedError('broker_connect_not_implemented')

    def refresh_auth(self) -> dict[str, Any]:
        return {'ok': False, 'status': 'refresh_auth_not_implemented'}

    def disconnect(self) -> dict[str, Any]:
        return {'ok': False, 'status': 'disconnect_not_implemented'}

    def get_order_status(self, broker_order_id: str) -> dict[str, Any]:
        return {'ok': False, 'status': 'get_order_status_not_implemented', 'broker_order_id': broker_order_id}

    def replace_order(self, broker_order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {'ok': False, 'status': 'replace_order_not_implemented', 'broker_order_id': broker_order_id}

    def get_fills(self, trading_date: str | None = None) -> list[dict[str, Any]]:
        return []

    def query_open_orders(self) -> list[dict[str, Any]]:
        return []

    def query_positions(self) -> list[dict[str, Any]]:
        return []

    def query_cash(self) -> dict[str, Any]:
        return {}

    def poll_callbacks(self, clear: bool = False) -> list[dict[str, Any]]:
        return []

    def reconcile(self) -> dict[str, Any]:
        return {'ok': False, 'status': 'reconcile_not_implemented'}

    def capability_report(self) -> dict[str, Any]:
        return {
            'connect': False,
            'refresh_auth': False,
            'place_order': True,
            'cancel_order': True,
            'replace_order': False,
            'get_order_status': False,
            'query_open_orders': False,
            'query_positions': False,
            'query_cash': False,
            'get_fills': False,
            'poll_callbacks': False,
            'reconcile': False,
        }
