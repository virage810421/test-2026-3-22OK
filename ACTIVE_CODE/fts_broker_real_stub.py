# -*- coding: utf-8 -*-
from fts_broker_interface import BrokerBase
from fts_models import AccountSnapshot, OrderStatus
from fts_utils import now_str

class RealBrokerStub(BrokerBase):
    def __init__(self, credentials):
        self.credentials = credentials
        self._positions = {}; self._cash = 0.0; self._last_prices = {}

    def restore_state(self, cash, positions, last_prices=None):
        self._cash = float(cash); self._positions = positions or {}; self._last_prices = last_prices or {}

    def update_market_price(self, ticker: str, price: float): self._last_prices[ticker] = price
    def place_order(self, order): raise NotImplementedError("RealBrokerStub 尚未接入真券商 API。")
    def cancel_order(self, order): order.status = OrderStatus.CANCELLED; order.updated_at = now_str(); return order
    def get_positions(self): return self._positions
    def get_account_snapshot(self): return AccountSnapshot(self._cash, 0.0, self._cash, 0.0, now_str())
