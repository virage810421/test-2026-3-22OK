# risk_gateway.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from .broker_base import OrderRequest, OrderSide
from .paper_broker import PaperBroker


@dataclass
class RiskCheckResult:
    approved: bool
    reason: str = ""


class RiskGateway:
    def __init__(
        self,
        broker: PaperBroker,
        max_single_order_value: float = 1_000_000,
        max_symbol_abs_position: int = 5000,
        allow_short: bool = False,
        block_duplicate_signal_id: bool = True,
        min_cash_buffer: float = 50_000,
    ):
        self.broker = broker
        self.max_single_order_value = max_single_order_value
        self.max_symbol_abs_position = max_symbol_abs_position
        self.allow_short = allow_short
        self.block_duplicate_signal_id = block_duplicate_signal_id
        self.min_cash_buffer = min_cash_buffer

        self.used_signal_ids = set()

    def validate(self, order: OrderRequest, ref_price: float) -> RiskCheckResult:
        if ref_price <= 0:
            return RiskCheckResult(False, "參考價格無效")

        if order.quantity <= 0:
            return RiskCheckResult(False, "數量必須 > 0")

        if self.block_duplicate_signal_id and order.signal_id:
            if order.signal_id in self.used_signal_ids:
                return RiskCheckResult(False, f"重複 signal_id：{order.signal_id}")

        order_value = order.quantity * ref_price
        if order_value > self.max_single_order_value:
            return RiskCheckResult(False, f"單筆委託金額超限：{order_value:,.0f}")

        current_pos = self.broker.get_positions().get(order.symbol, 0)
        projected_pos = self._calc_projected_position(current_pos, order)

        if abs(projected_pos) > self.max_symbol_abs_position:
            return RiskCheckResult(
                False,
                f"單檔絕對持倉超限：目前 {current_pos}，送單後 {projected_pos}，上限 {self.max_symbol_abs_position}"
            )

        if order.side == OrderSide.SHORT and not self.allow_short:
            return RiskCheckResult(False, "系統未開啟放空權限")

        if order.side == OrderSide.BUY:
            est_cost = order_value * (1 + self.broker.commission_rate)
            if self.broker.get_cash() - est_cost < self.min_cash_buffer:
                return RiskCheckResult(False, "下單後現金緩衝不足")

        if order.side == OrderSide.COVER:
            est_cost = order_value * (1 + self.broker.commission_rate)
            if self.broker.get_cash() - est_cost < self.min_cash_buffer:
                return RiskCheckResult(False, "回補後現金緩衝不足")

        return RiskCheckResult(True, "")

    def register_signal(self, signal_id: str) -> None:
        if signal_id:
            self.used_signal_ids.add(signal_id)

    @staticmethod
    def _calc_projected_position(current_pos: int, order: OrderRequest) -> int:
        if order.side == OrderSide.BUY:
            return current_pos + order.quantity
        if order.side == OrderSide.SELL:
            return current_pos - order.quantity
        if order.side == OrderSide.SHORT:
            return current_pos - order.quantity
        if order.side == OrderSide.COVER:
            return current_pos + order.quantity
        return current_pos
