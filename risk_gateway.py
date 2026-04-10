# risk_gateway.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any

from broker_base import OrderRequest, OrderSide, BrokerBase


@dataclass
class RiskCheckResult:
    approved: bool
    reason: str = ""
    projected_position: int = 0
    order_value: float = 0.0


class RiskGateway:
    def __init__(
        self,
        broker: BrokerBase,
        max_single_order_value: float = 1_000_000,
        max_symbol_abs_position: int = 5000,
        allow_short: bool = False,
        block_duplicate_signal_id: bool = True,
        min_cash_buffer: float = 50_000,
    ):
        self.broker = broker
        self.max_single_order_value = float(max_single_order_value)
        self.max_symbol_abs_position = int(max_symbol_abs_position)
        self.allow_short = bool(allow_short)
        self.block_duplicate_signal_id = bool(block_duplicate_signal_id)
        self.min_cash_buffer = float(min_cash_buffer)
        self.used_signal_ids = set()

    def _current_positions(self) -> Dict[str, int]:
        try:
            return {str(k): int(v) for k, v in (self.broker.get_positions() or {}).items()}
        except Exception:
            return {}

    def _current_cash(self) -> float:
        try:
            return float(self.broker.get_cash())
        except Exception:
            return 0.0

    def validate(self, order: OrderRequest, ref_price: float) -> RiskCheckResult:
        return self.validate_dict({
            'symbol': order.symbol,
            'quantity': order.quantity,
            'side': order.side.value,
            'signal_id': order.signal_id,
        }, ref_price, cash=self._current_cash(), current_positions=self._current_positions())

    def validate_dict(self, order: Dict[str, Any], ref_price: float, cash: float | None = None, current_positions: Dict[str, int] | None = None) -> RiskCheckResult:
        symbol = str(order.get('symbol') or order.get('Ticker SYMBOL') or order.get('Ticker') or '').strip()
        qty = int(float(order.get('quantity') or order.get('Target_Qty') or 0))
        side_raw = str(order.get('side') or order.get('Action') or 'BUY').strip().upper()
        signal_id = str(order.get('signal_id') or order.get('Signal_ID') or order.get('Client_Order_ID') or '').strip()
        if ref_price <= 0:
            return RiskCheckResult(False, '參考價格無效')
        if qty <= 0:
            return RiskCheckResult(False, '數量必須 > 0')
        if self.block_duplicate_signal_id and signal_id and signal_id in self.used_signal_ids:
            return RiskCheckResult(False, f'重複 signal_id：{signal_id}')
        order_value = qty * ref_price
        if order_value > self.max_single_order_value:
            return RiskCheckResult(False, f'單筆委託金額超限：{order_value:,.0f}', order_value=order_value)

        current_positions = current_positions or self._current_positions()
        cash = float(self._current_cash() if cash is None else cash)
        current_pos = int(current_positions.get(symbol, 0))
        projected = current_pos
        if side_raw == OrderSide.BUY.value:
            projected = current_pos + qty
        elif side_raw in {OrderSide.SELL.value, OrderSide.SHORT.value}:
            projected = current_pos - qty
        elif side_raw == OrderSide.COVER.value:
            projected = current_pos + qty
        if abs(projected) > self.max_symbol_abs_position:
            return RiskCheckResult(False, f'單檔絕對持倉超限：目前 {current_pos}，送單後 {projected}，上限 {self.max_symbol_abs_position}', projected_position=projected, order_value=order_value)
        if side_raw == OrderSide.SHORT.value and not self.allow_short:
            return RiskCheckResult(False, '系統未開啟放空權限', projected_position=projected, order_value=order_value)
        if side_raw in {OrderSide.BUY.value, OrderSide.COVER.value} and cash - order_value < self.min_cash_buffer:
            return RiskCheckResult(False, '下單後現金緩衝不足', projected_position=projected, order_value=order_value)
        return RiskCheckResult(True, '', projected_position=projected, order_value=order_value)

    def register_signal(self, signal_id: str) -> None:
        if signal_id:
            self.used_signal_ids.add(signal_id)

    def build_runtime_summary(self) -> dict[str, Any]:
        return {
            'max_single_order_value': self.max_single_order_value,
            'max_symbol_abs_position': self.max_symbol_abs_position,
            'allow_short': self.allow_short,
            'min_cash_buffer': self.min_cash_buffer,
            'used_signal_ids': len(self.used_signal_ids),
            'status': 'level3_risk_gateway_ready',
        }
