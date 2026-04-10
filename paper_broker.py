from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any

from broker_base import BrokerBase, FillEvent, OrderRecord, OrderRequest, OrderSide, OrderStatus, OrderType


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class PaperBroker(BrokerBase):
    def __init__(self, initial_cash: float = 5_000_000, commission_rate: float = 0.001425, tax_rate_sell: float = 0.003, tax_rate_short: float = 0.003, default_slippage_bps: float = 5.0, partial_fill_threshold_value: float = 1_500_000, partial_fill_ratio: float = 0.5, allow_short: bool = True):
        self.cash = float(initial_cash)
        self.commission_rate = float(commission_rate)
        self.tax_rate_sell = float(tax_rate_sell)
        self.tax_rate_short = float(tax_rate_short)
        self.default_slippage_bps = float(default_slippage_bps)
        self.partial_fill_threshold_value = float(partial_fill_threshold_value)
        self.partial_fill_ratio = float(partial_fill_ratio)
        self.allow_short = bool(allow_short)
        self.positions: Dict[str, int] = {}
        self.last_prices: Dict[str, float] = {}
        self.open_orders: Dict[str, OrderRecord] = {}
        self.pending_fills: List[FillEvent] = []
        self.fill_history: List[FillEvent] = []
        self.supports_short = self.allow_short

    def update_market_prices(self, price_map: Dict[str, float]) -> None:
        for symbol, price in price_map.items():
            try:
                px = float(price)
            except Exception:
                continue
            if px > 0:
                self.last_prices[str(symbol)] = px

    def _commission(self, gross: float) -> float:
        return round(gross * self.commission_rate, 2)

    def _tax(self, side: OrderSide, gross: float) -> float:
        if side == OrderSide.SELL:
            return round(gross * self.tax_rate_sell, 2)
        if side == OrderSide.SHORT:
            return round(gross * self.tax_rate_short, 2)
        return 0.0

    def _dynamic_bps(self, side: OrderSide, quantity: int, ref_price: float) -> float:
        bps = self.default_slippage_bps
        order_value = max(ref_price, 0) * max(quantity, 0)
        if ref_price < 50:
            bps *= 1.8
        elif ref_price < 100:
            bps *= 1.3
        if order_value > self.partial_fill_threshold_value:
            bps *= 1.5
        if side in (OrderSide.SHORT, OrderSide.COVER):
            bps *= 1.15
        return bps

    def _apply_slippage(self, side: OrderSide, quantity: int, ref_price: float) -> float:
        bps = self._dynamic_bps(side, quantity, ref_price) / 10000.0
        if side in (OrderSide.BUY, OrderSide.COVER):
            return round(ref_price * (1.0 + bps), 4)
        return round(ref_price * (1.0 - bps), 4)

    def _resolve_reference_price(self, order: OrderRequest) -> float:
        market_px = float(self.last_prices.get(order.symbol, 0.0) or 0.0)
        if order.order_type == OrderType.LIMIT and order.limit_price:
            limit = float(order.limit_price)
            if market_px <= 0:
                return limit
            if order.side in (OrderSide.BUY, OrderSide.COVER):
                return min(limit, market_px)
            return max(limit, market_px)
        return market_px if market_px > 0 else float(order.limit_price or 0.0)

    def _make_order_record(self, order: OrderRequest) -> OrderRecord:
        return OrderRecord(order_id=str(uuid.uuid4()), symbol=order.symbol, side=order.side, quantity=int(order.quantity), remaining_qty=int(order.quantity), order_type=order.order_type, limit_price=order.limit_price, status=OrderStatus.SUBMITTED, strategy_name=order.strategy_name, signal_id=order.signal_id, client_order_id=order.client_order_id, note=order.note, create_time=_now(), update_time=_now())

    def _append_fill(self, order: OrderRecord, qty: int, px: float, commission: float, tax: float) -> None:
        self.pending_fills.append(FillEvent(order_id=order.order_id, symbol=order.symbol, side=order.side, fill_qty=int(qty), fill_price=float(px), fill_time=_now(), commission=commission, tax=tax, slippage=round(abs(px - (order.limit_price or self.last_prices.get(order.symbol, px))), 4), strategy_name=order.strategy_name, signal_id=order.signal_id, note=order.note))

    def place_order(self, order: OrderRequest) -> OrderRecord:
        record = self._make_order_record(order)
        self.open_orders[record.order_id] = record
        if order.side in (OrderSide.SHORT, OrderSide.COVER) and not self.allow_short:
            record.status = OrderStatus.REJECTED
            record.reject_reason = 'short_not_allowed'
            record.update_time = _now()
            return record
        ref_price = self._resolve_reference_price(order)
        if ref_price <= 0:
            record.status = OrderStatus.REJECTED
            record.reject_reason = 'invalid_reference_price'
            record.update_time = _now()
            return record
        quantity = int(order.quantity)
        fill_qty = quantity
        if ref_price * quantity >= self.partial_fill_threshold_value:
            fill_qty = max(1, int(quantity * self.partial_fill_ratio))
            record.status = OrderStatus.PARTIALLY_FILLED
        else:
            record.status = OrderStatus.FILLED
        fill_price = self._apply_slippage(order.side, fill_qty, ref_price)
        gross = fill_price * fill_qty
        commission = self._commission(gross)
        tax = self._tax(order.side, gross)
        pos = int(self.positions.get(order.symbol, 0))
        if order.side in (OrderSide.BUY, OrderSide.COVER):
            total_cost = gross + commission + tax
            if self.cash < total_cost:
                record.status = OrderStatus.REJECTED
                record.reject_reason = 'insufficient_cash'
                record.update_time = _now()
                return record
            if order.side == OrderSide.COVER and pos >= 0:
                record.status = OrderStatus.REJECTED
                record.reject_reason = 'no_short_position'
                record.update_time = _now()
                return record
            self.cash -= total_cost
            self.positions[order.symbol] = pos + fill_qty
        else:
            if order.side == OrderSide.SELL and pos < fill_qty:
                record.status = OrderStatus.REJECTED
                record.reject_reason = 'insufficient_position'
                record.update_time = _now()
                return record
            self.cash += gross - commission - tax
            self.positions[order.symbol] = pos - fill_qty
        if self.positions.get(order.symbol, 0) == 0:
            self.positions.pop(order.symbol, None)
        record.filled_qty = fill_qty
        record.remaining_qty = max(0, quantity - fill_qty)
        record.avg_fill_price = fill_price
        record.update_time = _now()
        self._append_fill(record, fill_qty, fill_price, commission, tax)
        return record

    def cancel_order(self, order_id: str) -> bool:
        rec = self.open_orders.get(order_id)
        if not rec or rec.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
            return False
        rec.status = OrderStatus.CANCELLED
        rec.update_time = _now()
        return True

    def get_order_status(self, order_id: str) -> Optional[OrderRecord]:
        return self.open_orders.get(order_id)

    def get_open_orders(self) -> List[OrderRecord]:
        return [o for o in self.open_orders.values() if o.status in (OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED)]

    def get_positions(self) -> Dict[str, int]:
        return dict(self.positions)

    def get_cash(self) -> float:
        return round(self.cash, 2)

    def poll_fills(self) -> List[FillEvent]:
        fills = list(self.pending_fills)
        self.pending_fills.clear()
        return fills
