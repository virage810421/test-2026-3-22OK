# paper_broker.py
from __future__ import annotations

import math
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from broker_base import (
    BrokerBase,
    FillEvent,
    OrderRecord,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
)


class PaperBroker(BrokerBase):
    def __init__(
        self,
        initial_cash: float = 5_000_000,
        commission_rate: float = 0.001425,
        tax_rate_sell: float = 0.003,
        default_slippage_bps: float = 5.0,
        partial_fill_threshold_value: float = 1_500_000,
        partial_fill_ratio: float = 0.5,
    ):
        self.cash = float(initial_cash)
        self.positions: Dict[str, int] = {}
        self.orders: Dict[str, OrderRecord] = {}
        self.pending_fills: List[FillEvent] = []
        self.last_prices: Dict[str, float] = {}

        self.commission_rate = commission_rate
        self.tax_rate_sell = tax_rate_sell
        self.default_slippage_bps = default_slippage_bps
        self.partial_fill_threshold_value = partial_fill_threshold_value
        self.partial_fill_ratio = partial_fill_ratio

    def update_market_price(self, symbol: str, price: float) -> None:
        if symbol and price and price > 0:
            self.last_prices[symbol] = float(price)

    def update_market_prices(self, price_map: Dict[str, float]) -> None:
        for symbol, price in price_map.items():
            self.update_market_price(symbol, price)

    def place_order(self, order: OrderRequest) -> OrderRecord:
        order_id = self._new_order_id()
        now = self._now()

        record = OrderRecord(
            order_id=order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=int(order.quantity),
            remaining_qty=int(order.quantity),
            order_type=order.order_type,
            limit_price=order.limit_price,
            status=OrderStatus.PENDING_SUBMIT,
            create_time=now,
            update_time=now,
            strategy_name=order.strategy_name,
            signal_id=order.signal_id,
            client_order_id=order.client_order_id,
            note=order.note,
        )
        self.orders[order_id] = record

        valid, reason = self._precheck(order)
        if not valid:
            record.status = OrderStatus.REJECTED
            record.reject_reason = reason
            record.update_time = self._now()
            return record

        record.status = OrderStatus.SUBMITTED
        record.update_time = self._now()

        self._simulate_execution(record)
        return record

    def cancel_order(self, order_id: str) -> bool:
        record = self.orders.get(order_id)
        if not record:
            return False

        if record.status in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}:
            return False

        record.status = OrderStatus.CANCELLED
        record.update_time = self._now()
        return True

    def get_order_status(self, order_id: str) -> Optional[OrderRecord]:
        return self.orders.get(order_id)

    def get_open_orders(self) -> List[OrderRecord]:
        open_status = {
            OrderStatus.NEW,
            OrderStatus.PENDING_SUBMIT,
            OrderStatus.SUBMITTED,
            OrderStatus.PARTIALLY_FILLED,
        }
        return [o for o in self.orders.values() if o.status in open_status]

    def get_positions(self) -> Dict[str, int]:
        return dict(self.positions)

    def get_cash(self) -> float:
        return float(self.cash)

    def poll_fills(self) -> List[FillEvent]:
        fills = self.pending_fills[:]
        self.pending_fills.clear()
        return fills

    def _simulate_execution(self, record: OrderRecord) -> None:
        ref_price = self._resolve_reference_price(record)
        if ref_price <= 0:
            record.status = OrderStatus.REJECTED
            record.reject_reason = "無法取得有效市價"
            record.update_time = self._now()
            return

        if record.order_type == OrderType.LIMIT and record.limit_price is not None:
            if not self._limit_crossed(record.side, ref_price, record.limit_price):
                record.status = OrderStatus.SUBMITTED
                record.update_time = self._now()
                return

        order_value = ref_price * record.quantity
        if order_value >= self.partial_fill_threshold_value and record.quantity >= 2:
            fill_qty = max(1, int(math.floor(record.quantity * self.partial_fill_ratio)))
        else:
            fill_qty = record.quantity

        fill_price, slippage = self._apply_slippage(ref_price, record.side)

        ok = self._book_fill(record, fill_qty, fill_price, slippage)
        if not ok:
            record.status = OrderStatus.REJECTED
            record.reject_reason = "帳務處理失敗"
            record.update_time = self._now()
            return

        if fill_qty < record.quantity:
            record.status = OrderStatus.PARTIALLY_FILLED
            record.filled_qty = fill_qty
            record.remaining_qty = record.quantity - fill_qty
            record.avg_fill_price = round(fill_price, 4)
        else:
            record.status = OrderStatus.FILLED
            record.filled_qty = fill_qty
            record.remaining_qty = 0
            record.avg_fill_price = round(fill_price, 4)

        record.update_time = self._now()

    def _book_fill(self, record: OrderRecord, fill_qty: int, fill_price: float, slippage: float) -> bool:
        symbol = record.symbol
        side = record.side
        current_pos = self.positions.get(symbol, 0)

        gross_amount = fill_qty * fill_price
        commission = gross_amount * self.commission_rate
        tax = gross_amount * self.tax_rate_sell if side in {OrderSide.SELL, OrderSide.SHORT} else 0.0

        if side == OrderSide.BUY:
            total_cost = gross_amount + commission
            if self.cash < total_cost:
                return False
            self.cash -= total_cost
            self.positions[symbol] = current_pos + fill_qty

        elif side == OrderSide.SELL:
            if current_pos < fill_qty:
                return False
            self.cash += gross_amount - commission - tax
            self.positions[symbol] = current_pos - fill_qty

        elif side == OrderSide.SHORT:
            self.cash += gross_amount - commission - tax
            self.positions[symbol] = current_pos - fill_qty

        elif side == OrderSide.COVER:
            short_qty = abs(min(current_pos, 0))
            if short_qty < fill_qty:
                return False
            total_cost = gross_amount + commission
            if self.cash < total_cost:
                return False
            self.cash -= total_cost
            self.positions[symbol] = current_pos + fill_qty

        else:
            return False

        if self.positions.get(symbol, 0) == 0:
            self.positions.pop(symbol, None)

        self.pending_fills.append(
            FillEvent(
                order_id=record.order_id,
                symbol=symbol,
                side=side,
                fill_qty=fill_qty,
                fill_price=round(fill_price, 4),
                fill_time=self._now(),
                commission=round(commission, 4),
                tax=round(tax, 4),
                slippage=round(slippage, 6),
                strategy_name=record.strategy_name,
                signal_id=record.signal_id,
                note=record.note,
            )
        )
        return True

    def _precheck(self, order: OrderRequest) -> tuple[bool, str]:
        if not order.symbol:
            return False, "symbol 不可為空"

        if int(order.quantity) <= 0:
            return False, "quantity 必須 > 0"

        if order.order_type == OrderType.LIMIT and (order.limit_price is None or order.limit_price <= 0):
            return False, "LIMIT 單必須提供有效價格"

        ref_price = self._get_request_ref_price(order)
        if ref_price <= 0:
            return False, "參考價格無效"

        est_value = ref_price * order.quantity
        est_commission = est_value * self.commission_rate
        current_pos = self.positions.get(order.symbol, 0)

        if order.side == OrderSide.BUY:
            need_cash = est_value + est_commission
            if self.cash < need_cash:
                return False, f"現金不足，約需 {need_cash:,.0f}"

        elif order.side == OrderSide.SELL:
            if current_pos < order.quantity:
                return False, f"多單不足，現有 {current_pos}"

        elif order.side == OrderSide.COVER:
            short_qty = abs(min(current_pos, 0))
            if short_qty < order.quantity:
                return False, f"空單不足，現有 {short_qty}"
            need_cash = est_value + est_commission
            if self.cash < need_cash:
                return False, f"現金不足以回補，約需 {need_cash:,.0f}"

        return True, ""

    def _resolve_reference_price(self, record: OrderRecord) -> float:
        if record.order_type == OrderType.LIMIT and record.limit_price is not None:
            return float(record.limit_price)
        return float(self.last_prices.get(record.symbol, 0.0))

    def _get_request_ref_price(self, order: OrderRequest) -> float:
        if order.order_type == OrderType.LIMIT and order.limit_price is not None:
            return float(order.limit_price)
        return float(self.last_prices.get(order.symbol, 0.0))

    @staticmethod
    def _limit_crossed(side: OrderSide, market_price: float, limit_price: float) -> bool:
        if side in {OrderSide.BUY, OrderSide.COVER}:
            return market_price <= limit_price
        if side in {OrderSide.SELL, OrderSide.SHORT}:
            return market_price >= limit_price
        return False

    def _apply_slippage(self, ref_price: float, side: OrderSide) -> tuple[float, float]:
        slip = ref_price * (self.default_slippage_bps / 10000.0)
        if side in {OrderSide.BUY, OrderSide.COVER}:
            return ref_price + slip, slip
        return ref_price - slip, slip

    @staticmethod
    def _new_order_id() -> str:
        return f"PB-{uuid.uuid4().hex[:12].upper()}"

    @staticmethod
    def _now() -> str:
        return datetime.now().isoformat(timespec="seconds")
