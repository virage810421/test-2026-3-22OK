# -*- coding: utf-8 -*-
import uuid
from typing import Dict, List
from fts_config import CONFIG
from fts_models import Position, Fill, AccountSnapshot, Order, OrderStatus, OrderSide
from fts_utils import round_price, now_str
from fts_broker_interface import BrokerBase

class PaperBroker(BrokerBase):
    MODULE_VERSION = "v16"

    def __init__(self, starting_cash: float):
        self.cash = float(starting_cash)
        self.positions: Dict[str, Position] = {}
        self.last_prices: Dict[str, float] = {}

    def restore_state(self, cash, positions, last_prices=None):
        self.cash = float(cash)
        self.positions = positions or {}
        self.last_prices = last_prices or {}

    def update_market_price(self, ticker: str, price: float):
        if price > 0:
            self.last_prices[ticker] = float(price)
            pos = self.positions.get(ticker)
            if pos:
                pos.highest_price = max(pos.highest_price, float(price))

    def _calc_costs(self, side: OrderSide, gross_amount: float):
        commission = gross_amount * CONFIG.commission_rate
        tax = gross_amount * CONFIG.tax_rate_sell if side == OrderSide.SELL else 0.0
        return commission, tax

    def _apply_slippage(self, side: OrderSide, ref_price: float, step_mult: float = 1.0) -> float:
        bps = (CONFIG.slippage_bps * step_mult) / 10000.0
        return round_price(ref_price * (1.0 + bps)) if side == OrderSide.BUY else round_price(ref_price * (1.0 - bps))

    def _apply_fill_to_portfolio(self, order: Order, fill_qty: int, fill_price: float, commission: float, tax: float):
        gross_amount = fill_price * fill_qty
        if order.side == OrderSide.BUY:
            total_cost = gross_amount + commission + tax
            if self.cash < total_cost:
                return False
            self.cash -= total_cost
            pos = self.positions.get(order.ticker)
            if pos:
                new_qty = pos.qty + fill_qty
                new_avg_cost = ((pos.avg_cost * pos.qty) + gross_amount + commission) / max(new_qty, 1)
                pos.qty = new_qty
                pos.avg_cost = round_price(new_avg_cost)
                pos.updated_at = now_str()
                pos.highest_price = max(pos.highest_price, fill_price)
                pos.add_on_count += 1
                pos.lifecycle_note = "加碼完成"
            else:
                stop_loss = round_price(fill_price * (1 - CONFIG.default_stop_loss_pct))
                take_profit = round_price(fill_price * (1 + CONFIG.default_take_profit_pct))
                self.positions[order.ticker] = Position(
                    ticker=order.ticker,
                    qty=fill_qty,
                    avg_cost=round_price((gross_amount + commission) / max(fill_qty, 1)),
                    industry=order.industry,
                    updated_at=now_str(),
                    stop_loss_price=stop_loss,
                    take_profit_price=take_profit,
                    highest_price=fill_price,
                    cooldown_until=0,
                    entry_bar=CONFIG.current_bar_index,
                    partial_tp_done=False,
                    add_on_count=0,
                    lifecycle_note="新倉建立",
                )
            return True

        if order.side == OrderSide.SELL:
            pos = self.positions.get(order.ticker)
            if not pos or pos.qty < fill_qty:
                return False
            self.cash += gross_amount - commission - tax
            pos.qty -= fill_qty
            pos.updated_at = now_str()
            pos.lifecycle_note = "減碼/出場"
            if pos.qty == 0:
                del self.positions[order.ticker]
            return True
        return False

    def place_order(self, order: Order):
        order.status = OrderStatus.SUBMITTED
        order.updated_at = now_str()
        fills: List[Fill] = []

        if CONFIG.execution_style == "TWAP3" and order.qty >= 3 * CONFIG.lot_size:
            slices = [order.qty // 3, order.qty // 3, order.qty - 2 * (order.qty // 3)]
            slices = [max(CONFIG.lot_size, (x // CONFIG.lot_size) * CONFIG.lot_size) for x in slices]
            diff = order.qty - sum(slices)
            slices[-1] += diff

            for i, qty in enumerate(slices, start=1):
                fill_price = self._apply_slippage(order.side, order.ref_price, step_mult=i)
                self.last_prices[order.ticker] = fill_price
                gross = fill_price * qty
                commission, tax = self._calc_costs(order.side, gross)
                ok = self._apply_fill_to_portfolio(order, qty, fill_price, commission, tax)
                if not ok:
                    order.status = OrderStatus.PARTIALLY_FILLED if fills else OrderStatus.REJECTED
                    order.updated_at = now_str()
                    order.note = f"TWAP 第{i}段失敗"
                    return order, fills
                fills.append(Fill(str(uuid.uuid4()), order.order_id, order.ticker, order.side, qty, fill_price, round(commission,2), round(tax,2), now_str()))
            order.status = OrderStatus.FILLED
            order.updated_at = now_str()
            order.note = "TWAP3 完成成交"
            return order, fills

        fill_price = self._apply_slippage(order.side, order.ref_price)
        self.last_prices[order.ticker] = fill_price
        gross = fill_price * order.qty
        commission, tax = self._calc_costs(order.side, gross)
        ok = self._apply_fill_to_portfolio(order, order.qty, fill_price, commission, tax)
        if not ok:
            order.status = OrderStatus.REJECTED
            order.updated_at = now_str()
            order.note = "成交失敗：現金或持倉不足"
            return order, fills
        fills.append(Fill(str(uuid.uuid4()), order.order_id, order.ticker, order.side, order.qty, fill_price, round(commission,2), round(tax,2), now_str()))
        order.status = OrderStatus.FILLED
        order.updated_at = now_str()
        return order, fills

    def cancel_order(self, order: Order):
        order.status = OrderStatus.CANCELLED
        order.updated_at = now_str()
        return order

    def get_positions(self): return self.positions

    def get_account_snapshot(self):
        market_value = 0.0
        for ticker, pos in self.positions.items():
            px = self.last_prices.get(ticker, pos.avg_cost)
            market_value += px * pos.qty
        equity = self.cash + market_value
        exposure_ratio = (market_value / equity) if equity > 0 else 0.0
        return AccountSnapshot(round(self.cash,2), round(market_value,2), round(equity,2), round(exposure_ratio,6), now_str())
