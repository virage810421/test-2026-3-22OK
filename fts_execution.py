# -*- coding: utf-8 -*-
from fts_models import Order, OrderSide, OrderStatus, TradeSignal
from fts_utils import now_str, round_price
from fts_config import CONFIG
import uuid

class ReconciliationService:
    def summarize(self, orders, fills):
        return {"orders_count": len(orders), "fills_count": len(fills), "filled_order_ids": sorted(list({f.order_id for f in fills}))}

class PositionMonitor:
    def generate_exit_signals(self, broker):
        signals = []
        positions = broker.get_positions()
        for ticker, pos in positions.items():
            last_px = broker.last_prices.get(ticker, pos.avg_cost)
            trailing_stop = round_price(max(pos.stop_loss_price, pos.highest_price * (1 - CONFIG.trailing_stop_pct)))
            bars_held = max(0, CONFIG.current_bar_index - pos.entry_bar)

            if CONFIG.enable_bracket_exit:
                if not pos.partial_tp_done and pos.take_profit_price > 0 and last_px >= pos.take_profit_price:
                    sell_qty = max(CONFIG.lot_size, int(pos.qty * CONFIG.partial_take_profit_ratio) // CONFIG.lot_size * CONFIG.lot_size)
                    sell_qty = min(sell_qty, pos.qty)
                    signals.append(TradeSignal(
                        ticker=ticker, action="SELL", reference_price=last_px, target_qty=sell_qty,
                        score=99, ai_confidence=1.0, strategy_name="PartialTakeProfitExit",
                        reason="觸發分批停利", regime="Exit"
                    ))
                    pos.partial_tp_done = True
                    if CONFIG.break_even_after_partial_tp:
                        pos.stop_loss_price = max(pos.stop_loss_price, pos.avg_cost)
                        pos.lifecycle_note = "分批停利後保本停損"
                elif last_px <= trailing_stop:
                    signals.append(TradeSignal(
                        ticker=ticker, action="SELL", reference_price=last_px, target_qty=pos.qty,
                        score=99, ai_confidence=1.0, strategy_name="TrailingStopExit",
                        reason="觸發移動停損", regime="Exit"
                    ))
                    pos.cooldown_until = CONFIG.current_bar_index + CONFIG.position_cooldown_bars
                    pos.lifecycle_note = "移動停損出場"
                elif bars_held >= CONFIG.max_holding_bars:
                    signals.append(TradeSignal(
                        ticker=ticker, action="SELL", reference_price=last_px, target_qty=pos.qty,
                        score=99, ai_confidence=1.0, strategy_name="MaxHoldingExit",
                        reason="超過最大持有bar", regime="Exit"
                    ))
                    pos.cooldown_until = CONFIG.current_bar_index + CONFIG.position_cooldown_bars
                    pos.lifecycle_note = "時間出場"
        return signals

class ExecutionEngine:
    def __init__(self, broker, logger=None):
        self.broker = broker
        self.logger = logger
        self.reconciliation = ReconciliationService()
        self.monitor = PositionMonitor()

    def signal_to_order(self, s):
        side = OrderSide.BUY if s.action == "BUY" else OrderSide.SELL
        t = now_str()
        note_parts = [s.reason]
        if s.regime: note_parts.append(f"regime={s.regime}")
        return Order(str(uuid.uuid4()), s.ticker, side, s.target_qty, s.reference_price, s.reference_price,
                     OrderStatus.NEW, s.strategy_name, s.score, s.ai_confidence, s.industry,
                     t, t, " | ".join([x for x in note_parts if x]), s.model_name, s.model_version, s.regime)

    def execute(self, signals):
        auto_exit_signals = self.monitor.generate_exit_signals(self.broker)
        merged_signals = auto_exit_signals + signals

        submitted = filled = rejected = partially_filled = cancelled = 0
        orders = []; all_fills = []
        for s in merged_signals:
            self.broker.update_market_price(s.ticker, s.reference_price)
            order = self.signal_to_order(s); orders.append(order)
            if self.logger: self.logger.insert_order(order)
            order.status = OrderStatus.PENDING_SUBMIT; order.updated_at = now_str()
            if self.logger: self.logger.update_order_status(order.order_id, order.status, order.updated_at)
            order, fills = self.broker.place_order(order); all_fills.extend(fills)
            if order.status == OrderStatus.PARTIALLY_FILLED: partially_filled += 1
            elif order.status == OrderStatus.FILLED: filled += 1
            elif order.status == OrderStatus.REJECTED: rejected += 1
            elif order.status == OrderStatus.CANCELLED: cancelled += 1
            submitted += 1
            if self.logger:
                self.logger.update_order_status(order.order_id, order.status, order.updated_at, order.note)
                for f in fills: self.logger.insert_fill(f)
        recon = self.reconciliation.summarize(orders, all_fills)
        return {
            "submitted": submitted,
            "filled": filled,
            "partially_filled": partially_filled,
            "rejected": rejected,
            "cancelled": cancelled,
            "fills_count": len(all_fills),
            "auto_exit_signals": len(auto_exit_signals),
            "reconciliation": recon,
        }
