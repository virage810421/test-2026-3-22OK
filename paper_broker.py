from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any

from broker_base import BrokerBase, FillEvent, OrderRecord, OrderRequest, OrderSide, OrderStatus, OrderType

try:
    from fts_execution_journal_service import append_execution_journal_event
except Exception:  # pragma: no cover
    def append_execution_journal_event(*args, **kwargs):
        return {'ok': False, 'reason': 'journal_import_failed'}


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
        self.protective_stops: Dict[str, Dict[str, Any]] = {}
        self.stop_trigger_history: List[Dict[str, Any]] = []

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
        fill = FillEvent(order_id=order.order_id, symbol=order.symbol, side=order.side, fill_qty=int(qty), fill_price=float(px), fill_time=_now(), commission=commission, tax=tax, slippage=round(abs(px - (order.limit_price or self.last_prices.get(order.symbol, px))), 4), strategy_name=order.strategy_name, signal_id=order.signal_id, note=order.note)
        self.pending_fills.append(fill)
        self.fill_history.append(fill)
        append_execution_journal_event('PAPER_BROKER_FILL', source='paper_broker', ticker=order.symbol, order_id=order.order_id, side=order.side.value if hasattr(order.side, 'value') else str(order.side), filled_qty=int(qty), fill_price=float(px), status='FILLED', strategy_name=order.strategy_name, signal_id=order.signal_id, commission=float(commission), tax=float(tax), note=order.note)

    def place_order(self, order: OrderRequest) -> OrderRecord:
        record = self._make_order_record(order)
        self.open_orders[record.order_id] = record
        append_execution_journal_event('PAPER_BROKER_ORDER_SUBMITTED', source='paper_broker', ticker=order.symbol, order_id=record.order_id, client_order_id=record.client_order_id, side=order.side.value if hasattr(order.side, 'value') else str(order.side), qty=int(order.quantity), reference_price=float(order.limit_price or self.last_prices.get(order.symbol, 0.0) or 0.0), status='SUBMITTED', strategy_name=order.strategy_name, signal_id=order.signal_id, note=order.note)
        if order.side in (OrderSide.SHORT, OrderSide.COVER) and not self.allow_short:
            record.status = OrderStatus.REJECTED
            record.reject_reason = 'short_not_allowed'
            record.update_time = _now()
            append_execution_journal_event('PAPER_BROKER_ORDER_REJECTED', source='paper_broker', ticker=order.symbol, order_id=record.order_id, side=order.side.value if hasattr(order.side, 'value') else str(order.side), qty=int(order.quantity), status='REJECTED', reject_reason=record.reject_reason, strategy_name=order.strategy_name, signal_id=order.signal_id, note=order.note)
            return record
        ref_price = self._resolve_reference_price(order)
        if ref_price <= 0:
            record.status = OrderStatus.REJECTED
            record.reject_reason = 'invalid_reference_price'
            record.update_time = _now()
            append_execution_journal_event('PAPER_BROKER_ORDER_REJECTED', source='paper_broker', ticker=order.symbol, order_id=record.order_id, side=order.side.value if hasattr(order.side, 'value') else str(order.side), qty=int(order.quantity), status='REJECTED', reject_reason=record.reject_reason, strategy_name=order.strategy_name, signal_id=order.signal_id, note=order.note)
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
                append_execution_journal_event('PAPER_BROKER_ORDER_REJECTED', source='paper_broker', ticker=order.symbol, order_id=record.order_id, side=order.side.value if hasattr(order.side, 'value') else str(order.side), qty=int(order.quantity), status='REJECTED', reject_reason=record.reject_reason, strategy_name=order.strategy_name, signal_id=order.signal_id, note=order.note)
                return record
            if order.side == OrderSide.COVER and pos >= 0:
                record.status = OrderStatus.REJECTED
                record.reject_reason = 'no_short_position'
                record.update_time = _now()
                append_execution_journal_event('PAPER_BROKER_ORDER_REJECTED', source='paper_broker', ticker=order.symbol, order_id=record.order_id, side=order.side.value if hasattr(order.side, 'value') else str(order.side), qty=int(order.quantity), status='REJECTED', reject_reason=record.reject_reason, strategy_name=order.strategy_name, signal_id=order.signal_id, note=order.note)
                return record
            self.cash -= total_cost
            self.positions[order.symbol] = pos + fill_qty
        else:
            if order.side == OrderSide.SELL and pos < fill_qty:
                record.status = OrderStatus.REJECTED
                record.reject_reason = 'insufficient_position'
                record.update_time = _now()
                append_execution_journal_event('PAPER_BROKER_ORDER_REJECTED', source='paper_broker', ticker=order.symbol, order_id=record.order_id, side=order.side.value if hasattr(order.side, 'value') else str(order.side), qty=int(order.quantity), status='REJECTED', reject_reason=record.reject_reason, strategy_name=order.strategy_name, signal_id=order.signal_id, note=order.note)
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
        append_execution_journal_event('PAPER_BROKER_ORDER_STATUS', source='paper_broker', ticker=order.symbol, order_id=record.order_id, side=order.side.value if hasattr(order.side, 'value') else str(order.side), qty=int(order.quantity), filled_qty=int(fill_qty), fill_price=float(fill_price), status=record.status.value if hasattr(record.status, 'value') else str(record.status), strategy_name=order.strategy_name, signal_id=order.signal_id, note=order.note)
        return record


    def upsert_protective_stop(self, symbol: str, quantity: int, stop_price: float, side: str = "SELL", client_order_id: str = "", note: str = "") -> Dict[str, Any]:
        symbol = str(symbol).strip()
        if not symbol or quantity <= 0 or float(stop_price) <= 0:
            return {'ok': False, 'status': 'invalid_stop_payload', 'symbol': symbol}
        order_id = client_order_id or f"STOP-{symbol}-{len(self.protective_stops)+1:04d}"
        rec = self.protective_stops.get(order_id, {})
        rec.update({
            'order_id': order_id,
            'symbol': symbol,
            'side': str(side).upper(),
            'quantity': int(quantity),
            'stop_price': round(float(stop_price), 4),
            'status': 'WORKING',
            'note': str(note or ''),
            'update_time': _now(),
        })
        self.protective_stops[order_id] = rec
        return {'ok': True, 'status': 'protective_stop_upserted', 'broker_order_id': order_id, 'record': dict(rec)}

    def replace_order(self, order_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        rec = self.open_orders.get(order_id)
        if rec is not None:
            if rec.status not in (OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED):
                return {'ok': False, 'status': 'replace_blocked', 'broker_order_id': order_id}
            if 'price' in payload or 'limit_price' in payload:
                new_px = float(payload.get('price', payload.get('limit_price', rec.limit_price or 0.0)) or 0.0)
                if new_px > 0:
                    rec.limit_price = new_px
            if 'qty' in payload:
                rec.quantity = int(payload.get('qty') or rec.quantity)
                rec.remaining_qty = max(0, rec.quantity - rec.filled_qty)
            rec.note = str(payload.get('note', rec.note or ''))
            rec.update_time = _now()
            return {'ok': True, 'status': 'replace_ok', 'broker_order_id': order_id, 'record': rec.to_dict()}
        stop_rec = self.protective_stops.get(order_id)
        if stop_rec is not None:
            if 'stop_price' in payload:
                stop_rec['stop_price'] = round(float(payload.get('stop_price') or stop_rec['stop_price']), 4)
            if 'qty' in payload:
                stop_rec['quantity'] = int(payload.get('qty') or stop_rec['quantity'])
            stop_rec['note'] = str(payload.get('note', stop_rec.get('note', '')))
            stop_rec['update_time'] = _now()
            return {'ok': True, 'status': 'replace_ok', 'broker_order_id': order_id, 'record': dict(stop_rec)}
        return {'ok': False, 'status': 'missing_order', 'broker_order_id': order_id}

    def get_protective_stops(self) -> List[Dict[str, Any]]:
        return [dict(v) for v in self.protective_stops.values()]


    def process_protective_stops(self, price_map: Optional[Dict[str, float]] = None) -> List[Dict[str, Any]]:
        if price_map:
            self.update_market_prices(price_map)
        events: List[Dict[str, Any]] = []
        for order_id, stop_rec in list(self.protective_stops.items()):
            status = str(stop_rec.get('status', 'WORKING')).upper()
            if status not in {'WORKING', 'SUBMITTED'}:
                continue
            symbol = str(stop_rec.get('symbol', '')).strip()
            if not symbol:
                continue
            market_px = float(self.last_prices.get(symbol, 0.0) or 0.0)
            stop_price = float(stop_rec.get('stop_price', 0.0) or 0.0)
            if market_px <= 0 or stop_price <= 0:
                continue
            pos = int(self.positions.get(symbol, 0) or 0)
            qty = min(abs(pos), int(stop_rec.get('quantity', 0) or 0))
            if qty <= 0:
                stop_rec['status'] = 'CANCELLED_NO_POSITION'
                stop_rec['update_time'] = _now()
                continue
            stop_side = str(stop_rec.get('side', 'SELL') or 'SELL').upper()
            if stop_side == 'SELL':
                triggered = pos > 0 and market_px <= stop_price
                fill_side = OrderSide.SELL
                trigger_ref = min(market_px, stop_price)
            else:
                triggered = pos < 0 and market_px >= stop_price
                fill_side = OrderSide.COVER if pos < 0 else OrderSide.BUY
                trigger_ref = max(market_px, stop_price)
            if not triggered:
                continue
            fill_price = self._apply_slippage(fill_side, qty, trigger_ref)
            gross = fill_price * qty
            commission = self._commission(gross)
            tax = self._tax(fill_side, gross)
            if fill_side in (OrderSide.BUY, OrderSide.COVER):
                self.cash -= gross + commission + tax
                self.positions[symbol] = pos + qty
            else:
                self.cash += gross - commission - tax
                self.positions[symbol] = pos - qty
            if self.positions.get(symbol, 0) == 0:
                self.positions.pop(symbol, None)
            fill = FillEvent(
                order_id=str(order_id),
                symbol=symbol,
                side=fill_side,
                fill_qty=int(qty),
                fill_price=float(fill_price),
                fill_time=_now(),
                commission=commission,
                tax=tax,
                slippage=round(abs(fill_price - trigger_ref), 4),
                note=str(stop_rec.get('note', '') or '') + '|protective_stop_triggered',
            )
            self.pending_fills.append(fill)
            self.fill_history.append(fill)
            stop_rec['status'] = 'TRIGGERED_FILLED'
            stop_rec['trigger_time'] = _now()
            stop_rec['trigger_fill_price'] = float(fill_price)
            stop_rec['filled_qty'] = int(qty)
            stop_rec['update_time'] = _now()
            event = {
                'order_id': str(order_id),
                'symbol': symbol,
                'side': fill_side.value,
                'qty': int(qty),
                'stop_price': round(stop_price, 4),
                'trigger_price': round(trigger_ref, 4),
                'fill_price': round(fill_price, 4),
                'status': 'TRIGGERED_FILLED',
                'time': _now(),
            }
            self.stop_trigger_history.append(dict(event))
            append_execution_journal_event('PAPER_BROKER_PROTECTIVE_STOP_TRIGGERED', source='paper_broker', ticker=symbol, order_id=str(order_id), side=fill_side.value if hasattr(fill_side, 'value') else str(fill_side), qty=int(qty), fill_price=float(fill_price), status='TRIGGERED_FILLED', reference_price=float(trigger_ref), note=str(stop_rec.get('note', '') or ''))
            events.append(event)
        return events

    def get_fill_history_dicts(self) -> List[Dict[str, Any]]:
        return [
            {
                'order_id': f.order_id,
                'symbol': f.symbol,
                'side': f.side.value if hasattr(f.side, 'value') else str(f.side),
                'fill_qty': f.fill_qty,
                'fill_price': f.fill_price,
                'fill_time': f.fill_time,
                'commission': f.commission,
                'tax': f.tax,
                'slippage': f.slippage,
                'note': f.note,
            }
            for f in self.fill_history
        ]


    def get_open_orders_dicts(self) -> List[Dict[str, Any]]:
        return [self._order_record_to_row(x) for x in self.get_open_orders()]

    def get_positions_detailed(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for symbol, qty in self.positions.items():
            market_px = float(self.last_prices.get(symbol, 0.0) or 0.0)
            avg_cost = market_px
            market_value = abs(int(qty)) * market_px
            rows.append({
                'ticker': symbol,
                'qty': int(qty),
                'available_qty': abs(int(qty)),
                'avg_cost': avg_cost,
                'market_price': market_px,
                'market_value': market_value,
                'unrealized_pnl': 0.0,
                'realized_pnl': 0.0,
                'direction_bucket': 'LONG' if int(qty) >= 0 else 'SHORT',
                'strategy_name': 'paper_runtime',
                'industry': '未知',
            })
        return rows

    def export_runtime_snapshot(self) -> Dict[str, Any]:
        return {
            'cash': self.get_cash(),
            'positions': self.get_positions(),
            'open_orders': [self._order_record_to_row(x) for x in self.get_open_orders()],
            'protective_stops': self.get_protective_stops(),
            'stop_trigger_events': list(self.stop_trigger_history[-20:]),
        }

    @staticmethod
    def _order_record_to_row(record: OrderRecord) -> Dict[str, Any]:
        return {
            'order_id': record.order_id,
            'symbol': record.symbol,
            'side': record.side.value,
            'quantity': record.quantity,
            'filled_qty': record.filled_qty,
            'remaining_qty': record.remaining_qty,
            'avg_fill_price': record.avg_fill_price,
            'order_type': record.order_type.value,
            'limit_price': record.limit_price,
            'status': record.status.value,
            'create_time': record.create_time,
            'update_time': record.update_time,
            'reject_reason': record.reject_reason,
            'strategy_name': record.strategy_name,
            'signal_id': record.signal_id,
            'client_order_id': record.client_order_id,
            'note': record.note,
        }

    def cancel_order(self, order_id: str) -> bool:
        rec = self.open_orders.get(order_id)
        if rec is not None:
            if rec.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
                return False
            rec.status = OrderStatus.CANCELLED
            rec.update_time = _now()
            append_execution_journal_event('PAPER_BROKER_ORDER_CANCELLED', source='paper_broker', ticker=rec.symbol, order_id=rec.order_id, side=rec.side.value if hasattr(rec.side, 'value') else str(rec.side), qty=int(rec.quantity), status='CANCELLED', strategy_name=rec.strategy_name, signal_id=rec.signal_id, note=rec.note)
            return True
        stop_rec = self.protective_stops.get(order_id)
        if stop_rec is not None:
            stop_rec['status'] = 'CANCELLED'
            stop_rec['update_time'] = _now()
            append_execution_journal_event('PAPER_BROKER_STOP_CANCELLED', source='paper_broker', ticker=stop_rec.get('symbol'), order_id=order_id, side=stop_rec.get('side'), qty=int(stop_rec.get('quantity', 0) or 0), status='CANCELLED', note=stop_rec.get('note', ''))
            return True
        return False

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
        for f in fills:
            try:
                append_execution_journal_event('PAPER_BROKER_CALLBACK', source='paper_broker', ticker=getattr(f, 'symbol', ''), order_id=getattr(f, 'order_id', ''), side=(getattr(getattr(f, 'side', ''), 'value', getattr(f, 'side', ''))), filled_qty=int(getattr(f, 'fill_qty', 0) or 0), fill_price=float(getattr(f, 'fill_price', 0.0) or 0.0), status='CALLBACK_FILLED', note='poll_fills_callback')
            except Exception:
                pass
        return fills

# =============================================================================
# vNext lot-level / multi-batch position extension
# - Keeps the original aggregated self.positions behavior intact.
# - Adds FIFO lot tracking used by execution SQL snapshots and reconciliation.
# =============================================================================
try:
    _PB_ORIG_INIT = PaperBroker.__init__
    _PB_ORIG_APPEND_FILL = PaperBroker._append_fill
    _PB_ORIG_PROCESS_STOPS = PaperBroker.process_protective_stops

    def _pb_lot_now() -> str:
        return _now()

    def _pb_lot_side_from_order_side(side) -> str:
        side_v = side.value if hasattr(side, 'value') else str(side)
        side_v = str(side_v).upper()
        if side_v in {'BUY', 'LONG'}:
            return 'LONG'
        if side_v in {'SHORT'}:
            return 'SHORT'
        return side_v

    def _pb_close_side_from_order_side(side) -> str:
        side_v = side.value if hasattr(side, 'value') else str(side)
        side_v = str(side_v).upper()
        if side_v == 'SELL':
            return 'LONG'
        if side_v in {'COVER', 'BUY_TO_COVER'}:
            return 'SHORT'
        return ''

    def _pb_init_lot_book(self) -> None:
        if not hasattr(self, 'position_lots') or self.position_lots is None:
            self.position_lots = []
        if not hasattr(self, '_lot_seq'):
            self._lot_seq = 0
        if not hasattr(self, 'lot_close_history'):
            self.lot_close_history = []

    def _pb_next_lot_id(self, symbol: str) -> str:
        _pb_init_lot_book(self)
        self._lot_seq += 1
        return f"LOT-{str(symbol).replace('.', '')}-{self._lot_seq:06d}"

    def _pb_open_lot(self, symbol: str, side: str, qty: int, price: float, order_id: str = '', strategy_name: str = '', signal_id: str = '', note: str = '') -> dict:
        _pb_init_lot_book(self)
        qty = int(qty or 0)
        if qty <= 0:
            return {}
        lot = {
            'lot_id': _pb_next_lot_id(self, symbol),
            'ticker': str(symbol),
            'symbol': str(symbol),
            'side': str(side).upper(),
            'direction_bucket': str(side).upper(),
            'open_qty': qty,
            'remaining_qty': qty,
            'avg_cost': float(price or 0.0),
            'entry_price': float(price or 0.0),
            'entry_time': _pb_lot_now(),
            'entry_order_id': str(order_id or ''),
            'strategy_name': str(strategy_name or ''),
            'signal_id': str(signal_id or ''),
            'status': 'OPEN',
            'realized_pnl': 0.0,
            'close_qty': 0,
            'close_price': 0.0,
            'close_time': '',
            'note': str(note or ''),
        }
        self.position_lots.append(lot)
        return dict(lot)

    def _pb_close_lots_fifo(self, symbol: str, close_side: str, qty: int, price: float, order_id: str = '', note: str = '') -> list[dict]:
        _pb_init_lot_book(self)
        symbol = str(symbol)
        close_side = str(close_side).upper()
        remaining = int(qty or 0)
        closed: list[dict] = []
        if remaining <= 0 or close_side not in {'LONG', 'SHORT'}:
            return closed
        for lot in self.position_lots:
            if remaining <= 0:
                break
            if str(lot.get('ticker', lot.get('symbol', ''))) != symbol:
                continue
            if str(lot.get('side', '')).upper() != close_side:
                continue
            if str(lot.get('status', 'OPEN')).upper() != 'OPEN':
                continue
            lot_rem = int(lot.get('remaining_qty', 0) or 0)
            if lot_rem <= 0:
                continue
            take = min(lot_rem, remaining)
            entry_px = float(lot.get('avg_cost', lot.get('entry_price', 0)) or 0.0)
            if close_side == 'LONG':
                pnl = (float(price) - entry_px) * take
            else:
                pnl = (entry_px - float(price)) * take
            lot['remaining_qty'] = lot_rem - take
            lot['close_qty'] = int(lot.get('close_qty', 0) or 0) + take
            lot['realized_pnl'] = round(float(lot.get('realized_pnl', 0) or 0) + pnl, 4)
            lot['close_price'] = float(price or 0.0)
            lot['close_time'] = _pb_lot_now()
            lot['exit_order_id'] = str(order_id or '')
            if lot['remaining_qty'] <= 0:
                lot['status'] = 'CLOSED'
            event = {
                'lot_id': lot.get('lot_id'),
                'ticker': symbol,
                'side': close_side,
                'closed_qty': take,
                'entry_price': entry_px,
                'close_price': float(price or 0.0),
                'realized_pnl': round(pnl, 4),
                'exit_order_id': str(order_id or ''),
                'closed_at': _pb_lot_now(),
                'note': str(note or ''),
            }
            self.lot_close_history.append(event)
            closed.append(event)
            remaining -= take
        return closed

    def _pb_apply_lot_fill(self, order, qty: int, px: float, note: str = '') -> None:
        _pb_init_lot_book(self)
        symbol = getattr(order, 'symbol', '') or (order.get('symbol') if isinstance(order, dict) else '')
        side = getattr(order, 'side', '') or (order.get('side') if isinstance(order, dict) else '')
        order_id = getattr(order, 'order_id', '') or (order.get('order_id') if isinstance(order, dict) else '')
        strategy_name = getattr(order, 'strategy_name', '') or (order.get('strategy_name') if isinstance(order, dict) else '')
        signal_id = getattr(order, 'signal_id', '') or (order.get('signal_id') if isinstance(order, dict) else '')
        side_v = side.value if hasattr(side, 'value') else str(side)
        side_v = side_v.upper()
        if side_v in {'BUY', 'SHORT'}:
            lot_side = _pb_lot_side_from_order_side(side_v)
            _pb_open_lot(self, symbol, lot_side, int(qty), float(px), order_id=order_id, strategy_name=strategy_name, signal_id=signal_id, note=note)
        elif side_v in {'SELL', 'COVER', 'BUY_TO_COVER'}:
            close_side = _pb_close_side_from_order_side(side_v)
            _pb_close_lots_fifo(self, symbol, close_side, int(qty), float(px), order_id=order_id, note=note)

    def _pb_patched_init(self, *args, **kwargs):
        _PB_ORIG_INIT(self, *args, **kwargs)
        _pb_init_lot_book(self)

    def _pb_patched_append_fill(self, order, qty: int, px: float, commission: float, tax: float) -> None:
        _PB_ORIG_APPEND_FILL(self, order, qty, px, commission, tax)
        try:
            _pb_apply_lot_fill(self, order, int(qty), float(px), note=getattr(order, 'note', '') or 'paper_fill')
        except Exception as exc:
            # Lot tracking must never break the original broker fill path.
            if not hasattr(self, 'lot_tracking_errors'):
                self.lot_tracking_errors = []
            self.lot_tracking_errors.append({'time': _pb_lot_now(), 'error': repr(exc)})

    def _pb_patched_process_stops(self, price_map=None):
        before = len(getattr(self, 'stop_trigger_history', []) or [])
        events = _PB_ORIG_PROCESS_STOPS(self, price_map)
        try:
            new_events = list((getattr(self, 'stop_trigger_history', []) or [])[before:])
            for ev in new_events:
                dummy = {'symbol': ev.get('symbol', ''), 'side': ev.get('side', ''), 'order_id': ev.get('order_id', ''), 'strategy_name': 'protective_stop', 'signal_id': ev.get('order_id', '')}
                # Original process_stops updates aggregate positions directly, so only update lots here.
                side = str(ev.get('side', '')).upper()
                if side in {'SELL', 'COVER', 'BUY'}:
                    close_side = 'LONG' if side == 'SELL' else 'SHORT'
                    _pb_close_lots_fifo(self, ev.get('symbol', ''), close_side, int(ev.get('qty', 0) or 0), float(ev.get('fill_price', 0) or 0), order_id=str(ev.get('order_id', '') or ''), note='protective_stop_triggered')
        except Exception as exc:
            if not hasattr(self, 'lot_tracking_errors'):
                self.lot_tracking_errors = []
            self.lot_tracking_errors.append({'time': _pb_lot_now(), 'error': repr(exc), 'phase': 'stop_trigger_lots'})
        return events

    def _pb_get_position_lots(self, include_closed: bool = False) -> list[dict]:
        _pb_init_lot_book(self)
        rows = []
        for lot in self.position_lots:
            if include_closed or str(lot.get('status', '')).upper() == 'OPEN':
                row = dict(lot)
                market_px = float(self.last_prices.get(row.get('ticker', row.get('symbol', '')), row.get('avg_cost', 0)) or 0.0)
                rem = int(row.get('remaining_qty', 0) or 0)
                avg = float(row.get('avg_cost', 0) or 0.0)
                row['market_price'] = market_px
                row['market_value'] = abs(rem) * market_px
                row['unrealized_pnl'] = round((market_px - avg) * rem if row.get('side') == 'LONG' else (avg - market_px) * rem, 4)
                rows.append(row)
        return rows

    def _pb_get_position_lot_summary(self) -> list[dict]:
        lots = _pb_get_position_lots(self, include_closed=False)
        out: dict[tuple[str, str], dict] = {}
        for lot in lots:
            key = (lot.get('ticker', lot.get('symbol', '')), lot.get('side', 'LONG'))
            bucket = out.setdefault(key, {'ticker': key[0], 'direction_bucket': key[1], 'qty': 0, 'market_value': 0.0, 'cost_value': 0.0, 'lot_count': 0, 'realized_pnl': 0.0, 'unrealized_pnl': 0.0})
            qty = int(lot.get('remaining_qty', 0) or 0)
            bucket['qty'] += qty if key[1] == 'LONG' else -qty
            bucket['market_value'] += float(lot.get('market_value', 0) or 0.0)
            bucket['cost_value'] += qty * float(lot.get('avg_cost', 0) or 0.0)
            bucket['lot_count'] += 1
            bucket['unrealized_pnl'] += float(lot.get('unrealized_pnl', 0) or 0.0)
        for row in out.values():
            abs_qty = abs(int(row.get('qty', 0) or 0))
            row['avg_cost'] = round(row['cost_value'] / abs_qty, 4) if abs_qty else 0.0
            row['unrealized_pnl'] = round(row['unrealized_pnl'], 4)
        return list(out.values())

    def _pb_reconcile_lots_to_positions(self) -> dict:
        lots = _pb_get_position_lots(self, include_closed=False)
        lot_pos: dict[str, int] = {}
        for lot in lots:
            sym = lot.get('ticker', lot.get('symbol', ''))
            rem = int(lot.get('remaining_qty', 0) or 0)
            lot_pos[sym] = lot_pos.get(sym, 0) + (rem if lot.get('side') == 'LONG' else -rem)
        diffs = []
        symbols = set(lot_pos) | set(getattr(self, 'positions', {}).keys())
        for sym in sorted(symbols):
            agg = int(getattr(self, 'positions', {}).get(sym, 0) or 0)
            lot_qty = int(lot_pos.get(sym, 0) or 0)
            if agg != lot_qty:
                diffs.append({'ticker': sym, 'aggregate_qty': agg, 'lot_qty': lot_qty, 'diff_qty': agg - lot_qty})
        return {'ok': len(diffs) == 0, 'diffs': diffs, 'lot_count': len(lots)}

    # Patch class methods.
    PaperBroker.__init__ = _pb_patched_init
    PaperBroker._append_fill = _pb_patched_append_fill
    PaperBroker.process_protective_stops = _pb_patched_process_stops
    PaperBroker.get_position_lots = _pb_get_position_lots
    PaperBroker.get_position_lot_summary = _pb_get_position_lot_summary
    PaperBroker.reconcile_lots_to_positions = _pb_reconcile_lots_to_positions

    _PB_ORIG_EXPORT = PaperBroker.export_runtime_snapshot
    def _pb_patched_export_runtime_snapshot(self) -> Dict[str, Any]:
        snap = _PB_ORIG_EXPORT(self)
        try:
            snap['position_lots'] = self.get_position_lots(include_closed=True)
            snap['position_lot_summary'] = self.get_position_lot_summary()
            snap['lot_reconciliation'] = self.reconcile_lots_to_positions()
        except Exception as exc:
            snap['lot_reconciliation_error'] = repr(exc)
        return snap
    PaperBroker.export_runtime_snapshot = _pb_patched_export_runtime_snapshot

except Exception:
    # Keep import safety even if a downstream legacy PaperBroker differs.
    pass


# =============================================================================
# vNext institutional lot lifecycle / multi-strategy partition extension
# =============================================================================
try:
    import json as _json
    try:
        import config as _PB_CFG
    except Exception:
        _PB_CFG = None

    _PB_V2_ORIG_INIT = globals().get('_PB_ORIG_INIT', PaperBroker.__init__)
    _PB_V2_ORIG_APPEND_FILL = globals().get('_PB_ORIG_APPEND_FILL', PaperBroker._append_fill)
    _PB_V2_ORIG_UPSERT_STOP = PaperBroker.upsert_protective_stop
    _PB_V2_ORIG_REPLACE_ORDER = PaperBroker.replace_order
    _PB_V2_ORIG_PROCESS_STOPS2 = globals().get('_PB_ORIG_PROCESS_STOPS', PaperBroker.process_protective_stops)

    def _pb_cfg(name, default=None):
        return getattr(_PB_CFG, name, default) if _PB_CFG is not None else default

    def _pb_v2_now():
        return _now()

    def _pb_json_append(raw, value):
        try:
            arr = list(raw) if isinstance(raw, (list, tuple)) else (_json.loads(raw) if isinstance(raw, str) and raw else [])
        except Exception:
            arr = []
        arr.append(value)
        return _json.dumps(arr, ensure_ascii=False)

    def _pb_cost_method():
        return str(_pb_cfg('LOT_ACCOUNTING_METHOD', 'FIFO') or 'FIFO').upper()

    def _pb_init_lifecycle(self):
        self.position_lots = [] if not isinstance(getattr(self, 'position_lots', None), list) else self.position_lots
        self._lot_seq = int(getattr(self, '_lot_seq', 0) or 0)
        self.lot_close_history = [] if not isinstance(getattr(self, 'lot_close_history', None), list) else self.lot_close_history
        self.lot_fill_history = [] if not isinstance(getattr(self, 'lot_fill_history', None), list) else self.lot_fill_history
        self.lot_stop_link_events = [] if not isinstance(getattr(self, 'lot_stop_link_events', None), list) else self.lot_stop_link_events
        self.lot_lifecycle_errors = [] if not isinstance(getattr(self, 'lot_lifecycle_errors', None), list) else self.lot_lifecycle_errors

    def _pb_position_key(symbol, side, strategy_name='', signal_id=''):
        parts=[str(symbol).upper(), str(side).upper()]
        if bool(_pb_cfg('LOT_PARTITION_BY_STRATEGY', True)):
            parts.append(str(strategy_name or ''))
        if bool(_pb_cfg('LOT_PARTITION_BY_SIGNAL', True)):
            parts.append(str(signal_id or ''))
        return '|'.join(parts)

    def _pb_next_lot_id_v2(self, symbol: str) -> str:
        _pb_init_lifecycle(self)
        self._lot_seq += 1
        return f"LOT2-{str(symbol).replace('.', '')}-{self._lot_seq:07d}"

    def _pb_side_from_order(side) -> str:
        side_v = side.value if hasattr(side, 'value') else str(side)
        side_v = str(side_v).upper()
        return 'LONG' if side_v in {'BUY','LONG'} else ('SHORT' if side_v == 'SHORT' else side_v)

    def _pb_close_bucket_from_order(side) -> str:
        side_v = side.value if hasattr(side, 'value') else str(side)
        side_v = str(side_v).upper()
        return 'LONG' if side_v == 'SELL' else ('SHORT' if side_v in {'COVER','BUY_TO_COVER'} else '')

    def _pb_open_lot_v2(self, symbol: str, side: str, qty: int, price: float, order=None, fill_id: str = '', commission: float = 0.0, tax: float = 0.0, note: str = ''):
        _pb_init_lifecycle(self)
        qty = int(qty or 0)
        if qty <= 0:
            return {}
        strategy_name = getattr(order, 'strategy_name', '') or (order.get('strategy_name') if isinstance(order, dict) else '') or ''
        signal_id = getattr(order, 'signal_id', '') or (order.get('signal_id') if isinstance(order, dict) else '') or ''
        client_order_id = getattr(order, 'client_order_id', '') or (order.get('client_order_id') if isinstance(order, dict) else '') or ''
        order_id = getattr(order, 'order_id', '') or (order.get('order_id') if isinstance(order, dict) else '') or ''
        lot = {
            'lot_id': _pb_next_lot_id_v2(self, symbol), 'ticker': str(symbol), 'symbol': str(symbol), 'side': str(side).upper(), 'direction_bucket': str(side).upper(),
            'position_key': _pb_position_key(symbol, side, strategy_name, signal_id), 'strategy_name': str(strategy_name), 'strategy_bucket': str(strategy_name), 'signal_id': str(signal_id), 'client_order_id': str(client_order_id),
            'open_qty': qty, 'remaining_qty': qty, 'avg_cost': float(price or 0.0), 'entry_price': float(price or 0.0), 'entry_time': _pb_v2_now(), 'entry_order_id': str(order_id),
            'entry_fill_qty': qty, 'close_fill_qty': 0, 'entry_fill_count': 1, 'close_fill_count': 0, 'entry_fill_ids_json': _json.dumps([fill_id] if fill_id else []), 'exit_fill_ids_json': _json.dumps([]),
            'open_commission': float(commission or 0.0), 'open_tax': float(tax or 0.0), 'close_commission': 0.0, 'close_tax': 0.0, 'cost_basis_method': _pb_cost_method(),
            'status': 'OPEN', 'lifecycle_status': 'OPEN', 'realized_pnl': 0.0, 'close_qty': 0, 'close_price': 0.0, 'close_time': '', 'stop_order_id': '', 'stop_price': 0.0, 'stop_status': '', 'linked_stop_qty': 0, 'last_fill_time': _pb_v2_now(), 'note': str(note or '')
        }
        self.position_lots.append(lot)
        self.lot_fill_history.append({'fill_id': fill_id or '', 'lot_id': lot['lot_id'], 'symbol': symbol, 'event': 'OPEN', 'qty': qty, 'price': float(price or 0.0), 'commission': float(commission or 0.0), 'tax': float(tax or 0.0), 'time': _pb_v2_now()})
        return dict(lot)

    def _pb_iter_eligible_lots(self, symbol: str, close_side: str, strategy_name: str = '', signal_id: str = ''):
        rows=[]
        for lot in self.position_lots:
            if str(lot.get('ticker', lot.get('symbol', ''))).upper()!=str(symbol).upper(): continue
            if str(lot.get('side','')).upper()!=str(close_side).upper(): continue
            if int(lot.get('remaining_qty',0) or 0)<=0: continue
            if bool(_pb_cfg('LOT_PARTITION_BY_STRATEGY', True)) and strategy_name and str(lot.get('strategy_name',''))!=str(strategy_name): continue
            if bool(_pb_cfg('LOT_PARTITION_BY_SIGNAL', True)) and signal_id and str(lot.get('signal_id',''))!=str(signal_id): continue
            rows.append(lot)
        if not rows and bool(_pb_cfg('LOT_ALLOW_CROSS_STRATEGY_CLOSE', False)):
            rows=[lot for lot in self.position_lots if str(lot.get('ticker', lot.get('symbol',''))).upper()==str(symbol).upper() and str(lot.get('side','')).upper()==str(close_side).upper() and int(lot.get('remaining_qty',0) or 0)>0]
        rows.sort(key=lambda r:(str(r.get('entry_time','')), str(r.get('lot_id',''))))
        return rows

    def _pb_close_lots_v2(self, symbol: str, close_side: str, qty: int, price: float, order=None, fill_id: str = '', commission: float = 0.0, tax: float = 0.0, note: str = ''):
        _pb_init_lifecycle(self)
        remaining=int(qty or 0)
        if remaining<=0 or close_side not in {'LONG','SHORT'}: return []
        strategy_name = getattr(order, 'strategy_name', '') or (order.get('strategy_name') if isinstance(order, dict) else '') or ''
        signal_id = getattr(order, 'signal_id', '') or (order.get('signal_id') if isinstance(order, dict) else '') or ''
        order_id = getattr(order, 'order_id', '') or (order.get('order_id') if isinstance(order, dict) else '') or ''
        eligible=_pb_iter_eligible_lots(self, symbol, close_side, strategy_name=strategy_name, signal_id=signal_id)
        avg_basis=None
        if _pb_cost_method()=='AVERAGE' and eligible:
            tq=sum(int(l.get('remaining_qty',0) or 0) for l in eligible)
            tc=sum(int(l.get('remaining_qty',0) or 0)*float(l.get('avg_cost',0) or 0.0) for l in eligible)
            avg_basis=(tc/tq) if tq else None
        events=[]
        for lot in eligible:
            if remaining<=0: break
            lot_qty=int(lot.get('remaining_qty',0) or 0)
            if lot_qty<=0: continue
            take=min(lot_qty, remaining)
            basis=float(avg_basis if avg_basis is not None else (lot.get('avg_cost', lot.get('entry_price',0)) or 0.0))
            pnl=(float(price)-basis)*take if close_side=='LONG' else (basis-float(price))*take
            lot['remaining_qty']=lot_qty-take
            lot['close_qty']=int(lot.get('close_qty',0) or 0)+take
            lot['close_fill_qty']=int(lot.get('close_fill_qty',0) or 0)+take
            lot['close_fill_count']=int(lot.get('close_fill_count',0) or 0)+1
            if fill_id: lot['exit_fill_ids_json']=_pb_json_append(lot.get('exit_fill_ids_json'), fill_id)
            lot['realized_pnl']=round(float(lot.get('realized_pnl',0) or 0.0)+pnl-float(commission or 0.0)-float(tax or 0.0),4)
            lot['close_commission']=round(float(lot.get('close_commission',0) or 0.0)+float(commission or 0.0),4)
            lot['close_tax']=round(float(lot.get('close_tax',0) or 0.0)+float(tax or 0.0),4)
            lot['close_price']=float(price or 0.0)
            lot['close_time']=_pb_v2_now()
            lot['exit_order_id']=str(order_id)
            lot['last_fill_time']=_pb_v2_now()
            lot['close_cost_basis_method']=_pb_cost_method()
            lot['close_cost_basis_price']=round(float(basis or 0.0),4)
            if lot['remaining_qty']<=0:
                lot['status']='CLOSED'; lot['lifecycle_status']='CLOSED'
            else:
                lot['status']='PARTIAL_EXIT'; lot['lifecycle_status']='PARTIAL_EXIT'
            ev={'lot_id': lot.get('lot_id'), 'ticker': symbol, 'side': close_side, 'closed_qty': take, 'entry_price': float(lot.get('entry_price', lot.get('avg_cost',0)) or 0.0), 'close_price': float(price or 0.0), 'cost_basis_price': round(float(basis or 0.0),4), 'cost_basis_method': _pb_cost_method(), 'realized_pnl': round(float(pnl)-float(commission or 0.0)-float(tax or 0.0),4), 'exit_order_id': str(order_id), 'fill_id': fill_id, 'position_key': lot.get('position_key'), 'strategy_name': lot.get('strategy_name'), 'signal_id': lot.get('signal_id'), 'closed_at': _pb_v2_now(), 'note': str(note or '')}
            self.lot_close_history.append(ev); self.lot_fill_history.append({'fill_id': fill_id or '', 'lot_id': lot.get('lot_id'), 'symbol': symbol, 'event': 'CLOSE', 'qty': take, 'price': float(price or 0.0), 'commission': float(commission or 0.0), 'tax': float(tax or 0.0), 'time': _pb_v2_now()})
            events.append(ev); remaining-=take
        return events

    def _pb_link_stop_to_lots(self, stop_rec: dict):
        if not bool(_pb_cfg('LOT_STOP_LINKAGE_ENABLED', True)): return {'linked':0,'lot_ids':[]}
        symbol=str(stop_rec.get('symbol','') or '').upper(); stop_side=str(stop_rec.get('side','SELL') or 'SELL').upper(); close_side='LONG' if stop_side=='SELL' else 'SHORT'
        qty_need=int(stop_rec.get('quantity',0) or 0); strategy_name=str(stop_rec.get('strategy_name','') or ''); signal_id=str(stop_rec.get('signal_id','') or '')
        linked=[]; remaining=qty_need
        for lot in _pb_iter_eligible_lots(self, symbol, close_side, strategy_name=strategy_name if bool(_pb_cfg('LOT_STOP_LINKAGE_MATCH_STRATEGY', True)) else '', signal_id=signal_id if bool(_pb_cfg('LOT_STOP_LINKAGE_MATCH_SIGNAL', False)) else ''):
            if remaining<=0: break
            take=min(int(lot.get('remaining_qty',0) or 0), remaining)
            lot['stop_order_id']=str(stop_rec.get('order_id') or '')
            lot['stop_price']=float(stop_rec.get('stop_price',0) or 0.0)
            lot['stop_status']=str(stop_rec.get('status','WORKING') or '')
            lot['linked_stop_qty']=take
            linked.append(str(lot.get('lot_id'))); remaining-=take
        stop_rec['linked_lot_ids']=linked
        stop_rec['linked_position_keys']=[str(l.get('position_key')) for l in self.position_lots if str(l.get('lot_id')) in linked]
        self.lot_stop_link_events.append({'time': _pb_v2_now(), 'order_id': stop_rec.get('order_id'), 'symbol': symbol, 'linked_lot_ids': list(linked), 'linked_qty': qty_need-remaining})
        return {'linked': qty_need-remaining, 'lot_ids': linked}

    def _pb_init_wrapper_v2(self, *args, **kwargs):
        _PB_V2_ORIG_INIT(self, *args, **kwargs)
        self.position_lots=[]
        _pb_init_lifecycle(self)

    def _pb_append_fill_wrapper_v2(self, order, qty: int, px: float, commission: float, tax: float):
        _PB_V2_ORIG_APPEND_FILL(self, order, qty, px, commission, tax)
        try:
            fill_idx=len(getattr(self,'fill_history',[]) or [])
            fill_id=f"PFILL-{getattr(order,'order_id','') or (order.get('order_id') if isinstance(order, dict) else '')}-{fill_idx:06d}"
            side=getattr(order,'side','') or (order.get('side') if isinstance(order, dict) else '')
            side_v=side.value if hasattr(side,'value') else str(side)
            side_v=str(side_v).upper()
            symbol=getattr(order,'symbol','') or (order.get('symbol') if isinstance(order, dict) else '')
            if side_v in {'BUY','SHORT'}:
                _pb_open_lot_v2(self, symbol, _pb_side_from_order(side_v), int(qty), float(px), order=order, fill_id=fill_id, commission=commission, tax=tax, note=getattr(order,'note','') or 'paper_fill')
            elif side_v in {'SELL','COVER','BUY_TO_COVER'}:
                _pb_close_lots_v2(self, symbol, _pb_close_bucket_from_order(side_v), int(qty), float(px), order=order, fill_id=fill_id, commission=commission, tax=tax, note=getattr(order,'note','') or 'paper_fill')
        except Exception as exc:
            self.lot_lifecycle_errors.append({'time': _pb_v2_now(), 'error': repr(exc), 'phase': 'append_fill_v2'})

    def _pb_upsert_stop_wrapper_v2(self, symbol: str, quantity: int, stop_price: float, side: str = 'SELL', client_order_id: str = '', note: str = '', strategy_name: str = '', signal_id: str = '', position_key: str = ''):
        resp=_PB_V2_ORIG_UPSERT_STOP(self, symbol, quantity, stop_price, side=side, client_order_id=client_order_id, note=note)
        try:
            if resp.get('ok'):
                rec=resp.get('record') or {}
                if strategy_name: rec['strategy_name']=strategy_name
                if signal_id: rec['signal_id']=signal_id
                if position_key: rec['position_key']=position_key
                self.protective_stops[str(rec.get('order_id') or resp.get('broker_order_id'))]=rec
                _pb_link_stop_to_lots(self, rec)
                resp['record']=dict(rec)
        except Exception as exc:
            self.lot_lifecycle_errors.append({'time': _pb_v2_now(), 'error': repr(exc), 'phase': 'upsert_stop_v2'})
        return resp

    def _pb_replace_order_wrapper_v2(self, order_id: str, payload: dict):
        resp=_PB_V2_ORIG_REPLACE_ORDER(self, order_id, payload)
        try:
            rec=self.protective_stops.get(order_id)
            if rec is not None:
                if 'strategy_name' in payload: rec['strategy_name']=payload.get('strategy_name') or rec.get('strategy_name','')
                if 'signal_id' in payload: rec['signal_id']=payload.get('signal_id') or rec.get('signal_id','')
                if 'position_key' in payload: rec['position_key']=payload.get('position_key') or rec.get('position_key','')
                _pb_link_stop_to_lots(self, rec)
                resp['record']=dict(rec)
        except Exception as exc:
            self.lot_lifecycle_errors.append({'time': _pb_v2_now(), 'error': repr(exc), 'phase': 'replace_stop_v2'})
        return resp

    def _pb_process_stops_wrapper_v2(self, price_map=None):
        before=len(getattr(self,'stop_trigger_history',[]) or [])
        events=_PB_V2_ORIG_PROCESS_STOPS2(self, price_map)
        try:
            new_events=list((getattr(self,'stop_trigger_history',[]) or [])[before:])
            for ev in new_events:
                oid=str(ev.get('order_id') or '')
                stop_rec=self.protective_stops.get(oid,{})
                side=str(ev.get('side','') or '').upper(); close_side='LONG' if side=='SELL' else 'SHORT'
                dummy={'order_id': oid, 'strategy_name': stop_rec.get('strategy_name',''), 'signal_id': stop_rec.get('signal_id',''), 'client_order_id': stop_rec.get('client_order_id','')}
                _pb_close_lots_v2(self, ev.get('symbol',''), close_side, int(ev.get('qty',0) or 0), float(ev.get('fill_price',0) or 0.0), order=dummy, fill_id=f"STOPFILL-{oid}-{len(getattr(self,'lot_fill_history',[]) or []):06d}", note='protective_stop_triggered')
                for lot in self.position_lots:
                    if str(lot.get('stop_order_id',''))==oid:
                        lot['stop_status']='TRIGGERED_FILLED'
                        if int(lot.get('remaining_qty',0) or 0)<=0:
                            lot['status']='CLOSED'; lot['lifecycle_status']='CLOSED'
        except Exception as exc:
            self.lot_lifecycle_errors.append({'time': _pb_v2_now(), 'error': repr(exc), 'phase': 'process_stop_v2'})
        return events

    def _pb_get_position_lots_v2(self, include_closed: bool = False):
        rows=[]
        for lot in self.position_lots:
            if include_closed or int(lot.get('remaining_qty',0) or 0)>0 or str(lot.get('status','')).upper()=='PARTIAL_EXIT':
                row=dict(lot)
                market_px=float(self.last_prices.get(row.get('ticker',row.get('symbol','')), row.get('avg_cost',0)) or 0.0)
                rem=int(row.get('remaining_qty',0) or 0); avg=float(row.get('avg_cost',0) or 0.0)
                row['market_price']=market_px; row['market_value']=abs(rem)*market_px; row['cost_value']=abs(rem)*avg
                row['unrealized_pnl']=round((market_px-avg)*rem if row.get('side')=='LONG' else (avg-market_px)*rem,4)
                row['realized_unrealized_total']=round(float(row.get('realized_pnl',0) or 0.0)+float(row.get('unrealized_pnl',0) or 0.0),4)
                row['is_multi_fill_entry']=int(row.get('entry_fill_count',0) or 0)>1
                row['is_partial_exit']=int(row.get('close_fill_qty',0) or 0)>0 and rem>0
                rows.append(row)
        return rows

    def _pb_get_position_lot_summary_v2(self):
        lots=_pb_get_position_lots_v2(self, include_closed=False); out={}
        for lot in lots:
            key=(lot.get('ticker'), lot.get('side'), lot.get('strategy_name',''), lot.get('signal_id',''))
            bucket=out.setdefault(key, {'ticker':key[0], 'direction_bucket':key[1], 'strategy_name':key[2], 'signal_id':key[3], 'position_key': _pb_position_key(key[0], key[1], key[2], key[3]), 'qty':0, 'market_value':0.0, 'cost_value':0.0, 'lot_count':0, 'realized_pnl':0.0, 'unrealized_pnl':0.0})
            qty=int(lot.get('remaining_qty',0) or 0)
            bucket['qty'] += qty if key[1]=='LONG' else -qty
            bucket['market_value'] += float(lot.get('market_value',0) or 0.0)
            bucket['cost_value'] += float(lot.get('cost_value',0) or 0.0)
            bucket['lot_count'] += 1
            bucket['realized_pnl'] += float(lot.get('realized_pnl',0) or 0.0)
            bucket['unrealized_pnl'] += float(lot.get('unrealized_pnl',0) or 0.0)
        for row in out.values():
            abs_qty=abs(int(row.get('qty',0) or 0)); row['avg_cost']=round(row['cost_value']/abs_qty,4) if abs_qty else 0.0; row['total_pnl']=round(float(row['realized_pnl'])+float(row['unrealized_pnl']),4)
        return list(out.values())

    def _pb_reconcile_lots_v2(self):
        lots=_pb_get_position_lots_v2(self, include_closed=False); lot_pos={}; strategy_buckets={}
        for lot in lots:
            sym=lot.get('ticker', lot.get('symbol','')); rem=int(lot.get('remaining_qty',0) or 0)
            lot_pos[sym]=lot_pos.get(sym,0)+(rem if lot.get('side')=='LONG' else -rem); strategy_buckets.setdefault(sym,set()).add(str(lot.get('strategy_name','')))
        diffs=[]
        for sym in sorted(set(lot_pos)|set(getattr(self,'positions',{}).keys())):
            agg=int(getattr(self,'positions',{}).get(sym,0) or 0); lot_qty=int(lot_pos.get(sym,0) or 0)
            if agg!=lot_qty: diffs.append({'ticker':sym,'aggregate_qty':agg,'lot_qty':lot_qty,'diff_qty':agg-lot_qty,'strategy_buckets': sorted(strategy_buckets.get(sym,set()))})
        return {'ok': len(diffs)==0, 'diffs': diffs, 'lot_count': len(lots), 'cost_basis_method': _pb_cost_method()}

    PaperBroker.__init__ = _pb_init_wrapper_v2
    PaperBroker._append_fill = _pb_append_fill_wrapper_v2
    PaperBroker.upsert_protective_stop = _pb_upsert_stop_wrapper_v2
    PaperBroker.replace_order = _pb_replace_order_wrapper_v2
    PaperBroker.process_protective_stops = _pb_process_stops_wrapper_v2
    PaperBroker.get_position_lots = _pb_get_position_lots_v2
    PaperBroker.get_position_lot_summary = _pb_get_position_lot_summary_v2
    PaperBroker.reconcile_lots_to_positions = _pb_reconcile_lots_v2
    _PB_V2_ORIG_EXPORT = PaperBroker.export_runtime_snapshot
    def _pb_export_v2(self):
        snap=_PB_V2_ORIG_EXPORT(self)
        snap['position_lots']=self.get_position_lots(include_closed=True)
        snap['position_lot_summary']=self.get_position_lot_summary()
        snap['lot_reconciliation']=self.reconcile_lots_to_positions()
        snap['lot_fill_history']=list(getattr(self,'lot_fill_history',[])[-200:])
        snap['lot_stop_link_events']=list(getattr(self,'lot_stop_link_events',[])[-100:])
        return snap
    PaperBroker.export_runtime_snapshot = _pb_export_v2
except Exception:
    pass

# =============================================================================
# vNext tax-lot jurisdiction / wash-sale / report overlay
# =============================================================================
try:
    from fts_tax_lot_accounting import (
        decorate_open_lot as _tax_decorate_open_lot,
        enrich_open_lot as _tax_enrich_open_lot,
        closure_event as _tax_closure_event,
        update_lot_after_close as _tax_update_lot_after_close,
        apply_wash_sale_adjustments as _tax_apply_wash_sale,
        summarize_tax_lots as _tax_summarize_lots,
        export_tax_reports as _tax_export_reports,
        money as _tax_money,
        qty_int as _tax_qty,
    )
    _PB_TAX_ORIG_APPEND_FILL = PaperBroker._append_fill
    _PB_TAX_ORIG_PROCESS_STOPS = PaperBroker.process_protective_stops
    _PB_TAX_ORIG_GET_LOTS = PaperBroker.get_position_lots
    _PB_TAX_ORIG_EXPORT = PaperBroker.export_runtime_snapshot

    def _pb_tax_init(self):
        if not hasattr(self, 'tax_lot_closures') or not isinstance(getattr(self, 'tax_lot_closures', None), list):
            self.tax_lot_closures = []
        if not hasattr(self, 'tax_report_exports') or not isinstance(getattr(self, 'tax_report_exports', None), dict):
            self.tax_report_exports = {}

    def _pb_tax_decorate_all_lots(self):
        _pb_tax_init(self)
        lots = []
        for lot in list(getattr(self, 'position_lots', []) or []):
            try:
                lots.append(_tax_decorate_open_lot(lot))
            except Exception as exc:
                if not hasattr(self, 'tax_lot_errors'): self.tax_lot_errors=[]
                self.tax_lot_errors.append({'phase':'decorate_lot','error':repr(exc)})
                lots.append(lot)
        self.position_lots = lots

    def _pb_tax_sync_new_closures(self, before_count: int, order=None, qty: int = 0, px: float = 0.0, commission: float = 0.0, tax: float = 0.0, note: str = ''):
        _pb_tax_init(self)
        _pb_tax_decorate_all_lots(self)
        new_events = list((getattr(self, 'lot_close_history', []) or [])[before_count:])
        total_closed = sum(_tax_qty(ev.get('closed_qty') or ev.get('qty')) for ev in new_events) or _tax_qty(qty) or 1
        for ev in new_events:
            try:
                lot = None
                for row in getattr(self, 'position_lots', []) or []:
                    if str(row.get('lot_id')) == str(ev.get('lot_id')):
                        lot = row; break
                if lot is None:
                    lot = {'lot_id': ev.get('lot_id'), 'ticker_symbol': ev.get('ticker') or ev.get('symbol'), 'side': ev.get('side'), 'entry_price': ev.get('entry_price'), 'remaining_qty': ev.get('closed_qty') or ev.get('qty'), 'open_qty': ev.get('closed_qty') or ev.get('qty')}
                closed_qty = _tax_qty(ev.get('closed_qty') or ev.get('qty'))
                ratio = closed_qty / total_closed if total_closed else 1.0
                fill_id = getattr(order, 'fill_id', '') if order is not None else ''
                exit_order_id = getattr(order, 'order_id', '') if order is not None else str(ev.get('exit_order_id') or ev.get('order_id') or '')
                event = _tax_closure_event(lot=lot, qty=closed_qty, close_price=float(ev.get('close_price') or px or 0.0), exit_order_id=exit_order_id, fill_id=fill_id or str(ev.get('fill_id','')), commission=_tax_money(float(commission or 0.0)*ratio), tax=_tax_money(float(tax or 0.0)*ratio), note=note or ev.get('note',''))
                self.tax_lot_closures.append(event)
            except Exception as exc:
                if not hasattr(self, 'tax_lot_errors'): self.tax_lot_errors=[]
                self.tax_lot_errors.append({'phase':'closure_event','error':repr(exc),'event':ev})
        try:
            adjusted, adjusted_lots = _tax_apply_wash_sale(list(getattr(self, 'tax_lot_closures', []) or []), list(getattr(self, 'position_lots', []) or []))
            self.tax_lot_closures = adjusted
            self.position_lots = adjusted_lots
        except Exception as exc:
            if not hasattr(self, 'tax_lot_errors'): self.tax_lot_errors=[]
            self.tax_lot_errors.append({'phase':'wash_sale','error':repr(exc)})

    def _pb_tax_append_fill(self, order, qty: int, px: float, commission: float, tax: float) -> None:
        _pb_tax_init(self)
        before = len(getattr(self, 'lot_close_history', []) or [])
        _PB_TAX_ORIG_APPEND_FILL(self, order, qty, px, commission, tax)
        _pb_tax_decorate_all_lots(self)
        _pb_tax_sync_new_closures(self, before, order=order, qty=qty, px=px, commission=commission, tax=tax, note='paper_fill_tax_lot')

    def _pb_tax_process_stops(self, price_map=None):
        _pb_tax_init(self)
        before = len(getattr(self, 'lot_close_history', []) or [])
        events = _PB_TAX_ORIG_PROCESS_STOPS(self, price_map)
        _pb_tax_sync_new_closures(self, before, order=None, qty=0, px=0.0, commission=0.0, tax=0.0, note='protective_stop_tax_lot')
        return events

    def _pb_tax_get_lots(self, include_closed: bool = False):
        _pb_tax_decorate_all_lots(self)
        rows = _PB_TAX_ORIG_GET_LOTS(self, include_closed=include_closed)
        out=[]
        for row in rows:
            try:
                px = getattr(self, 'last_prices', {}).get(row.get('ticker') or row.get('symbol') or row.get('ticker_symbol'))
                out.append(_tax_enrich_open_lot(row, market_price=px))
            except Exception:
                out.append(row)
        return out

    def _pb_tax_export(self):
        snap = _PB_TAX_ORIG_EXPORT(self)
        _pb_tax_decorate_all_lots(self)
        closures = list(getattr(self, 'tax_lot_closures', []) or [])
        lots = _pb_tax_get_lots(self, include_closed=True)
        try:
            adjusted, adjusted_lots = _tax_apply_wash_sale(closures, lots)
            closures, lots = adjusted, adjusted_lots
            self.tax_lot_closures = closures
            self.position_lots = lots
        except Exception:
            pass
        snap['tax_lot_closures'] = closures
        snap['tax_lot_summary'] = _tax_summarize_lots(closures, lots)
        snap['tax_lot_accounting'] = {'engine': 'fts_tax_lot_accounting', 'wash_sale_enabled': True, 'report_export_enabled': True}
        try:
            self.tax_report_exports = _tax_export_reports(closures, lots)
            snap['tax_report_exports'] = self.tax_report_exports
        except Exception as exc:
            snap['tax_report_export_error'] = repr(exc)
        return snap

    PaperBroker._append_fill = _pb_tax_append_fill
    PaperBroker.process_protective_stops = _pb_tax_process_stops
    PaperBroker.get_position_lots = _pb_tax_get_lots
    PaperBroker.export_runtime_snapshot = _pb_tax_export
except Exception:
    pass


# =============================================================================
# Formal class facade
# =============================================================================
# Historical update packs extended PaperBroker through patch blocks.  The public
# symbol below is now a formal subclass layer so factories/importers can depend
# on a stable class boundary.
_PatchedPaperBrokerBase = PaperBroker
class FormalPaperBroker(_PatchedPaperBrokerBase):
    FORMAL_CLASS_LAYER = True
    LOT_LIFECYCLE_LAYER = "formalized"
    TAX_LOT_LAYER = "formalized"

PaperBroker = FormalPaperBroker
