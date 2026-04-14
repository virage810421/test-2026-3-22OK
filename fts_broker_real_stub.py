# -*- coding: utf-8 -*-
from __future__ import annotations

import uuid
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Tuple

from fts_broker_interface import BrokerBase
from fts_config import CONFIG
from fts_models import AccountSnapshot, Fill, Order, OrderSide, OrderStatus, Position
from fts_utils import now_str, round_price

try:
    from fts_callback_event_store import CallbackEventStore  # type: ignore
except Exception:  # pragma: no cover
    CallbackEventStore = None  # type: ignore


class RealBrokerStub(BrokerBase):
    """
    Phase-2 mock broker.
    名字仍叫 RealBrokerStub，是為了讓你現有 broker factory 不用重寫，
    但它現在已經能做「假真券商」測試：
    - 模擬 connect / auth / place / cancel / replace
    - 產生 broker_order_id / callback event
    - 模擬 partial fill / reject / fill
    - 提供 orders / fills / positions / cash snapshot
    """

    MODULE_VERSION = "v82_mock_real_broker"

    def __init__(self, credentials: dict[str, Any] | None = None):
        self.credentials = credentials or {}
        self._positions: Dict[str, Position] = {}
        self._cash = float(getattr(CONFIG, "starting_cash", 0.0))
        self._last_prices: Dict[str, float] = {}
        self._orders: Dict[str, dict[str, Any]] = {}
        self._fills: List[dict[str, Any]] = []
        self._callbacks: List[dict[str, Any]] = []
        self._protective_stops: Dict[str, dict[str, Any]] = {}
        self._connected = False
        self._session_id = f"SIM-{uuid.uuid4().hex[:8].upper()}"
        self._event_store = CallbackEventStore() if CallbackEventStore is not None else None

    def restore_state(self, cash, positions, last_prices=None):
        self._cash = float(cash)
        self._positions = positions or {}
        self._last_prices = last_prices or {}

    def update_market_price(self, ticker: str, price: float):
        if price and float(price) > 0:
            self._last_prices[str(ticker).strip().upper()] = float(price)

    # ---------------------------
    # Broker-like API
    # ---------------------------
    def connect(self) -> dict[str, Any]:
        self._connected = True
        payload = {
            "ok": True,
            "status": "connected",
            "session_id": self._session_id,
            "broker_mode": "mock_real",
            "production_ready": False,
            "missing_for_true_broker": [
                'real_sdk_binding',
                'real_callback_wireup',
                'broker_ledger_reconcile',
                'credential_rotation',
            ],
            "connected_at": now_str(),
        }
        return payload

    def capability_report(self) -> dict[str, Any]:
        return {
            'module_version': self.MODULE_VERSION,
            'broker_kind': 'stub',
            'paper_prelive_ready': True,
            'true_broker_ready': False,
            'missing_for_true_broker': [
                'real_sdk_binding',
                'real_callback_wireup',
                'broker_ledger_reconcile',
                'credential_rotation',
            ],
        }

    def refresh_auth(self) -> dict[str, Any]:
        return {
            "ok": True,
            "status": "auth_refreshed",
            "session_id": self._session_id,
            "refreshed_at": now_str(),
        }

    def disconnect(self) -> dict[str, Any]:
        self._connected = False
        return {
            "ok": True,
            "status": "disconnected",
            "session_id": self._session_id,
            "disconnected_at": now_str(),
        }

    def poll_callbacks(self, clear: bool = False) -> list[dict[str, Any]]:
        out = list(self._callbacks)
        if clear:
            self._callbacks = []
        return out

    def snapshot_orders(self) -> list[dict[str, Any]]:
        return list(self._orders.values())

    def snapshot_fills(self) -> list[dict[str, Any]]:
        return list(self._fills)

    def get_cash(self) -> dict[str, Any]:
        market_value = 0.0
        for pos in self._positions.values():
            px = self._last_prices.get(pos.ticker, pos.avg_cost)
            market_value += abs(px * pos.qty)
        equity = self._cash + market_value
        return {
            "cash_available": round(self._cash, 2),
            "market_value": round(market_value, 2),
            "equity": round(equity, 2),
            "buying_power": round(self._cash, 2),
            "updated_at": now_str(),
        }


    def get_protective_stops(self) -> list[dict[str, Any]]:
        return [dict(v) for v in self._protective_stops.values()]

    def get_open_orders_dicts(self) -> list[dict[str, Any]]:
        return [dict(v) for v in self._orders.values() if str(v.get('status', '')).upper() in {'SUBMITTED', 'PARTIALLY_FILLED'}]

    def get_fill_history_dicts(self) -> list[dict[str, Any]]:
        return [dict(v) for v in self._fills]

    def get_positions_detailed(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for pos in self._positions.values():
            ticker = getattr(pos, 'ticker', '')
            qty = int(getattr(pos, 'qty', 0) or 0)
            avg_cost = float(getattr(pos, 'avg_cost', 0) or 0)
            market_px = float(self._last_prices.get(ticker, avg_cost) or avg_cost or 0.0)
            market_value = abs(qty) * market_px
            unrealized = (market_px - avg_cost) * qty if qty >= 0 else (avg_cost - market_px) * abs(qty)
            rows.append({
                'ticker': ticker,
                'qty': qty,
                'available_qty': abs(qty),
                'avg_cost': avg_cost,
                'market_price': market_px,
                'market_value': market_value,
                'unrealized_pnl': round(unrealized, 4),
                'realized_pnl': 0.0,
                'direction_bucket': 'LONG' if qty >= 0 else 'SHORT',
                'strategy_name': getattr(pos, 'lifecycle_note', '') or 'mock_real_runtime',
                'industry': getattr(pos, 'industry', '未知'),
                'note': getattr(pos, 'lifecycle_note', ''),
            })
        return rows

    def export_runtime_snapshot(self) -> dict[str, Any]:
        snap = self.get_cash()
        return {
            'cash': snap.get('cash_available', 0.0),
            'positions': [dict(x) for x in self.get_positions_rows()],
            'open_orders': [dict(x) for x in self.snapshot_orders()],
            'fills': [dict(x) for x in self.get_fill_history_dicts()[-20:]],
            'protective_stops': self.get_protective_stops(),
        }

    def get_positions_rows(self) -> list[dict[str, Any]]:
        rows = []
        for pos in self._positions.values():
            row = asdict(pos) if is_dataclass(pos) else dict(pos)
            rows.append(row)
        return rows

    def get_order_status(self, broker_order_id: str) -> dict[str, Any]:
        return dict(self._orders.get(str(broker_order_id), {}))

    def get_fills(self, trading_date: str | None = None) -> list[dict[str, Any]]:
        if not trading_date:
            return list(self._fills)
        return [x for x in self._fills if str(x.get("fill_time", "")).startswith(str(trading_date))]

    def poll_fills(self) -> list[dict[str, Any]]:
        fills = list(self._fills)
        self._fills = []
        return fills

    def process_protective_stops(self, price_map: dict[str, float] | None = None) -> list[dict[str, Any]]:
        for ticker, price in (price_map or {}).items():
            self.update_market_price(ticker, price)
        events: list[dict[str, Any]] = []
        for broker_order_id, rec in list(self._protective_stops.items()):
            if str(rec.get('status', 'SUBMITTED')).upper() not in {'SUBMITTED', 'WORKING'}:
                continue
            ticker = str(rec.get('ticker', '')).strip().upper()
            market_px = float(self._last_prices.get(ticker, 0) or 0)
            stop_px = float(rec.get('stop_price', 0) or 0)
            qty = int(rec.get('qty', 0) or 0)
            if not ticker or market_px <= 0 or stop_px <= 0 or qty <= 0:
                continue
            pos = self._positions.get(ticker)
            pos_qty = int(getattr(pos, 'qty', 0) or 0)
            side = str(rec.get('side', 'SELL')).upper()
            if side == 'SELL':
                triggered = pos_qty > 0 and market_px <= stop_px
                fill_side = 'SELL'
                trigger_px = min(market_px, stop_px)
            else:
                triggered = pos_qty < 0 and market_px >= stop_px
                fill_side = 'BUY'
                trigger_px = max(market_px, stop_px)
            if not triggered:
                continue
            fill_qty = min(abs(pos_qty), qty)
            order_row = {
                'broker_order_id': broker_order_id,
                'client_order_id': rec.get('client_order_id', broker_order_id),
                'ticker': ticker,
                'side': fill_side,
                'qty': fill_qty,
                'filled_qty': 0,
                'remaining_qty': fill_qty,
                'submitted_price': trigger_px,
                'status': 'SUBMITTED',
                'strategy_name': 'protective_stop',
                'industry': getattr(pos, 'industry', '未知') if pos is not None else '未知',
                'created_at': now_str(),
                'updated_at': now_str(),
                'avg_fill_price': 0.0,
            }
            self._apply_fill(order_row, fill_qty=fill_qty, fill_price=trigger_px)
            rec['status'] = 'TRIGGERED_FILLED'
            rec['updated_at'] = now_str()
            rec['filled_qty'] = fill_qty
            rec['trigger_fill_price'] = round_price(trigger_px)
            event = {'order_id': broker_order_id, 'symbol': ticker, 'side': fill_side, 'qty': fill_qty, 'stop_price': stop_px, 'trigger_price': trigger_px, 'fill_price': trigger_px, 'status': 'TRIGGERED_FILLED', 'time': now_str()}
            self._record_callback({
                'broker_order_id': broker_order_id,
                'client_order_id': rec.get('client_order_id', broker_order_id),
                'event_type': 'STOP_TRIGGERED',
                'status': 'FILLED',
                'symbol': ticker,
                'filled_qty': fill_qty,
                'remaining_qty': 0,
                'timestamp': now_str(),
                'avg_fill_price': trigger_px,
            })
            events.append(event)
        return events


    def upsert_protective_stop(self, symbol: str, quantity: int, stop_price: float, side: str = "SELL", client_order_id: str = "", note: str = "") -> dict[str, Any]:
        if not symbol or quantity <= 0 or stop_price <= 0:
            return {'ok': False, 'status': 'invalid_stop_payload', 'symbol': symbol}
        broker_order_id = client_order_id or f'STOP-{symbol}-{len(self._protective_stops)+1:04d}'
        rec = self._protective_stops.get(broker_order_id, {})
        rec.update({'broker_order_id': broker_order_id, 'ticker': symbol, 'qty': int(quantity), 'stop_price': round_price(float(stop_price)), 'side': str(side).upper(), 'status': 'SUBMITTED', 'updated_at': now_str(), 'note': str(note or '')})
        self._protective_stops[broker_order_id] = rec
        return {'ok': True, 'status': 'protective_stop_upserted', 'broker_order_id': broker_order_id, 'updated_order': dict(rec)}

    def replace_order(self, broker_order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        order = self._orders.get(str(broker_order_id))
        if not order:
            stop_rec = self._protective_stops.get(str(broker_order_id))
            if stop_rec is not None:
                if 'stop_price' in payload:
                    stop_rec['stop_price'] = round_price(float(payload['stop_price']))
                if 'qty' in payload:
                    stop_rec['qty'] = int(payload['qty'])
                stop_rec['updated_at'] = now_str()
                stop_rec['note'] = str(payload.get('note', stop_rec.get('note', '')))
                return {'ok': True, 'status': 'replace_ok', 'broker_order_id': broker_order_id, 'updated_order': dict(stop_rec)}
            return {"ok": False, "status": "missing_order", "broker_order_id": broker_order_id}
        if order.get("status") not in {"SUBMITTED", "PARTIALLY_FILLED"}:
            return {"ok": False, "status": "replace_blocked", "broker_order_id": broker_order_id, "reason": "order_not_working"}
        if "price" in payload:
            order["submitted_price"] = round_price(float(payload["price"]))
        if "stop_price" in payload:
            order["submitted_price"] = round_price(float(payload["stop_price"]))
        if "qty" in payload:
            order["qty"] = int(payload["qty"])
            order["remaining_qty"] = max(order["qty"] - int(order.get("filled_qty", 0) or 0), 0)
        order["updated_at"] = now_str()
        self._record_callback({
            "broker_order_id": order["broker_order_id"],
            "client_order_id": order["client_order_id"],
            "event_type": "REPLACED",
            "status": order["status"],
            "symbol": order["ticker"],
            "filled_qty": order.get("filled_qty", 0),
            "remaining_qty": order.get("remaining_qty", 0),
            "timestamp": now_str(),
            "avg_fill_price": order.get("avg_fill_price", 0.0),
        })
        return {"ok": True, "status": "replace_ok", "broker_order_id": broker_order_id, "updated_order": dict(order)}

    def cancel_order(self, order):
        broker_order_id = getattr(order, "broker_order_id", None) or getattr(order, "order_id", None) or str(order)
        payload = self._orders.get(str(broker_order_id))
        if not payload:
            if hasattr(order, "status"):
                order.status = OrderStatus.CANCELLED
                order.updated_at = now_str()
                return order
            return {"ok": False, "status": "missing_order", "broker_order_id": broker_order_id}
        payload["status"] = "CANCELLED"
        payload["updated_at"] = now_str()
        payload["remaining_qty"] = 0
        self._record_callback({
            "broker_order_id": payload["broker_order_id"],
            "client_order_id": payload["client_order_id"],
            "event_type": "CANCELLED",
            "status": "CANCELLED",
            "symbol": payload["ticker"],
            "filled_qty": payload.get("filled_qty", 0),
            "remaining_qty": 0,
            "timestamp": now_str(),
            "avg_fill_price": payload.get("avg_fill_price", 0.0),
        })
        if hasattr(order, "status"):
            order.status = OrderStatus.CANCELLED
            order.updated_at = now_str()
            return order
        return {"ok": True, "status": "cancelled", "broker_order_id": broker_order_id}

    # ---------------------------
    # BrokerBase-compatible API
    # ---------------------------
    def place_order(self, order):
        if getattr(CONFIG, "mock_broker_auto_connect", True) and not self._connected:
            self.connect()
        if hasattr(order, "ticker") and hasattr(order, "qty"):
            return self._place_order_object(order)
        return self._place_order_payload(dict(order))

    def get_positions(self):
        return self._positions

    def get_account_snapshot(self):
        snap = self.get_cash()
        return AccountSnapshot(
            cash=snap["cash_available"],
            market_value=snap["market_value"],
            equity=snap["equity"],
            exposure_ratio=round((snap["market_value"] / snap["equity"]) if snap["equity"] > 0 else 0.0, 6),
            updated_at=snap["updated_at"],
        )

    # ---------------------------
    # Internal helpers
    # ---------------------------
    def _place_order_object(self, order: Order):
        result = self._place_order_payload({
            "ticker": order.ticker,
            "side": order.side.value if hasattr(order.side, "value") else str(order.side),
            "qty": int(order.qty),
            "price": float(order.submitted_price or order.ref_price or 0),
            "ref_price": float(order.ref_price or order.submitted_price or 0),
            "client_order_id": order.order_id,
            "strategy_name": order.strategy_name,
            "industry": order.industry,
            "model_name": order.model_name,
            "model_version": order.model_version,
            "regime": order.regime,
        })
        mapped = self._orders[result["broker_order_id"]]
        status_map = {
            "SUBMITTED": OrderStatus.SUBMITTED,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "REJECTED": OrderStatus.REJECTED,
            "CANCELLED": OrderStatus.CANCELLED,
        }
        order.status = status_map.get(mapped["status"], OrderStatus.SUBMITTED)
        order.updated_at = now_str()
        order.submitted_price = float(mapped.get("submitted_price", order.submitted_price))
        order.note = result.get("note", "")
        fill_rows = [x for x in self._fills if x.get("order_id") == order.order_id]
        fills = [
            Fill(
                fill_id=x["fill_id"],
                order_id=x["order_id"],
                ticker=x["ticker"],
                side=OrderSide(x["side"]),
                fill_qty=int(x["fill_qty"]),
                fill_price=float(x["fill_price"]),
                commission=float(x["commission"]),
                tax=float(x["tax"]),
                fill_time=x["fill_time"],
            )
            for x in fill_rows
        ]
        return order, fills

    def _place_order_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        ticker = str(payload.get("ticker", "")).strip().upper()
        side = str(payload.get("side", "BUY")).strip().upper()
        qty = int(payload.get("qty", 0) or 0)
        ref_price = float(payload.get("ref_price", payload.get("price", 0)) or 0)
        submitted_price = round_price(float(payload.get("price", ref_price) or 0))
        client_order_id = str(payload.get("client_order_id") or f"CLIENT-{uuid.uuid4().hex[:10].upper()}")
        broker_order_id = f"BRK-{uuid.uuid4().hex[:10].upper()}"
        industry = str(payload.get("industry", "未知") or "未知")
        strategy_name = str(payload.get("strategy_name", "mock_real_strategy") or "mock_real_strategy")

        order_row = {
            "broker_order_id": broker_order_id,
            "client_order_id": client_order_id,
            "order_id": client_order_id,
            "ticker": ticker,
            "side": side,
            "qty": qty,
            "filled_qty": 0,
            "remaining_qty": qty,
            "submitted_price": submitted_price,
            "status": "NEW",
            "strategy_name": strategy_name,
            "industry": industry,
            "model_name": str(payload.get("model_name", "")),
            "model_version": str(payload.get("model_version", "")),
            "regime": str(payload.get("regime", "")),
            "created_at": now_str(),
            "updated_at": now_str(),
            "avg_fill_price": 0.0,
        }
        self._orders[broker_order_id] = order_row
        self._record_callback({
            "broker_order_id": broker_order_id,
            "client_order_id": client_order_id,
            "event_type": "ACK",
            "status": "SUBMITTED",
            "symbol": ticker,
            "filled_qty": 0,
            "remaining_qty": qty,
            "timestamp": now_str(),
        })
        order_row["status"] = "SUBMITTED"

        reject_reason = self._check_reject_reason(side, ticker, qty, submitted_price)
        if reject_reason:
            order_row["status"] = "REJECTED"
            order_row["updated_at"] = now_str()
            self._record_callback({
                "broker_order_id": broker_order_id,
                "client_order_id": client_order_id,
                "event_type": "REJECT",
                "status": "REJECTED",
                "symbol": ticker,
                "filled_qty": 0,
                "remaining_qty": qty,
                "timestamp": now_str(),
                "reject_reason": reject_reason,
            })
            return {
                "ok": False,
                "status": "REJECTED",
                "broker_order_id": broker_order_id,
                "client_order_id": client_order_id,
                "note": reject_reason,
            }

        partial_threshold = int(getattr(CONFIG, "mock_broker_partial_fill_threshold_lots", 2)) * int(getattr(CONFIG, "lot_size", 1000))
        if qty >= partial_threshold:
            first_fill = max(int(getattr(CONFIG, "lot_size", 1000)), (qty // 2 // int(getattr(CONFIG, "lot_size", 1000))) * int(getattr(CONFIG, "lot_size", 1000)))
            first_fill = min(first_fill, qty)
            self._apply_fill(order_row, fill_qty=first_fill, fill_price=submitted_price)
            order_row["status"] = "PARTIALLY_FILLED" if order_row["remaining_qty"] > 0 else "FILLED"
            self._record_callback({
                "broker_order_id": broker_order_id,
                "client_order_id": client_order_id,
                "event_type": "PARTIAL_FILL" if order_row["remaining_qty"] > 0 else "FILL",
                "status": order_row["status"],
                "symbol": ticker,
                "filled_qty": order_row["filled_qty"],
                "remaining_qty": order_row["remaining_qty"],
                "timestamp": now_str(),
                "avg_fill_price": order_row["avg_fill_price"],
            })
            return {
                "ok": True,
                "status": order_row["status"],
                "broker_order_id": broker_order_id,
                "client_order_id": client_order_id,
                "note": "mock partial fill created",
            }

        self._apply_fill(order_row, fill_qty=qty, fill_price=submitted_price)
        order_row["status"] = "FILLED"
        self._record_callback({
            "broker_order_id": broker_order_id,
            "client_order_id": client_order_id,
            "event_type": "FILL",
            "status": "FILLED",
            "symbol": ticker,
            "filled_qty": order_row["filled_qty"],
            "remaining_qty": order_row["remaining_qty"],
            "timestamp": now_str(),
            "avg_fill_price": order_row["avg_fill_price"],
        })
        return {
            "ok": True,
            "status": "FILLED",
            "broker_order_id": broker_order_id,
            "client_order_id": client_order_id,
            "note": "mock fill complete",
        }

    def finalize_open_orders(self) -> list[dict[str, Any]]:
        completed = []
        for broker_order_id, order_row in list(self._orders.items()):
            if order_row.get("status") != "PARTIALLY_FILLED":
                continue
            remain = int(order_row.get("remaining_qty", 0) or 0)
            if remain <= 0:
                continue
            self._apply_fill(order_row, fill_qty=remain, fill_price=float(order_row.get("submitted_price", 0) or 0))
            order_row["status"] = "FILLED"
            order_row["updated_at"] = now_str()
            event = {
                "broker_order_id": broker_order_id,
                "client_order_id": order_row["client_order_id"],
                "event_type": "FILL",
                "status": "FILLED",
                "symbol": order_row["ticker"],
                "filled_qty": order_row["filled_qty"],
                "remaining_qty": 0,
                "timestamp": now_str(),
                "avg_fill_price": order_row["avg_fill_price"],
            }
            self._record_callback(event)
            completed.append(dict(order_row))
        return completed

    def _apply_fill(self, order_row: dict[str, Any], fill_qty: int, fill_price: float) -> None:
        fill_qty = int(fill_qty)
        fill_price = round_price(fill_price)
        side = str(order_row["side"]).upper()
        gross = fill_price * fill_qty
        commission = round(gross * float(getattr(CONFIG, "commission_rate", 0.0)), 2)
        tax = round(gross * float(getattr(CONFIG, "tax_rate_sell", 0.0)), 2) if side == "SELL" else 0.0

        if side == "BUY":
            total_cost = gross + commission + tax
            self._cash -= total_cost
            pos = self._positions.get(order_row["ticker"])
            if pos:
                new_qty = pos.qty + fill_qty
                new_avg = ((pos.avg_cost * pos.qty) + gross + commission) / max(new_qty, 1)
                pos.qty = new_qty
                pos.avg_cost = round_price(new_avg)
                pos.updated_at = now_str()
                pos.highest_price = max(float(pos.highest_price or 0), fill_price)
            else:
                self._positions[order_row["ticker"]] = Position(
                    ticker=order_row["ticker"],
                    qty=fill_qty,
                    avg_cost=round_price((gross + commission) / max(fill_qty, 1)),
                    industry=order_row.get("industry", "未知"),
                    updated_at=now_str(),
                    stop_loss_price=round_price(fill_price * (1 - float(getattr(CONFIG, "default_stop_loss_pct", 0.04)))),
                    take_profit_price=round_price(fill_price * (1 + float(getattr(CONFIG, "default_take_profit_pct", 0.12)))),
                    highest_price=fill_price,
                    cooldown_until=0,
                    entry_bar=int(getattr(CONFIG, "current_bar_index", 0)),
                    partial_tp_done=False,
                    add_on_count=0,
                    lifecycle_note="mock_real_fill_open",
                )
        else:
            pos = self._positions.get(order_row["ticker"])
            if pos:
                self._cash += gross - commission - tax
                pos.qty -= fill_qty
                pos.updated_at = now_str()
                pos.lifecycle_note = "mock_real_fill_close"
                if pos.qty <= 0:
                    self._positions.pop(order_row["ticker"], None)

        order_row["filled_qty"] = int(order_row.get("filled_qty", 0) or 0) + fill_qty
        order_row["remaining_qty"] = max(int(order_row["qty"]) - int(order_row["filled_qty"]), 0)
        order_row["updated_at"] = now_str()
        order_row["avg_fill_price"] = fill_price if not order_row.get("avg_fill_price") else round_price((float(order_row["avg_fill_price"]) + fill_price) / 2.0)

        fill_row = {
            "fill_id": f"FILL-{uuid.uuid4().hex[:10].upper()}",
            "order_id": order_row["client_order_id"],
            "broker_order_id": order_row["broker_order_id"],
            "ticker": order_row["ticker"],
            "side": side,
            "fill_qty": fill_qty,
            "fill_price": fill_price,
            "commission": commission,
            "tax": tax,
            "fill_time": now_str(),
        }
        self._fills.append(fill_row)
        self._last_prices[order_row["ticker"]] = fill_price

    def _check_reject_reason(self, side: str, ticker: str, qty: int, price: float) -> str | None:
        if not self._connected:
            return "broker_not_connected"
        if not ticker:
            return "missing_ticker"
        if qty <= 0 or price <= 0:
            return "invalid_qty_or_price"
        notional = float(qty) * float(price)
        if notional > float(getattr(CONFIG, "mock_broker_reject_notional", 900000)):
            return "mock_risk_reject_notional"
        if side == "BUY":
            total_cost = notional * (1 + float(getattr(CONFIG, "commission_rate", 0.0)))
            if total_cost > self._cash:
                return "insufficient_cash"
        if side == "SELL":
            pos = self._positions.get(ticker)
            if pos is None or pos.qty < qty:
                return "insufficient_position"
        return None

    def _record_callback(self, event: dict[str, Any]) -> None:
        self._callbacks.append(dict(event))
        if self._event_store is not None:
            try:
                self._event_store.record(event)
            except Exception:
                pass

# =============================================================================
# vNext callback / reconciliation / lot-level extension for mock real broker
# =============================================================================
try:
    _RBS_ORIG_INIT = RealBrokerStub.__init__
    _RBS_ORIG_APPLY_FILL = RealBrokerStub._apply_fill
    _RBS_ORIG_RECORD_CALLBACK = RealBrokerStub._record_callback

    def _rbs_lot_now() -> str:
        return now_str()

    def _rbs_init_lot_book(self) -> None:
        if not hasattr(self, '_position_lots') or self._position_lots is None:
            self._position_lots = []
        if not hasattr(self, '_lot_seq'):
            self._lot_seq = 0
        if not hasattr(self, '_callback_handlers') or self._callback_handlers is None:
            self._callback_handlers = []
        if not hasattr(self, '_callback_cursor'):
            self._callback_cursor = 0
        if not hasattr(self, '_lot_close_history'):
            self._lot_close_history = []

    def _rbs_patched_init(self, *args, **kwargs):
        _RBS_ORIG_INIT(self, *args, **kwargs)
        _rbs_init_lot_book(self)

    def _rbs_next_lot_id(self, ticker: str) -> str:
        _rbs_init_lot_book(self)
        self._lot_seq += 1
        return f"RLOT-{str(ticker).replace('.', '')}-{self._lot_seq:06d}"

    def _rbs_open_lot(self, ticker: str, side: str, qty: int, price: float, order_row: dict) -> None:
        _rbs_init_lot_book(self)
        qty = int(qty or 0)
        if qty <= 0:
            return
        self._position_lots.append({
            'lot_id': _rbs_next_lot_id(self, ticker),
            'ticker': str(ticker),
            'symbol': str(ticker),
            'side': str(side).upper(),
            'direction_bucket': str(side).upper(),
            'open_qty': qty,
            'remaining_qty': qty,
            'avg_cost': float(price or 0.0),
            'entry_price': float(price or 0.0),
            'entry_time': _rbs_lot_now(),
            'entry_order_id': str(order_row.get('broker_order_id') or order_row.get('client_order_id') or ''),
            'client_order_id': str(order_row.get('client_order_id') or ''),
            'strategy_name': str(order_row.get('strategy_name') or ''),
            'status': 'OPEN',
            'realized_pnl': 0.0,
            'close_qty': 0,
            'close_price': 0.0,
            'close_time': '',
        })

    def _rbs_close_lots_fifo(self, ticker: str, close_side: str, qty: int, price: float, order_row: dict) -> None:
        _rbs_init_lot_book(self)
        remaining = int(qty or 0)
        if remaining <= 0:
            return
        for lot in self._position_lots:
            if remaining <= 0:
                break
            if str(lot.get('ticker', '')).upper() != str(ticker).upper():
                continue
            if str(lot.get('side', '')).upper() != str(close_side).upper():
                continue
            if str(lot.get('status', '')).upper() != 'OPEN':
                continue
            lot_qty = int(lot.get('remaining_qty', 0) or 0)
            if lot_qty <= 0:
                continue
            take = min(lot_qty, remaining)
            entry = float(lot.get('avg_cost', 0) or 0.0)
            pnl = (float(price) - entry) * take if close_side == 'LONG' else (entry - float(price)) * take
            lot['remaining_qty'] = lot_qty - take
            lot['close_qty'] = int(lot.get('close_qty', 0) or 0) + take
            lot['realized_pnl'] = round(float(lot.get('realized_pnl', 0) or 0) + pnl, 4)
            lot['close_price'] = float(price or 0.0)
            lot['close_time'] = _rbs_lot_now()
            lot['exit_order_id'] = str(order_row.get('broker_order_id') or order_row.get('client_order_id') or '')
            if lot['remaining_qty'] <= 0:
                lot['status'] = 'CLOSED'
            self._lot_close_history.append({'lot_id': lot.get('lot_id'), 'ticker': ticker, 'side': close_side, 'closed_qty': take, 'realized_pnl': round(pnl, 4), 'exit_order_id': lot.get('exit_order_id'), 'closed_at': _rbs_lot_now()})
            remaining -= take

    def _rbs_apply_lot_fill(self, order_row: dict, fill_qty: int, fill_price: float) -> None:
        side = str(order_row.get('side', '')).upper()
        ticker = str(order_row.get('ticker', '')).upper()
        if side == 'BUY':
            _rbs_open_lot(self, ticker, 'LONG', int(fill_qty), float(fill_price), order_row)
        elif side == 'SHORT':
            _rbs_open_lot(self, ticker, 'SHORT', int(fill_qty), float(fill_price), order_row)
        elif side == 'SELL':
            _rbs_close_lots_fifo(self, ticker, 'LONG', int(fill_qty), float(fill_price), order_row)
        elif side in {'COVER', 'BUY_TO_COVER'}:
            _rbs_close_lots_fifo(self, ticker, 'SHORT', int(fill_qty), float(fill_price), order_row)

    def _rbs_patched_apply_fill(self, order_row: dict[str, Any], fill_qty: int, fill_price: float) -> None:
        _RBS_ORIG_APPLY_FILL(self, order_row, fill_qty, fill_price)
        try:
            _rbs_apply_lot_fill(self, order_row, fill_qty, fill_price)
        except Exception as exc:
            if not hasattr(self, '_lot_errors'):
                self._lot_errors = []
            self._lot_errors.append({'time': _rbs_lot_now(), 'error': repr(exc)})

    def _rbs_patched_record_callback(self, event: dict[str, Any]) -> None:
        _RBS_ORIG_RECORD_CALLBACK(self, event)
        _rbs_init_lot_book(self)
        for handler in list(getattr(self, '_callback_handlers', []) or []):
            try:
                handler(dict(event))
            except Exception:
                pass

    def _rbs_register_callback_handler(self, handler) -> dict[str, Any]:
        _rbs_init_lot_book(self)
        if callable(handler):
            self._callback_handlers.append(handler)
            return {'ok': True, 'status': 'callback_handler_registered', 'handler_count': len(self._callback_handlers)}
        return {'ok': False, 'status': 'handler_not_callable'}

    def _rbs_drain_new_callbacks(self) -> list[dict[str, Any]]:
        _rbs_init_lot_book(self)
        events = list(self._callbacks[self._callback_cursor:])
        self._callback_cursor = len(self._callbacks)
        return events

    def _rbs_get_position_lots(self, include_closed: bool = False) -> list[dict[str, Any]]:
        _rbs_init_lot_book(self)
        rows = []
        for lot in self._position_lots:
            if include_closed or str(lot.get('status', '')).upper() == 'OPEN':
                row = dict(lot)
                px = float(self._last_prices.get(row.get('ticker', ''), row.get('avg_cost', 0)) or 0.0)
                qty = int(row.get('remaining_qty', 0) or 0)
                avg = float(row.get('avg_cost', 0) or 0.0)
                row['market_price'] = px
                row['market_value'] = abs(qty) * px
                row['unrealized_pnl'] = round((px - avg) * qty if row.get('side') == 'LONG' else (avg - px) * qty, 4)
                rows.append(row)
        return rows

    def _rbs_reconcile_lots_to_positions(self) -> dict[str, Any]:
        lots = _rbs_get_position_lots(self, include_closed=False)
        lot_pos: dict[str, int] = {}
        for lot in lots:
            sym = str(lot.get('ticker', '')).upper()
            rem = int(lot.get('remaining_qty', 0) or 0)
            lot_pos[sym] = lot_pos.get(sym, 0) + (rem if lot.get('side') == 'LONG' else -rem)
        diffs = []
        symbols = set(lot_pos) | set(getattr(self, '_positions', {}).keys())
        for sym in sorted(symbols):
            p = self._positions.get(sym)
            agg = int(getattr(p, 'qty', 0) or 0) if p is not None else 0
            lot_qty = int(lot_pos.get(sym, 0) or 0)
            if agg != lot_qty:
                diffs.append({'ticker': sym, 'aggregate_qty': agg, 'lot_qty': lot_qty, 'diff_qty': agg - lot_qty})
        return {'ok': len(diffs) == 0, 'diffs': diffs, 'lot_count': len(lots)}

    def _rbs_export_runtime_snapshot(self) -> dict[str, Any]:
        snap = RealBrokerStub._orig_export_runtime_snapshot(self) if hasattr(RealBrokerStub, '_orig_export_runtime_snapshot') else {}
        snap['position_lots'] = _rbs_get_position_lots(self, include_closed=True)
        snap['lot_reconciliation'] = _rbs_reconcile_lots_to_positions(self)
        snap['callbacks'] = list(getattr(self, '_callbacks', [])[-50:])
        return snap

    RealBrokerStub.__init__ = _rbs_patched_init
    RealBrokerStub._apply_fill = _rbs_patched_apply_fill
    RealBrokerStub._record_callback = _rbs_patched_record_callback
    RealBrokerStub.register_callback_handler = _rbs_register_callback_handler
    RealBrokerStub.drain_new_callbacks = _rbs_drain_new_callbacks
    RealBrokerStub.get_position_lots = _rbs_get_position_lots
    RealBrokerStub.reconcile_lots_to_positions = _rbs_reconcile_lots_to_positions
    if not hasattr(RealBrokerStub, '_orig_export_runtime_snapshot'):
        RealBrokerStub._orig_export_runtime_snapshot = RealBrokerStub.export_runtime_snapshot
    RealBrokerStub.export_runtime_snapshot = _rbs_export_runtime_snapshot

except Exception:
    pass
