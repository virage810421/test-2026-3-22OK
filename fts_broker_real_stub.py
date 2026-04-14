# -*- coding: utf-8 -*-
from __future__ import annotations

import uuid
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Tuple

from fts_broker_interface import BrokerBase
from fts_config import CONFIG
from fts_models import AccountSnapshot, Fill, Order, OrderSide, OrderStatus, Position
from fts_utils import now_str, round_price
from fts_exception_policy import record_diagnostic
from fts_broker_callback_mapping import normalize_broker_callback

try:
    from fts_execution_models import CallbackEventStore  # type: ignore
except ImportError as exc:  # pragma: no cover - optional runtime component
    record_diagnostic('broker_adapter', 'callback_event_store_import_unavailable', exc, severity='warning', fail_closed=False)
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
            "status": "paper_prelive_only",
            "session_id": self._session_id,
            "broker_mode": "paper_prelive_only",
            "production_ready": False,
            "true_broker_ready": False,
            "real_money_execution": False,
            "broker_bound": False,
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
            'broker_kind': 'paper_prelive_only_stub',
            'broker_mode': 'paper_prelive_only',
            'paper_prelive_ready': True,
            'true_broker_ready': False,
            'real_money_execution': False,
            'broker_bound': False,
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
        normalized = normalize_broker_callback(event, broker="REAL_STUB", account_id=str(self.credentials.get("account_id", "")))
        self._callbacks.append(dict(normalized))
        if self._event_store is not None:
            try:
                self._event_store.record(normalized)
            except Exception as exc:
                record_diagnostic('broker_adapter', 'callback_event_store_record', exc, severity='warning', fail_closed=False)

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
            record_diagnostic('broker_adapter', 'apply_lot_fill_failed', exc, severity='warning', fail_closed=False)
            if not hasattr(self, '_lot_errors'):
                self._lot_errors = []
            self._lot_errors.append({'time': _rbs_lot_now(), 'error': repr(exc)})

    def _rbs_patched_record_callback(self, event: dict[str, Any]) -> None:
        _RBS_ORIG_RECORD_CALLBACK(self, event)
        _rbs_init_lot_book(self)
        for handler in list(getattr(self, '_callback_handlers', []) or []):
            try:
                handler(dict(event))
            except Exception as exc:
                record_diagnostic('broker_adapter', 'callback_handler_failed', exc, severity='warning', fail_closed=False)

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

except Exception as exc:
    record_diagnostic('broker_adapter', 'broker_stub_extension_patch_failed', exc, severity='error', fail_closed=True)


# =============================================================================
# vNext institutional lot lifecycle extension for mock real broker
# =============================================================================
try:
    import json as _json_rbs
    from datetime import datetime
    try:
        from fts_config import CONFIG as _RBS_CFG
    except Exception as exc:
        record_diagnostic('broker_adapter', 'load_rbs_config', exc, severity='warning', fail_closed=False)
        _RBS_CFG = None

    _RBS_V2_ORIG_INIT = globals().get('_RBS_ORIG_INIT', RealBrokerStub.__init__)
    _RBS_V2_ORIG_APPLY_FILL = globals().get('_RBS_ORIG_APPLY_FILL', RealBrokerStub._apply_fill)
    _RBS_V2_ORIG_UPSERT_STOP = RealBrokerStub.upsert_protective_stop
    _RBS_V2_ORIG_REPLACE = RealBrokerStub.replace_order
    _RBS_V2_ORIG_PROCESS_STOPS = globals().get('_RBS_ORIG_PROCESS_STOPS', RealBrokerStub.process_protective_stops)

    def _rbs_cfg(name, default=None): return getattr(_RBS_CFG, name, default) if _RBS_CFG is not None else default
    def _rbs_cost_method(): return str(_rbs_cfg('lot_accounting_method', 'FIFO') or 'FIFO').upper()
    def _rbs_now(): return datetime.now().isoformat(timespec='seconds')
    def _rbs_parse_list(raw):
        if isinstance(raw, str):
            try: return _json_rbs.loads(raw)
            except Exception as exc:
                record_diagnostic('broker_adapter', 'parse_rbs_json_list', exc, severity='warning', fail_closed=False)
                return []
        return list(raw) if isinstance(raw, (list, tuple)) else []
    def _rbs_append_json(raw, val):
        arr=_rbs_parse_list(raw); arr.append(val); return _json_rbs.dumps(arr, ensure_ascii=False)
    def _rbs_init_v2(self):
        self._position_lots=[] if not isinstance(getattr(self,'_position_lots',None), list) else self._position_lots
        self._lot_seq=int(getattr(self,'_lot_seq',0) or 0)
        self._lot_fill_history=[] if not isinstance(getattr(self,'_lot_fill_history',None), list) else self._lot_fill_history
        self._lot_stop_link_events=[] if not isinstance(getattr(self,'_lot_stop_link_events',None), list) else self._lot_stop_link_events
        self._lot_close_history=[] if not isinstance(getattr(self,'_lot_close_history',None), list) else self._lot_close_history
        self._lot_errors=[] if not isinstance(getattr(self,'_lot_errors',None), list) else self._lot_errors
    def _rbs_position_key(symbol, side, strategy_name='', signal_id=''):
        parts=[str(symbol).upper(), str(side).upper()]
        if bool(_rbs_cfg('lot_partition_by_strategy', True)): parts.append(str(strategy_name or ''))
        if bool(_rbs_cfg('lot_partition_by_signal', True)): parts.append(str(signal_id or ''))
        return '|'.join(parts)
    def _rbs_next_lot_id_v2(self, ticker:str)->str:
        _rbs_init_v2(self); self._lot_seq += 1; return f"RLOT2-{str(ticker).replace('.', '')}-{self._lot_seq:07d}"
    def _rbs_open_lot_v2(self, ticker:str, side:str, qty:int, price:float, order_row:dict, fill_id:str=''):
        _rbs_init_v2(self); qty=int(qty or 0)
        if qty<=0: return
        strategy_name=str(order_row.get('strategy_name') or '')
        signal_id=str(order_row.get('signal_id') or '')
        client_order_id=str(order_row.get('client_order_id') or '')
        self._position_lots.append({'lot_id': _rbs_next_lot_id_v2(self,ticker), 'ticker': str(ticker), 'symbol': str(ticker), 'side': str(side).upper(), 'direction_bucket': str(side).upper(), 'position_key': _rbs_position_key(ticker, side, strategy_name, signal_id), 'strategy_name': strategy_name, 'strategy_bucket': strategy_name, 'signal_id': signal_id, 'client_order_id': client_order_id, 'open_qty': qty, 'remaining_qty': qty, 'avg_cost': float(price or 0.0), 'entry_price': float(price or 0.0), 'entry_time': _rbs_now(), 'entry_order_id': str(order_row.get('broker_order_id') or order_row.get('client_order_id') or ''), 'cost_basis_method': _rbs_cost_method(), 'entry_fill_qty': qty, 'close_fill_qty': 0, 'entry_fill_count': 1, 'close_fill_count': 0, 'entry_fill_ids_json': _json_rbs.dumps([fill_id] if fill_id else []), 'exit_fill_ids_json': _json_rbs.dumps([]), 'open_commission': float(order_row.get('commission',0) or 0.0), 'open_tax': float(order_row.get('tax',0) or 0.0), 'close_commission': 0.0, 'close_tax': 0.0, 'status': 'OPEN', 'lifecycle_status': 'OPEN', 'realized_pnl': 0.0, 'close_qty': 0, 'close_price': 0.0, 'close_time': '', 'stop_order_id': '', 'stop_price': 0.0, 'stop_status': '', 'linked_stop_qty': 0, 'last_fill_time': _rbs_now()})
    def _rbs_iter_lots(self, ticker:str, close_side:str, strategy_name:str='', signal_id:str=''):
        out=[]
        for lot in self._position_lots:
            if str(lot.get('ticker','')).upper()!=str(ticker).upper(): continue
            if str(lot.get('side','')).upper()!=str(close_side).upper(): continue
            if int(lot.get('remaining_qty',0) or 0)<=0: continue
            if bool(_rbs_cfg('lot_partition_by_strategy', True)) and strategy_name and str(lot.get('strategy_name',''))!=strategy_name: continue
            if bool(_rbs_cfg('lot_partition_by_signal', True)) and signal_id and str(lot.get('signal_id',''))!=signal_id: continue
            out.append(lot)
        if not out and bool(_rbs_cfg('lot_allow_cross_strategy_close', False)):
            out=[lot for lot in self._position_lots if str(lot.get('ticker','')).upper()==str(ticker).upper() and str(lot.get('side','')).upper()==str(close_side).upper() and int(lot.get('remaining_qty',0) or 0)>0]
        out.sort(key=lambda r:(str(r.get('entry_time','')), str(r.get('lot_id','')))); return out
    def _rbs_close_lots_v2(self, ticker:str, close_side:str, qty:int, price:float, order_row:dict, fill_id:str=''):
        remaining=int(qty or 0)
        if remaining<=0: return
        strategy_name=str(order_row.get('strategy_name') or '')
        signal_id=str(order_row.get('signal_id') or '')
        eligible=_rbs_iter_lots(self, ticker, close_side, strategy_name=strategy_name, signal_id=signal_id)
        avg_basis=None
        if _rbs_cost_method()=='AVERAGE' and eligible:
            tq=sum(int(l.get('remaining_qty',0) or 0) for l in eligible); tc=sum(int(l.get('remaining_qty',0) or 0)*float(l.get('avg_cost',0) or 0.0) for l in eligible); avg_basis=(tc/tq) if tq else None
        for lot in eligible:
            if remaining<=0: break
            lot_qty=int(lot.get('remaining_qty',0) or 0)
            if lot_qty<=0: continue
            take=min(lot_qty, remaining)
            basis=float(avg_basis if avg_basis is not None else (lot.get('avg_cost',0) or 0.0))
            pnl=(float(price)-basis)*take if close_side=='LONG' else (basis-float(price))*take
            lot['remaining_qty']=lot_qty-take; lot['close_qty']=int(lot.get('close_qty',0) or 0)+take; lot['close_fill_qty']=int(lot.get('close_fill_qty',0) or 0)+take; lot['close_fill_count']=int(lot.get('close_fill_count',0) or 0)+1
            if fill_id: lot['exit_fill_ids_json']=_rbs_append_json(lot.get('exit_fill_ids_json'), fill_id)
            lot['realized_pnl']=round(float(lot.get('realized_pnl',0) or 0.0)+pnl-float(order_row.get('commission',0) or 0.0)-float(order_row.get('tax',0) or 0.0),4)
            lot['close_commission']=round(float(lot.get('close_commission',0) or 0.0)+float(order_row.get('commission',0) or 0.0),4); lot['close_tax']=round(float(lot.get('close_tax',0) or 0.0)+float(order_row.get('tax',0) or 0.0),4)
            lot['close_price']=float(price or 0.0); lot['close_time']=_rbs_now(); lot['exit_order_id']=str(order_row.get('broker_order_id') or order_row.get('client_order_id') or ''); lot['last_fill_time']=_rbs_now(); lot['close_cost_basis_method']=_rbs_cost_method(); lot['close_cost_basis_price']=round(basis,4)
            lot['status']='CLOSED' if lot['remaining_qty']<=0 else 'PARTIAL_EXIT'; lot['lifecycle_status']=lot['status']
            self._lot_close_history.append({'lot_id': lot.get('lot_id'), 'ticker': ticker, 'side': close_side, 'closed_qty': take, 'realized_pnl': round(pnl,4), 'fill_id': fill_id, 'exit_order_id': lot.get('exit_order_id'), 'closed_at': _rbs_now(), 'position_key': lot.get('position_key')})
            self._lot_fill_history.append({'fill_id': fill_id or '', 'lot_id': lot.get('lot_id'), 'symbol': ticker, 'event': 'CLOSE', 'qty': take, 'price': float(price or 0.0), 'time': _rbs_now()})
            remaining-=take
    def _rbs_apply_fill_v2(self, order_row:dict, fill_qty:int, fill_price:float):
        side=str(order_row.get('side','')).upper(); ticker=str(order_row.get('ticker','')).upper(); fill_id=f"RFILL-{str(order_row.get('broker_order_id') or order_row.get('client_order_id') or 'NA')}-{len(getattr(self,'_fills',[]) or []):06d}"
        before=len(getattr(self,'_position_lots',[]) or [])
        if side=='BUY': _rbs_open_lot_v2(self,ticker,'LONG',int(fill_qty),float(fill_price),order_row,fill_id=fill_id)
        elif side=='SHORT': _rbs_open_lot_v2(self,ticker,'SHORT',int(fill_qty),float(fill_price),order_row,fill_id=fill_id)
        elif side=='SELL': _rbs_close_lots_v2(self,ticker,'LONG',int(fill_qty),float(fill_price),order_row,fill_id=fill_id)
        elif side in {'COVER','BUY_TO_COVER'}: _rbs_close_lots_v2(self,ticker,'SHORT',int(fill_qty),float(fill_price),order_row,fill_id=fill_id)
        try:
            recent_lots=(getattr(self,'_position_lots',[]) or [])[before:]
            if getattr(self,'_callbacks',None):
                cb=self._callbacks[-1]
                lot_ids=[str(l.get('lot_id')) for l in recent_lots] or [str(l.get('lot_id')) for l in (getattr(self,'_position_lots',[]) or []) if str(l.get('exit_order_id',''))==str(order_row.get('broker_order_id') or order_row.get('client_order_id') or '')][-5:]
                if lot_ids:
                    cb['lot_id']=lot_ids[0]
                    cb['lot_ids']=lot_ids
                    cb['position_key']=str(recent_lots[0].get('position_key')) if recent_lots else ''
                    cb['strategy_name']=str(order_row.get('strategy_name') or '')
                    cb['signal_id']=str(order_row.get('signal_id') or '')
        except Exception as exc:
            record_diagnostic('broker_adapter', 'rbs_callback_enrichment_failed', exc, severity='warning', fail_closed=False)
    def _rbs_init_wrapper_v2(self,*a,**kw): _RBS_V2_ORIG_INIT(self,*a,**kw); _rbs_init_v2(self)
    def _rbs_apply_fill_wrapper_v2(self, order_row, fill_qty, fill_price): _RBS_V2_ORIG_APPLY_FILL(self, order_row, fill_qty, fill_price); _rbs_apply_fill_v2(self, order_row, fill_qty, fill_price)
    def _rbs_link_stop(self, rec:dict):
        ticker=str(rec.get('symbol','') or '').upper(); stop_side=str(rec.get('side','SELL') or 'SELL').upper(); close_side='LONG' if stop_side=='SELL' else 'SHORT'; qty_need=int(rec.get('quantity',0) or 0); strategy_name=str(rec.get('strategy_name','') or ''); signal_id=str(rec.get('signal_id','') or '')
        linked=[]; remaining=qty_need
        for lot in _rbs_iter_lots(self, ticker, close_side, strategy_name=strategy_name if bool(_rbs_cfg('lot_stop_linkage_match_strategy', True)) else '', signal_id=signal_id if bool(_rbs_cfg('lot_stop_linkage_match_signal', False)) else ''):
            if remaining<=0: break
            take=min(int(lot.get('remaining_qty',0) or 0), remaining); lot['stop_order_id']=str(rec.get('broker_order_id') or rec.get('order_id') or ''); lot['stop_price']=float(rec.get('stop_price',0) or 0.0); lot['stop_status']=str(rec.get('status','WORKING') or ''); lot['linked_stop_qty']=take; linked.append(str(lot.get('lot_id'))); remaining-=take
        rec['linked_lot_ids']=linked
        self._lot_stop_link_events.append({'time': _rbs_now(), 'broker_order_id': rec.get('broker_order_id') or rec.get('order_id'), 'linked_lot_ids': list(linked), 'linked_qty': qty_need-remaining})
    def _rbs_upsert_stop_wrapper_v2(self, symbol:str, quantity:int, stop_price:float, side:str='SELL', client_order_id:str='', note:str='', strategy_name:str='', signal_id:str='', position_key:str=''):
        resp=_RBS_V2_ORIG_UPSERT_STOP(self,symbol,quantity,stop_price,side=side,client_order_id=client_order_id,note=note)
        rec=resp.get('record') or {}
        if strategy_name: rec['strategy_name']=strategy_name
        if signal_id: rec['signal_id']=signal_id
        if position_key: rec['position_key']=position_key
        self._protective_stops[str(rec.get('broker_order_id') or rec.get('order_id'))]=rec; _rbs_link_stop(self, rec); resp['record']=dict(rec); return resp
    def _rbs_replace_wrapper_v2(self, broker_order_id:str, payload:dict):
        resp=_RBS_V2_ORIG_REPLACE(self, broker_order_id, payload); rec=self._protective_stops.get(str(broker_order_id))
        if rec is not None:
            if 'strategy_name' in payload: rec['strategy_name']=payload.get('strategy_name') or rec.get('strategy_name','')
            if 'signal_id' in payload: rec['signal_id']=payload.get('signal_id') or rec.get('signal_id','')
            if 'position_key' in payload: rec['position_key']=payload.get('position_key') or rec.get('position_key','')
            _rbs_link_stop(self, rec); resp['record']=dict(rec)
        return resp
    def _rbs_process_stops_wrapper_v2(self, price_map=None):
        before=len(getattr(self,'_callbacks',[]) or []); events=_RBS_V2_ORIG_PROCESS_STOPS(self, price_map); new=list((getattr(self,'_callbacks',[]) or [])[before:])
        for ev in new:
            if str(ev.get('event_type','')).upper()!='STOP_TRIGGERED': continue
            oid=str(ev.get('broker_order_id') or ev.get('order_id') or ''); rec=self._protective_stops.get(oid,{})
            for lot in self._position_lots:
                if str(lot.get('stop_order_id',''))==oid or lot.get('lot_id') in list(rec.get('linked_lot_ids') or []): lot['stop_status']='TRIGGERED_FILLED'
        return events
    def _rbs_get_position_lots_v2(self, include_closed: bool = False):
        rows=[]
        for lot in self._position_lots:
            if include_closed or int(lot.get('remaining_qty',0) or 0)>0 or str(lot.get('status','')).upper()=='PARTIAL_EXIT':
                row=dict(lot); px=float(self._last_prices.get(row.get('ticker',''), row.get('avg_cost',0)) or 0.0); qty=int(row.get('remaining_qty',0) or 0); avg=float(row.get('avg_cost',0) or 0.0); row['market_price']=px; row['market_value']=abs(qty)*px; row['cost_value']=abs(qty)*avg; row['unrealized_pnl']=round((px-avg)*qty if row.get('side')=='LONG' else (avg-px)*qty,4); row['realized_unrealized_total']=round(float(row.get('realized_pnl',0) or 0.0)+float(row.get('unrealized_pnl',0) or 0.0),4); rows.append(row)
        return rows
    def _rbs_reconcile_lots_v2(self):
        lots=_rbs_get_position_lots_v2(self, include_closed=False); lot_pos={}
        for lot in lots:
            sym=str(lot.get('ticker','')).upper(); rem=int(lot.get('remaining_qty',0) or 0); lot_pos[sym]=lot_pos.get(sym,0)+(rem if lot.get('side')=='LONG' else -rem)
        diffs=[]
        for sym in sorted(set(lot_pos)|set(getattr(self,'_positions',{}).keys())):
            p=self._positions.get(sym); agg=int(getattr(p,'qty',0) or 0) if p is not None else 0; lot_qty=int(lot_pos.get(sym,0) or 0)
            if agg!=lot_qty: diffs.append({'ticker':sym,'aggregate_qty':agg,'lot_qty':lot_qty,'diff_qty':agg-lot_qty})
        return {'ok': len(diffs)==0, 'diffs': diffs, 'lot_count': len(lots), 'cost_basis_method': _rbs_cost_method()}
    RealBrokerStub.__init__ = _rbs_init_wrapper_v2
    RealBrokerStub._apply_fill = _rbs_apply_fill_wrapper_v2
    RealBrokerStub.upsert_protective_stop = _rbs_upsert_stop_wrapper_v2
    RealBrokerStub.replace_order = _rbs_replace_wrapper_v2
    RealBrokerStub.process_protective_stops = _rbs_process_stops_wrapper_v2
    RealBrokerStub.get_position_lots = _rbs_get_position_lots_v2
    RealBrokerStub.reconcile_lots_to_positions = _rbs_reconcile_lots_v2
    if not hasattr(RealBrokerStub,'_orig_export_runtime_snapshot'): RealBrokerStub._orig_export_runtime_snapshot = RealBrokerStub.export_runtime_snapshot
    def _rbs_export_v2(self):
        snap=RealBrokerStub._orig_export_runtime_snapshot(self) if hasattr(RealBrokerStub,'_orig_export_runtime_snapshot') else {}; snap['position_lots']=self.get_position_lots(include_closed=True); snap['lot_reconciliation']=self.reconcile_lots_to_positions(); snap['callbacks']=list(getattr(self,'_callbacks',[])[-100:]); snap['lot_fill_history']=list(getattr(self,'_lot_fill_history',[])[-200:]); snap['lot_stop_link_events']=list(getattr(self,'_lot_stop_link_events',[])[-100:]); return snap
    RealBrokerStub.export_runtime_snapshot = _rbs_export_v2
except Exception as exc:
    record_diagnostic('broker_adapter', 'broker_stub_extension_patch_failed', exc, severity='error', fail_closed=True)

# =============================================================================
# vNext tax-lot jurisdiction / wash-sale / report overlay for mock real broker
# =============================================================================
try:
    from fts_tax_lot_accounting import (
        decorate_open_lot as _rbstax_decorate_open_lot,
        enrich_open_lot as _rbstax_enrich_open_lot,
        closure_event as _rbstax_closure_event,
        apply_wash_sale_adjustments as _rbstax_apply_wash_sale,
        summarize_tax_lots as _rbstax_summarize_lots,
        export_tax_reports as _rbstax_export_reports,
        money as _rbstax_money,
        qty_int as _rbstax_qty,
    )
    _RBS_TAX_ORIG_APPLY_FILL = RealBrokerStub._apply_fill
    _RBS_TAX_ORIG_PROCESS_STOPS = RealBrokerStub.process_protective_stops
    _RBS_TAX_ORIG_GET_LOTS = RealBrokerStub.get_position_lots
    _RBS_TAX_ORIG_EXPORT = RealBrokerStub.export_runtime_snapshot

    def _rbs_tax_init(self):
        if not hasattr(self, '_tax_lot_closures') or not isinstance(getattr(self, '_tax_lot_closures', None), list):
            self._tax_lot_closures = []
        if not hasattr(self, '_tax_report_exports') or not isinstance(getattr(self, '_tax_report_exports', None), dict):
            self._tax_report_exports = {}

    def _rbs_tax_decorate_all(self):
        _rbs_tax_init(self)
        rows=[]
        for lot in list(getattr(self, '_position_lots', []) or []):
            try:
                rows.append(_rbstax_decorate_open_lot(lot))
            except Exception as exc:
                record_diagnostic(
                    'broker_adapter',
                    'rbs_tax_decorate_open_lot_failed',
                    exc,
                    severity='warning',
                    fail_closed=False,
                    context={'lot_id': str(lot.get('lot_id', '')) if isinstance(lot, dict) else ''},
                )
                rows.append(lot)
        self._position_lots = rows

    def _rbs_tax_sync_new_closures(self, before_count: int, order_row=None, fill_qty: int = 0, fill_price: float = 0.0, note: str = ''):
        _rbs_tax_init(self); _rbs_tax_decorate_all(self)
        new_events = list((getattr(self, '_lot_close_history', []) or [])[before_count:])
        total_closed = sum(_rbstax_qty(ev.get('closed_qty') or ev.get('qty')) for ev in new_events) or _rbstax_qty(fill_qty) or 1
        for ev in new_events:
            try:
                lot = None
                for row in getattr(self, '_position_lots', []) or []:
                    if str(row.get('lot_id')) == str(ev.get('lot_id')):
                        lot = row; break
                if lot is None:
                    lot = {'lot_id': ev.get('lot_id'), 'ticker_symbol': ev.get('ticker') or ev.get('symbol'), 'side': ev.get('side'), 'entry_price': ev.get('entry_price'), 'remaining_qty': ev.get('closed_qty') or ev.get('qty'), 'open_qty': ev.get('closed_qty') or ev.get('qty')}
                q = _rbstax_qty(ev.get('closed_qty') or ev.get('qty'))
                ratio = q / total_closed if total_closed else 1.0
                commission = float((order_row or {}).get('commission', 0) or 0) * ratio if isinstance(order_row, dict) else 0.0
                tax = float((order_row or {}).get('tax', 0) or 0) * ratio if isinstance(order_row, dict) else 0.0
                event = _rbstax_closure_event(lot=lot, qty=q, close_price=float(ev.get('close_price') or fill_price or 0.0), exit_order_id=str((order_row or {}).get('broker_order_id') or (order_row or {}).get('client_order_id') or ev.get('exit_order_id') or ''), fill_id=str(ev.get('fill_id','')), commission=commission, tax=tax, note=note or ev.get('note',''))
                self._tax_lot_closures.append(event)
            except Exception as exc:
                record_diagnostic('broker_adapter', 'rbs_tax_closure_failed', exc, severity='warning', fail_closed=False)
        try:
            adjusted, lots = _rbstax_apply_wash_sale(list(self._tax_lot_closures), list(getattr(self, '_position_lots', []) or []))
            self._tax_lot_closures = adjusted
            self._position_lots = lots
        except Exception as exc:
            record_diagnostic('broker_adapter', 'rbs_tax_wash_sale_failed', exc, severity='warning', fail_closed=False)

    def _rbs_tax_apply_fill(self, order_row, fill_qty, fill_price):
        _rbs_tax_init(self)
        before = len(getattr(self, '_lot_close_history', []) or [])
        _RBS_TAX_ORIG_APPLY_FILL(self, order_row, fill_qty, fill_price)
        _rbs_tax_decorate_all(self)
        _rbs_tax_sync_new_closures(self, before, order_row=order_row, fill_qty=fill_qty, fill_price=fill_price, note='real_stub_fill_tax_lot')

    def _rbs_tax_process_stops(self, price_map=None):
        _rbs_tax_init(self)
        before = len(getattr(self, '_lot_close_history', []) or [])
        events = _RBS_TAX_ORIG_PROCESS_STOPS(self, price_map)
        _rbs_tax_sync_new_closures(self, before, order_row=None, fill_qty=0, fill_price=0.0, note='real_stub_stop_tax_lot')
        return events

    def _rbs_tax_get_lots(self, include_closed: bool = False):
        _rbs_tax_decorate_all(self)
        rows = _RBS_TAX_ORIG_GET_LOTS(self, include_closed=include_closed)
        out=[]
        for row in rows:
            try:
                px = getattr(self, '_last_prices', {}).get(row.get('ticker') or row.get('symbol') or row.get('ticker_symbol'))
                out.append(_rbstax_enrich_open_lot(row, market_price=px))
            except Exception as exc:
                record_diagnostic(
                    'broker_adapter',
                    'rbs_tax_enrich_open_lot_failed',
                    exc,
                    severity='warning',
                    fail_closed=False,
                    context={'lot_id': str(row.get('lot_id', '')) if isinstance(row, dict) else ''},
                )
                out.append(row)
        return out

    def _rbs_tax_export(self):
        snap = _RBS_TAX_ORIG_EXPORT(self)
        lots = _rbs_tax_get_lots(self, include_closed=True)
        closures = list(getattr(self, '_tax_lot_closures', []) or [])
        try:
            closures, lots = _rbstax_apply_wash_sale(closures, lots)
            self._tax_lot_closures = closures
            self._position_lots = lots
        except Exception as exc:
            record_diagnostic('broker_adapter', 'rbs_tax_export_wash_sale_failed', exc, severity='warning', fail_closed=False)
        snap['tax_lot_closures'] = closures
        snap['tax_lot_summary'] = _rbstax_summarize_lots(closures, lots)
        snap['tax_lot_accounting'] = {'engine': 'fts_tax_lot_accounting', 'broker': 'real_stub'}
        try:
            self._tax_report_exports = _rbstax_export_reports(closures, lots)
            snap['tax_report_exports'] = self._tax_report_exports
        except Exception as exc:
            record_diagnostic('broker_adapter', 'rbs_tax_report_export_failed', exc, severity='warning', fail_closed=False)
            snap['tax_report_export_error'] = repr(exc)
        return snap

    RealBrokerStub._apply_fill = _rbs_tax_apply_fill
    RealBrokerStub.process_protective_stops = _rbs_tax_process_stops
    RealBrokerStub.get_position_lots = _rbs_tax_get_lots
    RealBrokerStub.export_runtime_snapshot = _rbs_tax_export
except Exception as exc:
    record_diagnostic('broker_adapter', 'broker_stub_tax_lot_patch_failed', exc, severity='warning', fail_closed=False)


# =============================================================================
# Formal class facade
# =============================================================================
# Patch blocks above are retained for backward compatibility with historical update
# packs.  New code should import RealBrokerStub after this point; the public class
# is now a formal subclass rather than a monkey-patched base symbol.
_PatchedRealBrokerStubBase = RealBrokerStub
class FormalRealBrokerStub(_PatchedRealBrokerStubBase):
    FORMAL_CLASS_LAYER = True
    CALLBACK_MAPPING_LAYER = "fts_broker_callback_mapping.BrokerCallbackMapper"

RealBrokerStub = FormalRealBrokerStub
