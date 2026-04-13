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

    MODULE_VERSION = "v86_mock_real_broker_contract_aligned"

    def __init__(self, credentials: dict[str, Any] | None = None):
        self.credentials = credentials or {}
        self._positions: Dict[str, Position] = {}
        self._cash = float(getattr(CONFIG, "starting_cash", 0.0))
        self._last_prices: Dict[str, float] = {}
        self._orders: Dict[str, dict[str, Any]] = {}
        self._fills: List[dict[str, Any]] = []
        self._callbacks: List[dict[str, Any]] = []
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
        return {
            "ok": True,
            "status": "connected",
            "session_id": self._session_id,
            "broker_mode": "mock_real",
            "connected_at": now_str(),
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
            market_value += px * pos.qty
        equity = self._cash + market_value
        return {
            "cash_available": round(self._cash, 2),
            "market_value": round(market_value, 2),
            "equity": round(equity, 2),
            "buying_power": round(self._cash, 2),
            "updated_at": now_str(),
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

    def query_open_orders(self) -> list[dict[str, Any]]:
        return [dict(x) for x in self._orders.values() if str(x.get('status', '')).upper() in {'NEW', 'SUBMITTED', 'PARTIALLY_FILLED'}]

    def query_positions(self) -> list[dict[str, Any]]:
        return self.get_positions_rows()

    def query_cash(self) -> dict[str, Any]:
        return self.get_cash()

    def reconcile(self) -> dict[str, Any]:
        cash = self.get_cash()
        positions = self.get_positions_rows()
        open_orders = self.query_open_orders()
        fills = self.get_fills()
        backlog = len(self._callbacks)
        return {
            'ok': True,
            'status': 'reconciled_mock_real',
            'as_of': now_str(),
            'cash': cash,
            'positions': positions,
            'open_orders': open_orders,
            'fills': fills,
            'callback_backlog': backlog,
        }

    def capability_report(self) -> dict[str, Any]:
        return {
            'connect': True,
            'refresh_auth': True,
            'disconnect': True,
            'place_order': True,
            'cancel_order': True,
            'replace_order': True,
            'get_order_status': True,
            'query_open_orders': True,
            'query_positions': True,
            'query_cash': True,
            'get_fills': True,
            'poll_callbacks': True,
            'reconcile': True,
            'broker_mode': 'mock_real',
        }

    def replace_order(self, broker_order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        order = self._orders.get(str(broker_order_id))
        if not order:
            return {"ok": False, "status": "missing_order", "broker_order_id": broker_order_id}
        if order.get("status") not in {"SUBMITTED", "PARTIALLY_FILLED"}:
            return {"ok": False, "status": "replace_blocked", "broker_order_id": broker_order_id, "reason": "order_not_working"}
        if "price" in payload:
            order["submitted_price"] = round_price(float(payload["price"]))
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
