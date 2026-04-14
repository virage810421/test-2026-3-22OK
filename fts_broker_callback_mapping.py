# -*- coding: utf-8 -*-
"""Broker callback normalization layer.

This module freezes the callback contract between real broker adapters, paper
broker simulators, execution_engine and SQL persistence.  Adapters should map
raw SDK/websocket payloads into this normalized schema before the rest of the
system sees them.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any

try:
    from fts_runtime_diagnostics import record_issue
except ImportError:  # pragma: no cover - early boot diagnostic fallback
    def record_issue(*args, **kwargs):
        return {}

STATUS_MAP = {
    "NEW": "NEW", "ACK": "NEW", "PENDING_SUBMIT": "NEW",
    "SUBMITTED": "SUBMITTED", "ACCEPTED": "SUBMITTED", "WORKING": "SUBMITTED",
    "PARTIAL": "PARTIALLY_FILLED", "PARTIALLY_FILLED": "PARTIALLY_FILLED",
    "FILLED": "FILLED", "DONE": "FILLED", "TRIGGERED_FILLED": "FILLED",
    "CANCELLED": "CANCELLED", "CANCELED": "CANCELLED",
    "REJECTED": "REJECTED", "ERROR": "REJECTED",
    "REPLACED": "REPLACED", "AMENDED": "REPLACED",
}
EVENT_TYPE_MAP = {
    "ORDER": "ORDER_STATUS", "ORDER_STATUS": "ORDER_STATUS",
    "FILL": "FILL", "TRADE": "FILL", "EXECUTION": "FILL",
    "REJECT": "REJECT", "CANCEL": "CANCEL", "REPLACE": "REPLACE",
    "STOP_TRIGGER": "STOP_TRIGGER", "PROTECTIVE_STOP": "STOP_TRIGGER",
}

@dataclass
class NormalizedBrokerCallback:
    callback_id: str
    broker: str
    account_id: str
    event_type: str
    status: str
    broker_order_id: str
    client_order_id: str
    ticker_symbol: str
    side: str
    filled_qty: int
    remaining_qty: int
    avg_fill_price: float
    fill_price: float
    fill_id: str
    reject_code: str
    reject_reason: str
    event_time: str
    strategy_name: str
    signal_id: str
    lot_id: str
    position_key: str
    raw_payload: dict[str, Any]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _pick(row: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return default


def _int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError, OverflowError):
        return 0.0


def normalize_status(value: Any) -> str:
    return STATUS_MAP.get(str(value or "").strip().upper(), str(value or "UNKNOWN").strip().upper() or "UNKNOWN")


def normalize_event_type(value: Any, status: str = "") -> str:
    raw = str(value or "").strip().upper()
    if not raw and normalize_status(status) in {"FILLED", "PARTIALLY_FILLED"}:
        return "FILL"
    return EVENT_TYPE_MAP.get(raw, raw or "ORDER_STATUS")


class BrokerCallbackMapper:
    def __init__(self, broker: str = "GENERIC", account_id: str = ""):
        self.broker = broker or "GENERIC"
        self.account_id = account_id or ""

    def normalize(self, event: dict[str, Any] | None) -> dict[str, Any]:
        row = dict(event or {})
        status = normalize_status(_pick(row, "status", "order_status", "state", default=""))
        event_type = normalize_event_type(_pick(row, "event_type", "type", "callback_type", default=""), status=status)
        broker_order_id = str(_pick(row, "broker_order_id", "order_id", "id", default=""))
        client_order_id = str(_pick(row, "client_order_id", "client_id", default=""))
        ticker = str(_pick(row, "ticker_symbol", "symbol", "ticker", "Ticker SYMBOL", default="")).strip().upper()
        side = str(_pick(row, "side", "action", "direction", default="")).strip().upper()
        filled_qty = _int(_pick(row, "filled_qty", "fill_qty", "qty", "quantity", default=0))
        remaining_qty = _int(_pick(row, "remaining_qty", "leaves_qty", default=0))
        avg_fill_price = _float(_pick(row, "avg_fill_price", "average_price", "price", "fill_price", default=0.0))
        fill_price = _float(_pick(row, "fill_price", "price", "avg_fill_price", default=0.0))
        fill_id = str(_pick(row, "fill_id", "execution_id", "trade_id", default=""))
        event_time = str(_pick(row, "event_time", "timestamp", "fill_time", "update_time", default=_now()))
        callback_id = str(_pick(row, "callback_id", default="")) or "CB-" + (broker_order_id or client_order_id or fill_id or str(abs(hash(str(row)))))[-32:]
        out = NormalizedBrokerCallback(
            callback_id=callback_id,
            broker=str(_pick(row, "broker", default=self.broker)),
            account_id=str(_pick(row, "account_id", default=self.account_id)),
            event_type=event_type,
            status=status,
            broker_order_id=broker_order_id,
            client_order_id=client_order_id,
            ticker_symbol=ticker,
            side=side,
            filled_qty=filled_qty,
            remaining_qty=remaining_qty,
            avg_fill_price=avg_fill_price,
            fill_price=fill_price,
            fill_id=fill_id,
            reject_code=str(_pick(row, "reject_code", default="")),
            reject_reason=str(_pick(row, "reject_reason", "reason", default="")),
            event_time=event_time,
            strategy_name=str(_pick(row, "strategy_name", "strategy", default="")),
            signal_id=str(_pick(row, "signal_id", default="")),
            lot_id=str(_pick(row, "lot_id", default="")),
            position_key=str(_pick(row, "position_key", default="")),
            raw_payload=row,
        )
        return asdict(out)


def normalize_broker_callback(event: dict[str, Any] | None, broker: str = "GENERIC", account_id: str = "") -> dict[str, Any]:
    try:
        return BrokerCallbackMapper(broker=broker, account_id=account_id).normalize(event)
    except Exception as exc:
        record_issue("broker_callback_mapping", "callback_normalization_failed", exc, severity="ERROR", fail_mode="fail_closed", context={"event": str(event)[:500]})
        row = dict(event or {})
        row.setdefault("callback_mapping_error", str(exc))
        row.setdefault("status", "UNKNOWN")
        row.setdefault("event_type", "UNKNOWN")
        return row
