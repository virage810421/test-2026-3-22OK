# broker_base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Mapping, Sequence


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    SHORT = "SHORT"
    COVER = "COVER"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(str, Enum):
    NEW = "NEW"
    PENDING_SUBMIT = "PENDING_SUBMIT"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


def _enum_value(value: Any) -> Any:
    return getattr(value, 'value', value)


@dataclass
class OrderRequest:
    symbol: str
    side: OrderSide
    quantity: int
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    strategy_name: str = ""
    signal_id: str = ""
    client_order_id: str = ""
    note: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "ticker": self.symbol,
            "symbol": self.symbol,
            "side": _enum_value(self.side),
            "qty": int(self.quantity),
            "quantity": int(self.quantity),
            "order_type": _enum_value(self.order_type),
            "price": self.limit_price,
            "limit_price": self.limit_price,
            "strategy_name": self.strategy_name,
            "signal_id": self.signal_id,
            "client_order_id": self.client_order_id,
            "note": self.note,
        }


@dataclass
class FillEvent:
    order_id: str
    symbol: str
    side: OrderSide
    fill_qty: int
    fill_price: float
    fill_time: str
    commission: float = 0.0
    tax: float = 0.0
    slippage: float = 0.0
    strategy_name: str = ""
    signal_id: str = ""
    note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OrderRecord:
    order_id: str
    symbol: str
    side: OrderSide
    quantity: int
    filled_qty: int = 0
    remaining_qty: int = 0
    avg_fill_price: float = 0.0
    order_type: OrderType = OrderType.MARKET
    limit_price: Optional[float] = None
    status: OrderStatus = OrderStatus.NEW
    create_time: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    update_time: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    reject_reason: str = ""
    strategy_name: str = ""
    signal_id: str = ""
    client_order_id: str = ""
    note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class BrokerBase(ABC):
    """
    單一正式 broker contract。

    正式 contract 使用固定方法簽名；歷史相容邏輯集中在 compat helper，
    避免在每個正式介面內散落 hasattr/getattr 分支。
    """

    CONTRACT_VERSION = "v94_unified_broker_contract_hardened"

    @abstractmethod
    def place_order(self, order: OrderRequest | Mapping[str, Any]) -> OrderRecord | dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, order_id: str | Mapping[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    def _compat_list_call(self, method_names: Sequence[str]) -> list[Any]:
        for method_name in method_names:
            method = getattr(self, method_name, None)
            if not callable(method):
                continue
            try:
                rows = method()
            except Exception:
                return []
            if rows is None:
                return []
            try:
                return list(rows)
            except TypeError:
                return []
        return []

    def _compat_attr_dict_update(self, attr_name: str, payload: Mapping[str, Any]) -> None:
        target = getattr(self, attr_name, None)
        if isinstance(target, dict):
            target.update(dict(payload))

    def get_order_status(self, order_id: str) -> dict[str, Any]:
        return {"ok": False, "status": "get_order_status_not_implemented", "broker_order_id": order_id}

    def replace_order(self, order_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        return {"ok": False, "status": "replace_order_not_implemented", "broker_order_id": order_id, "payload": dict(payload or {})}

    def get_open_orders(self) -> list[OrderRecord | dict[str, Any]]:
        compat_rows = self._compat_list_call(("query_open_orders", "snapshot_orders"))
        if compat_rows:
            normalized: list[OrderRecord | dict[str, Any]] = []
            for row in compat_rows:
                if isinstance(row, Mapping):
                    status = str(row.get("status", "")).upper()
                    if status in {"NEW", "PENDING_SUBMIT", "SUBMITTED", "PARTIALLY_FILLED", "WORKING"}:
                        normalized.append(dict(row))
                else:
                    normalized.append(row)
            return normalized
        return []

    def query_open_orders(self) -> list[OrderRecord | dict[str, Any]]:
        return self.get_open_orders()

    def get_positions(self) -> dict[str, Any] | list[dict[str, Any]]:
        return {}

    def query_positions(self) -> list[dict[str, Any]]:
        compat_rows = self._compat_list_call(("get_positions_rows",))
        if compat_rows:
            return [dict(x) for x in compat_rows if isinstance(x, Mapping)]
        pos = self.get_positions()
        if isinstance(pos, dict):
            return [{"ticker": k, "qty": v} for k, v in pos.items()]
        return [dict(x) for x in list(pos or []) if isinstance(x, Mapping)]

    def get_cash(self) -> dict[str, Any]:
        return {"cash_available": 0.0, "equity": 0.0, "market_value": 0.0}

    def query_cash(self) -> dict[str, Any]:
        snap = self.get_cash()
        if isinstance(snap, dict):
            return dict(snap)
        cash = float(snap or 0.0)
        return {"cash_available": cash, "cash": cash}

    def get_account_snapshot(self) -> dict[str, Any]:
        return self.query_cash()

    def poll_fills(self) -> list[FillEvent | dict[str, Any]]:
        return self._compat_list_call(("get_fills",))

    def get_fills(self, trading_date: str | None = None) -> list[FillEvent | dict[str, Any]]:
        return []

    def update_market_price(self, ticker: str, price: float) -> None:
        if type(self).update_market_prices is not BrokerBase.update_market_prices:
            type(self).update_market_prices(self, {ticker: price})
            return
        self._compat_attr_dict_update('last_prices', {str(ticker): float(price or 0.0)})

    def update_market_prices(self, price_map: Mapping[str, float]) -> None:
        if self._compat_attr_is_dict('last_prices'):
            self._compat_attr_dict_update('last_prices', {str(t): float(p or 0.0) for t, p in dict(price_map or {}).items()})
            return
        for ticker, price in dict(price_map or {}).items():
            self.update_market_price(str(ticker), float(price or 0.0))

    def restore_state(self, cash: Any, positions: Any, last_prices: Mapping[str, float] | None = None) -> None:
        if hasattr(self, 'cash'):
            try:
                setattr(self, 'cash', float(cash or 0.0))
            except (TypeError, ValueError):
                pass
        if hasattr(self, 'positions') and isinstance(positions, dict):
            setattr(self, 'positions', dict(positions))
        if last_prices is not None and hasattr(self, 'last_prices'):
            setattr(self, 'last_prices', dict(last_prices))

    def connect(self) -> dict[str, Any]:
        return {"ok": False, "status": "broker_connect_not_implemented"}

    def refresh_auth(self) -> dict[str, Any]:
        return {"ok": False, "status": "refresh_auth_not_implemented"}

    def disconnect(self) -> dict[str, Any]:
        return {"ok": False, "status": "disconnect_not_implemented"}

    def poll_callbacks(self, clear: bool = False) -> list[dict[str, Any]]:
        return []

    def reconcile(self) -> dict[str, Any]:
        return {"ok": False, "status": "reconcile_not_implemented"}

    def capability_report(self) -> dict[str, Any]:
        return {
            "contract_version": self.CONTRACT_VERSION,
            "api_bound": False,
            "callback_bound": False,
            "ledger_bound": False,
            "reconcile_bound": False,
            "kill_switch_bound": False,
            "connect": False,
            "refresh_auth": False,
            "place_order": True,
            "cancel_order": True,
            "replace_order": False,
            "get_order_status": False,
            "query_open_orders": False,
            "query_positions": False,
            "query_cash": False,
            "get_fills": False,
            "poll_callbacks": False,
            "reconcile": False,
            "true_broker_ready": False,
            "real_money_execution": False,
        }
