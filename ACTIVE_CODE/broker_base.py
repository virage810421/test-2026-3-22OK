# broker_base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any


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
    @abstractmethod
    def place_order(self, order: OrderRequest) -> OrderRecord:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def get_order_status(self, order_id: str) -> Optional[OrderRecord]:
        raise NotImplementedError

    @abstractmethod
    def get_open_orders(self) -> List[OrderRecord]:
        raise NotImplementedError

    @abstractmethod
    def get_positions(self) -> Dict[str, int]:
        raise NotImplementedError

    @abstractmethod
    def get_cash(self) -> float:
        raise NotImplementedError

    @abstractmethod
    def poll_fills(self) -> List[FillEvent]:
        raise NotImplementedError