# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
from enum import Enum

class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderStatus(str, Enum):
    NEW = "NEW"
    PENDING_SUBMIT = "PENDING_SUBMIT"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"

@dataclass
class TradeSignal:
    ticker: str
    action: str
    reference_price: float
    target_qty: int
    score: float = 0.0
    ai_confidence: float = 0.5
    industry: str = "未知"
    strategy_name: str = "未命名策略"
    reason: str = ""
    model_name: str = ""
    model_version: str = ""
    regime: str = ""
    expected_return: float = 0.0
    kelly_fraction: float = 0.0
    raw: dict = field(default_factory=dict)

@dataclass
class Order:
    order_id: str
    ticker: str
    side: OrderSide
    qty: int
    ref_price: float
    submitted_price: float
    status: OrderStatus
    strategy_name: str
    signal_score: float
    ai_confidence: float
    industry: str
    created_at: str
    updated_at: str
    note: str = ""
    model_name: str = ""
    model_version: str = ""
    regime: str = ""

@dataclass
class Fill:
    fill_id: str
    order_id: str
    ticker: str
    side: OrderSide
    fill_qty: int
    fill_price: float
    commission: float
    tax: float
    fill_time: str

@dataclass
class Position:
    ticker: str
    qty: int
    avg_cost: float
    industry: str = "未知"
    updated_at: str = ""
    stop_loss_price: float = 0.0
    take_profit_price: float = 0.0
    highest_price: float = 0.0
    cooldown_until: int = 0
    entry_bar: int = 0
    partial_tp_done: bool = False
    add_on_count: int = 0
    lifecycle_note: str = ""

@dataclass
class AccountSnapshot:
    cash: float
    market_value: float
    equity: float
    exposure_ratio: float
    updated_at: str
