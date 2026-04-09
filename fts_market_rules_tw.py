# -*- coding: utf-8 -*-
from dataclasses import dataclass, asdict
from typing import Optional

@dataclass
class MarketRuleCheck:
    ticker: str
    passed: bool
    ref_price: float
    qty: int
    reason: str = ""
    tick_size: float = 0.0
    limit_up: Optional[float] = None
    limit_down: Optional[float] = None
    lot_mode: str = "board"

    def to_dict(self):
        return asdict(self)


def tick_size_for_price(price: float) -> float:
    p = float(price)
    if p < 10:
        return 0.01
    if p < 50:
        return 0.05
    if p < 100:
        return 0.1
    if p < 500:
        return 0.5
    if p < 1000:
        return 1.0
    return 5.0


def compute_price_limits(price: float):
    p = float(price)
    return round(p * 1.10, 2), round(p * 0.90, 2)


def is_valid_tick(price: float, tick: float) -> bool:
    q = round(float(price) / float(tick), 8)
    return abs(q - round(q)) < 1e-6


def validate_order_payload(
    ticker: str,
    ref_price: float,
    qty: int,
    lot_size: int = 1000,
    allow_odd_lot: bool = False,
) -> MarketRuleCheck:
    ref_price = float(ref_price or 0)
    qty = int(qty or 0)
    tick = tick_size_for_price(ref_price) if ref_price > 0 else 0.0
    limit_up, limit_down = compute_price_limits(ref_price) if ref_price > 0 else (None, None)

    if not ticker:
        return MarketRuleCheck(ticker=ticker, passed=False, ref_price=ref_price, qty=qty, reason='missing_ticker', tick_size=tick, limit_up=limit_up, limit_down=limit_down)
    if ref_price <= 0:
        return MarketRuleCheck(ticker=ticker, passed=False, ref_price=ref_price, qty=qty, reason='missing_or_invalid_price', tick_size=tick, limit_up=limit_up, limit_down=limit_down)
    if qty <= 0:
        return MarketRuleCheck(ticker=ticker, passed=False, ref_price=ref_price, qty=qty, reason='missing_or_invalid_qty', tick_size=tick, limit_up=limit_up, limit_down=limit_down)
    if not is_valid_tick(ref_price, tick):
        return MarketRuleCheck(ticker=ticker, passed=False, ref_price=ref_price, qty=qty, reason='price_not_valid_tick', tick_size=tick, limit_up=limit_up, limit_down=limit_down)

    if allow_odd_lot:
        return MarketRuleCheck(ticker=ticker, passed=True, ref_price=ref_price, qty=qty, reason='ok_odd_lot_allowed', tick_size=tick, limit_up=limit_up, limit_down=limit_down, lot_mode='odd')
    if qty % int(lot_size) != 0:
        return MarketRuleCheck(ticker=ticker, passed=False, ref_price=ref_price, qty=qty, reason='qty_not_board_lot_multiple', tick_size=tick, limit_up=limit_up, limit_down=limit_down)
    return MarketRuleCheck(ticker=ticker, passed=True, ref_price=ref_price, qty=qty, reason='ok', tick_size=tick, limit_up=limit_up, limit_down=limit_down, lot_mode='board')
