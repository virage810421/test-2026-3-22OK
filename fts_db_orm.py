# -*- coding: utf-8 -*-
from __future__ import annotations

"""Optional ORM facade.

若環境安裝 SQLAlchemy，可用 declarative model；
否則退回 dataclass metadata，維持專案可編譯與 schema 可讀性。
"""

from dataclasses import dataclass

try:  # pragma: no cover
    from sqlalchemy.orm import declarative_base
    from sqlalchemy import Column, String, Integer, DateTime, Numeric
    SQLALCHEMY_AVAILABLE = True
    Base = declarative_base()

    class ExecutionOrder(Base):
        __tablename__ = 'execution_orders'
        order_id = Column(String(64), primary_key=True)
        client_order_id = Column(String(64))
        broker_order_id = Column(String(64))
        ticker_symbol = Column(String(32))
        direction_bucket = Column(String(16))
        strategy_bucket = Column(String(64))
        status = Column(String(32))
        qty = Column(Integer)
        filled_qty = Column(Integer)
        remaining_qty = Column(Integer)
        created_at = Column(DateTime)
        updated_at = Column(DateTime)
        signal_id = Column(String(100))
        note = Column(String)

except Exception:  # pragma: no cover
    SQLALCHEMY_AVAILABLE = False
    Base = object

    @dataclass
    class ExecutionOrder:
        order_id: str
        client_order_id: str | None = None
        broker_order_id: str | None = None
        ticker_symbol: str | None = None
        direction_bucket: str | None = None
        strategy_bucket: str | None = None
        status: str | None = None
        qty: int | None = None
        filled_qty: int | None = None
        remaining_qty: int | None = None
        signal_id: str | None = None
        note: str | None = None
