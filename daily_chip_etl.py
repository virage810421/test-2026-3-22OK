# -*- coding: utf-8 -*-
"""Compatibility wrapper.
v83 收編後，舊門牌 `daily_chip_etl.py` 仍可執行；
但真正的 ETL / 排程引擎已搬到 `fts_etl_daily_chip_service.py`。
"""
from fts_etl_daily_chip_service import *  # noqa: F401,F403

if __name__ == "__main__":
    main_scheduler()
