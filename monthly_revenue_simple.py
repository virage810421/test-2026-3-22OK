# -*- coding: utf-8 -*-
"""Compatibility wrapper.
v83 收編後，舊門牌 `monthly_revenue_simple.py` 仍可執行；
但真正的月營收 ETL 引擎已搬到 `fts_etl_monthly_revenue_service.py`。
"""
from fts_etl_monthly_revenue_service import *  # noqa: F401,F403

if __name__ == "__main__":
    main()
