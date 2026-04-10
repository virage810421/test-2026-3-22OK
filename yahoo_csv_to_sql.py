# -*- coding: utf-8 -*-
"""Level-2 compatibility wrapper for yahoo_csv_to_sql.

舊門牌保留；真正的基本面 ETL 主線改由 fts_fundamentals_etl_mainline 提供。
預設採安全 local-sync 模式，避免在未配置 yfinance / SQL 條件時破壞主線。
"""
from __future__ import annotations

import os

from fts_fundamentals_etl_mainline import FundamentalsETLMainline

BRIDGE_LEVEL = 'level_2'
BRIDGE_TARGET = 'fts_fundamentals_etl_mainline.FundamentalsETLMainline'
LEGACY_SOURCE = 'yahoo_csv_to_sql.py'


def run_fundamentals_mainline(enable_network_fetch: bool | None = None, write_sql: bool = True):
    runner = FundamentalsETLMainline()
    if enable_network_fetch is None:
        enable_network_fetch = os.getenv('FTS_FUNDAMENTALS_ENABLE_NETWORK_FETCH', '0').strip().lower() in ('1', 'true', 'yes', 'y', 'on')
    if enable_network_fetch:
        return runner.smart_sync(enable_network_fetch=True, write_sql=write_sql)
    return runner.build_summary(mode='local_sync_only')


def main() -> int:
    run_fundamentals_mainline()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
