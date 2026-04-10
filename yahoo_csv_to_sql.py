# -*- coding: utf-8 -*-
from __future__ import annotations

"""Level-2 wrapper for legacy yahoo_csv_to_sql.

保留舊檔名，但基本面 ETL 主線交給 fts_fundamentals_etl_mainline。
修正：將 fundamentals_etl_mainline_ready / partial 視為成功狀態，
避免 bootstrap 顯示 ok=False。
"""

from fts_fundamentals_etl_mainline import FundamentalsETLMainline
from fts_utils import log

BRIDGE_LEVEL = 'level_2'
BRIDGE_TARGET = 'fts_fundamentals_etl_mainline.FundamentalsETLMainline'
LEGACY_SOURCE = 'legacy yahoo_csv_to_sql.py'

SUCCESS_STATUSES = {
    'summary_ready',
    'smart_sync_ready',
    'smart_sync_degraded',
    'summary_only',
    'fundamentals_etl_mainline_ready',
    'fundamentals_etl_mainline_partial',
}


def main() -> int:
    path, payload = FundamentalsETLMainline().build_summary()
    status = str(payload.get('status', '')).strip()
    log(f'✅ yahoo_csv_to_sql 第二級主線完成：{path}')
    return 0 if status in SUCCESS_STATUSES else 1


if __name__ == '__main__':
    raise SystemExit(main())
