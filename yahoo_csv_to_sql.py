# -*- coding: utf-8 -*-
from __future__ import annotations

"""Level-2 wrapper for legacy yahoo_csv_to_sql.

保留舊檔名，但基本面 ETL 主線交給 fts_fundamentals_etl_mainline。
"""

from fts_fundamentals_etl_mainline import FundamentalsETLMainline
from fts_utils import log

BRIDGE_LEVEL = 'level_2'
BRIDGE_TARGET = 'fts_fundamentals_etl_mainline.FundamentalsETLMainline'
LEGACY_SOURCE = 'legacy yahoo_csv_to_sql.py'


def main() -> int:
    path, payload = FundamentalsETLMainline().build_summary()
    log(f'✅ yahoo_csv_to_sql 第二級主線完成：{path}')
    return 0 if payload.get('status') in {'summary_ready', 'smart_sync_ready', 'smart_sync_degraded', 'summary_only'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
