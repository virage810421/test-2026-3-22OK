# -*- coding: utf-8 -*-
from __future__ import annotations

"""Legacy CLI facade for yahoo_csv_to_sql.

新主線請直接走 fts_fundamentals_etl_mainline.FundamentalsETLMainline。
"""

import warnings

from fts_fundamentals_etl_mainline import FundamentalsETLMainline
from fts_utils import log

LEGACY_FACADE = True
SERVICE_ENTRYPOINT = 'fts_fundamentals_etl_mainline.FundamentalsETLMainline'

SUCCESS_STATUSES = {
    'summary_ready',
    'smart_sync_ready',
    'smart_sync_degraded',
    'summary_only',
    'fundamentals_etl_mainline_ready',
    'fundamentals_etl_mainline_partial',
}


def smart_sync(*args, **kwargs):
    warnings.warn('yahoo_csv_to_sql.py 已退役為 legacy facade；新主線請改用 fts_fundamentals_etl_mainline。', DeprecationWarning, stacklevel=2)
    return FundamentalsETLMainline().smart_sync(*args, **kwargs)


def main() -> int:
    warnings.warn('yahoo_csv_to_sql.py 已退役為 legacy facade；新主線請改用 fts_fundamentals_etl_mainline。', DeprecationWarning, stacklevel=2)
    path, payload = FundamentalsETLMainline().build_summary()
    status = str(payload.get('status', '')).strip()
    log(f'✅ yahoo_csv_to_sql 主線完成：{path}')
    return 0 if status in SUCCESS_STATUSES else 1


if __name__ == '__main__':
    raise SystemExit(main())
