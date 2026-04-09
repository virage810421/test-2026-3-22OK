# -*- coding: utf-8 -*-
from __future__ import annotations

from fts_cross_sectional_percentile_service import CrossSectionalPercentileService

if __name__ == '__main__':
    path, payload = CrossSectionalPercentileService().build_snapshot()
    print('完成：', path)
    print(payload.get('status'))
    print('ticker_count =', payload.get('ticker_count'))
