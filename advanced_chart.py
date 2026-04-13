# -*- coding: utf-8 -*-
"""Legacy facade for advanced_chart.

核心主線禁止依賴本檔；真正實作請走 fts_chart_service。
"""
from __future__ import annotations

import warnings

LEGACY_FACADE = True
SERVICE_ENTRYPOINT = 'fts_chart_service.draw_chart'
LEGACY_SOURCE = 'advanced_chart(1).zip::advanced_chart.py'


def draw_chart(*args, **kwargs):
    warnings.warn('advanced_chart.py 已退役為 legacy facade；新主線請改用 fts_chart_service.draw_chart。', DeprecationWarning, stacklevel=2)
    from fts_chart_service import draw_chart as _draw_chart
    return _draw_chart(*args, **kwargs)


__all__ = ['draw_chart', 'LEGACY_FACADE', 'SERVICE_ENTRYPOINT', 'LEGACY_SOURCE']
