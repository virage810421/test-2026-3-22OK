# -*- coding: utf-8 -*-
"""Level-1 compatibility bridge for advanced_chart.

保留舊門牌 advanced_chart.py，真正圖表能力由 fts_chart_service.py 提供。
採延後載入，避免單純 import / audit 時因 plotly、yfinance 等重依賴失敗。
"""
from __future__ import annotations

BRIDGE_LEVEL = "level_1"
BRIDGE_TARGET = "fts_chart_service.draw_chart"
LEGACY_SOURCE = "advanced_chart(1).zip::advanced_chart.py"


def draw_chart(*args, **kwargs):
    from fts_chart_service import draw_chart as _draw_chart
    return _draw_chart(*args, **kwargs)


__all__ = ["draw_chart", "BRIDGE_LEVEL", "BRIDGE_TARGET", "LEGACY_SOURCE"]
