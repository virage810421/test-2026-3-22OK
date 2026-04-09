# -*- coding: utf-8 -*-
"""Compatibility wrapper.
v83 收編後，舊門牌 `advanced_chart.py` 仍可 import；
但真正的圖表渲染能力已搬到 `fts_chart_service.py`。
"""
from fts_chart_service import *  # noqa: F401,F403
