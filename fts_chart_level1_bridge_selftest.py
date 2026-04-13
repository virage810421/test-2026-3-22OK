# -*- coding: utf-8 -*-
"""Self-test for chart facade integration.

這支只驗證 facade 狀態與介面，不直接觸發實際繪圖。
"""
from __future__ import annotations

import importlib
import json
from pathlib import Path


def main():
    adv = importlib.import_module("advanced_chart")
    svc = importlib.import_module("fts_chart_service")

    results = {
        "advanced_chart_has_draw_chart": hasattr(adv, "draw_chart"),
        "advanced_chart_has_render_trade_chart": hasattr(adv, "render_trade_chart"),
        "service_has_draw_chart": hasattr(svc, "draw_chart"),
        "service_has_render_trade_chart": hasattr(svc, "render_trade_chart"),
        "renderer_source": getattr(svc, "CHART_RENDERER_SOURCE", None),
        "facade_mode": getattr(svc, "CHART_RENDERER_MODE", None),
        "integration_status": "service_facade_ready",
    }

    out = Path("chart_facade_selftest_result.json")
    out.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
