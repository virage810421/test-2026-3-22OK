# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartBridgeRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_bridge_registry.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "charting": {
                "current_mode": "service_facade_only",
                "rendering_source": "正式主線已改走 fts_chart_service，舊檔名僅保留外部相容入口",
                "status": "legacy_facade_decommissioning",
                "notes": [
                    "目前畫圖/圖表多數仍沿用舊版模組輸出",
                    "新主控已可把 research/decision/execution 納回流程，但尚未完全重寫成新圖表引擎",
                    "之後可再升級成 chart contract / chart registry / chart artifact governance"
                ]
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🖼️ 已輸出 chart bridge registry：{self.path}")
        return self.path, payload
