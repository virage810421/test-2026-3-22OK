# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartDecisionBridgeSummary:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_decision_bridge_summary.json"

    def build(self, compat_info: dict, readiness: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "bridge_summary": {
                "decision_rows": compat_info.get("row_count", 0),
                "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                "signal_count": readiness.get("total_signals", 0),
                "bridge_mode": "decision_to_chart_context_registered",
                "status": "summary_only"
            },
            "notes": [
                "目前圖表與 decision 仍多透過舊模組間接連接",
                "v40 開始把 decision 與 chart 的橋接資訊正式輸出成摘要"
            ]
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🌉 已輸出 chart decision bridge summary：{self.path}")
        return self.path, payload
