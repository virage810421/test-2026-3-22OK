# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchQualityStatsBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_quality_stats.json"

    def build(self, compat_info: dict):
        row_count = compat_info.get("row_count", 0)
        rows_with_ticker = compat_info.get("rows_with_ticker", 0)
        rows_with_action = compat_info.get("rows_with_action", 0)
        rows_with_price = compat_info.get("rows_with_price", 0)

        def ratio(x):
            return round(x / row_count, 4) if row_count else 0

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "row_count": row_count,
            "stats": {
                "ticker_coverage": ratio(rows_with_ticker),
                "action_coverage": ratio(rows_with_action),
                "price_coverage": ratio(rows_with_price),
            },
            "status": "quality_stats_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🔬 已輸出 research quality stats：{self.path}")
        return self.path, payload
