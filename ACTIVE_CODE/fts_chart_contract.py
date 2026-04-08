# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartOutputContract:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_output_contract.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "required_fields": [
                "ticker",
                "chart_type",
                "artifact_path_or_id",
            ],
            "optional_fields": [
                "timeframe",
                "signal_context",
                "strategy_name",
                "notes",
            ],
            "status": "defined",
            "notes": "v40 先把圖表輸出契約定義好，後續可再把舊圖表模組逐步遷移到此契約。"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📊 已輸出 chart output contract：{self.path}")
        return self.path, payload
