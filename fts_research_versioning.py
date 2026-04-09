# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchVersioningBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_versioning.json"

    def build(self, compat_info: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "versioning": {
                "research_output_version": getattr(CONFIG, "package_version", "v53"),
                "decision_input_rows": compat_info.get("row_count", 0),
                "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                "rows_with_action": compat_info.get("rows_with_action", 0),
                "rows_with_price": compat_info.get("rows_with_price", 0),
            },
            "required_metadata": [
                "research_output_version",
                "generated_at",
                "decision_input_rows",
            ],
            "status": "versioning_defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🏷️ 已輸出 research versioning：{self.path}")
        return self.path, payload
