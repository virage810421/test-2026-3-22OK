# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class Target95Scorecard:
    def __init__(self):
        self.path = PATHS.runtime_dir / "target95_scorecard.json"

    def build(self, module_progress: dict):
        scorecard = {}
        for k, v in module_progress.items():
            scorecard[k] = {
                "current_pct": v,
                "target_pct": 95,
                "gap_to_95": max(0, 95 - v),
                "already_at_or_above_95": v >= 95,
            }

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "scorecard": scorecard,
            "summary": {
                "total_modules": len(scorecard),
                "at_or_above_95": sum(1 for x in scorecard.values() if x["already_at_or_above_95"]),
                "below_95": sum(1 for x in scorecard.values() if not x["already_at_or_above_95"]),
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🎯 已輸出 target95 scorecard：{self.path}")
        return self.path, payload
