# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class TrainingGapReportBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "training_gap_report.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "gaps": [
                {
                    "gap": "walk_forward_validation_not_formally_enforced",
                    "impact": "high",
                    "why_it_matters": "避免只靠單一切分或過度擬合結果上線"
                },
                {
                    "gap": "no_formal_model_promotion_policy",
                    "impact": "high",
                    "why_it_matters": "模型不能只因為一次訓練分數較高就直接晉升"
                },
                {
                    "gap": "no_shadow_live_observation_phase",
                    "impact": "high",
                    "why_it_matters": "缺少實盤前的觀察區，無法先驗證訊號穩定性"
                },
                {
                    "gap": "drift_monitoring_not_strong_enough",
                    "impact": "medium",
                    "why_it_matters": "資料分布變化可能讓舊模型失效"
                },
                {
                    "gap": "rollback_policy_not_formally_bound",
                    "impact": "medium",
                    "why_it_matters": "上線後若異常，缺少明確回退準則"
                }
            ],
            "status": "gap_report_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧯 已輸出 training gap report：{self.path}")
        return self.path, payload
