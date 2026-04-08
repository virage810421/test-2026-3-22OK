# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class TrainerPromotionPolicyBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "trainer_promotion_policy.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "promotion_policy": {
                "stages": [
                    "offline_training_pass",
                    "walk_forward_validation_pass",
                    "artifact_integrity_pass",
                    "registry_update_pass",
                    "shadow_live_observation_pass",
                    "operator_approval_pass",
                    "paper_live_safe_pass"
                ],
                "minimum_requirements": [
                    "模型產物完整",
                    "版本號已更新",
                    "驗證報告存在",
                    "未觸發風險阻擋",
                    "可回退版本存在"
                ],
                "deployment_rule": "not_promote_to_live_without_all_required_stages"
            },
            "status": "promotion_policy_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🚦 已輸出 trainer promotion policy：{self.path}")
        return self.path, payload
