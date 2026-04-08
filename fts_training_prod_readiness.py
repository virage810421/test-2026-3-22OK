# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class TrainingProdReadinessBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "training_prod_readiness.json"

    def build(self):
        checks = [
            {"area": "training_data_generation", "ready": True, "level": "bridge_ready"},
            {"area": "trainer_entrypoint", "ready": True, "level": "bridge_ready"},
            {"area": "model_artifact_output", "ready": True, "level": "governed"},
            {"area": "model_version_registry", "ready": True, "level": "governed"},
            {"area": "training_quality_report", "ready": True, "level": "governed"},
            {"area": "walk_forward_validation", "ready": False, "level": "not_strong_enough"},
            {"area": "automatic rollback policy", "ready": False, "level": "not_strong_enough"},
            {"area": "live shadow evaluation", "ready": False, "level": "not_strong_enough"},
            {"area": "data drift monitoring", "ready": False, "level": "not_strong_enough"},
            {"area": "promotion gate for live deployment", "ready": False, "level": "not_strong_enough"},
        ]
        ready_count = sum(1 for c in checks if c["ready"])
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "ready_count": ready_count,
                "total_checks": len(checks),
                "readiness_ratio": round(ready_count / len(checks), 4),
                "overall_conclusion": "not_live_grade_yet"
            },
            "checks": checks,
            "status": "training_readiness_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🎓 已輸出 training prod readiness：{self.path}")
        return self.path, payload
