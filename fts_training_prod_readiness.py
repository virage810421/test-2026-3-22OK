# -*- coding: utf-8 -*-
import json
from pathlib import Path

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class TrainingProdReadinessBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "training_prod_readiness.json"
        self.required_model_files = [
            PATHS.model_dir / "selected_features.pkl",
            PATHS.model_dir / "model_趨勢多頭.pkl",
            PATHS.model_dir / "model_區間盤整.pkl",
            PATHS.model_dir / "model_趨勢空頭.pkl",
        ]
        self.directional_feature_files = [
            PATHS.model_dir / "selected_features_long.pkl",
            PATHS.model_dir / "selected_features_short.pkl",
            PATHS.model_dir / "selected_features_range.pkl",
        ]

    def _exists(self, p: Path) -> bool:
        return p.exists() and p.is_file()

    def build(self):
        training_data_exists = (PATHS.data_dir / "ml_training_data.csv").exists()
        policy_exists = (PATHS.runtime_dir / "trainer_promotion_policy.json").exists()
        decision_exists = any(p.exists() for p in PATHS.decision_csv_candidates)
        model_count = sum(1 for p in self.required_model_files if self._exists(p))
        directional_feature_count = sum(1 for p in self.directional_feature_files if self._exists(p))
        directional_model_count = len(list(PATHS.model_dir.glob('model_long_*.pkl')) + list(PATHS.model_dir.glob('model_short_*.pkl')) + list(PATHS.model_dir.glob('model_range_*.pkl')))
        checks = [
            {"area": "training_data_generation", "ready": training_data_exists, "level": "governed" if training_data_exists else "missing"},
            {"area": "trainer_entrypoint", "ready": (PATHS.base_dir / "ml_trainer.py").exists(), "level": "bridge_ready"},
            {"area": "shared_model_artifact_output", "ready": model_count >= 2, "level": "governed" if model_count == len(self.required_model_files) else "partial"},
            {"area": "directional_selected_feature_output", "ready": directional_feature_count == 3, "level": "governed" if directional_feature_count == 3 else "partial"},
            {"area": "directional_lane_model_output", "ready": directional_model_count >= 3, "level": "governed" if directional_model_count >= 9 else "partial"},
            {"area": "model_version_registry", "ready": (PATHS.base_dir / "model_governance.py").exists(), "level": "governed"},
            {"area": "training_quality_report", "ready": (PATHS.runtime_dir / "training_orchestrator.json").exists(), "level": "governed"},
            {"area": "walk_forward_validation", "ready": policy_exists, "level": "policy_ready" if policy_exists else "missing"},
            {"area": "automatic rollback policy", "ready": policy_exists, "level": "policy_ready" if policy_exists else "missing"},
            {"area": "live shadow evaluation", "ready": decision_exists, "level": "bridge_ready" if decision_exists else "missing"},
            {"area": "data drift monitoring", "ready": (PATHS.base_dir / "fts_ai_quality.py").exists(), "level": "bridge_ready"},
            {"area": "promotion gate for live deployment", "ready": policy_exists, "level": "policy_ready" if policy_exists else "missing"},
        ]
        ready_count = sum(1 for c in checks if c["ready"])
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "ready_count": ready_count,
                "total_checks": len(checks),
                "readiness_ratio": round(ready_count / len(checks), 4),
                "overall_conclusion": "prelive_grade" if ready_count >= 8 else "not_live_grade_yet",
                "required_model_count": model_count,
                "directional_feature_count": directional_feature_count,
                "directional_model_count": directional_model_count,
            },
            "checks": checks,
            "status": "training_readiness_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🎓 已輸出 training prod readiness：{self.path}")
        return self.path, payload
