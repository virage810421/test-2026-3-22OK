# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 3 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_training_quality_suite.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_training_quality_suite.py
# ==============================================================================
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
            {"area": "data drift monitoring", "ready": (PATHS.base_dir / "fts_operations_suite.py").exists(), "level": "bridge_ready"},
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


# ==============================================================================
# Merged from: fts_training_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log
from model_governance import ModelGovernanceManager


class TrainingStressAudit:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'training_stress_audit.json'
        self.backend_path = PATHS.runtime_dir / 'trainer_backend_report.json'

    def build(self) -> tuple[Path, dict[str, Any]]:
        if self.backend_path.exists():
            report = json.loads(self.backend_path.read_text(encoding='utf-8'))
        else:
            report = {}
        integrity = ModelGovernanceManager().evaluate_training_integrity(report or {'leakage_guards': {}, 'out_of_time': {}, 'overfit_gap': 1.0, 'feature_to_sample_ratio': 1.0})
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'trainer_backend_report_exists': self.backend_path.exists(),
            'integrity': integrity,
            'key_findings': {
                'overfit_gap': float(report.get('overfit_gap', 0.0) or 0.0),
                'oot_hit_rate': float(report.get('out_of_time', {}).get('hit_rate', 0.0) or 0.0),
                'oot_profit_factor': float(report.get('out_of_time', {}).get('profit_factor', 0.0) or 0.0),
                'feature_to_sample_ratio': float(report.get('feature_to_sample_ratio', 0.0) or 0.0),
            },
            'status': integrity.get('status', 'training_integrity_blocked'),
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧪 已輸出 training stress audit：{self.path}')
        return self.path, payload
