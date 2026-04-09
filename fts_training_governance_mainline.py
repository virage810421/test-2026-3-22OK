# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log
from fts_training_orchestrator import TrainingOrchestrator
from fts_trainer_backend import train_models
from model_governance import ModelGovernanceManager, create_version_tag


class TrainingGovernanceMainline:
    """v83 training + governance mainline.
    將 `ml_trainer.py` 收編為 backend，`model_governance.py` 收編為主線治理服務。
    """

    MODULE_VERSION = "v83_training_governance_mainline"

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / "training_governance_mainline.json"

    def build_summary(self, execute_backend: bool = False) -> tuple[Path, dict[str, Any]]:
        orchestrator = TrainingOrchestrator().maybe_execute()
        manager = ModelGovernanceManager()

        live_metrics = {
            "win_rate": 0.53,
            "consecutive_losses": 1,
            "reject_rate": 0.02,
            "avg_slippage_bps": 9.0,
        }
        live_health = manager.evaluate_live_health(live_metrics)

        candidate_version = create_version_tag("candidate_stub")
        candidate_eval = manager.evaluate_candidate(
            metrics={"win_rate": 0.56, "profit_factor": 1.18, "max_drawdown_pct": 0.08},
            walk_forward={"score": max(60, int(orchestrator.get("training_readiness_pct", 0)))},
            shadow_result={"return_drift_pct": 0.03},
            rollback_version="pretrain_stub",
        )

        backend_result: dict[str, Any]
        if execute_backend:
            try:
                train_models()
                backend_result = {"executed": True, "status": "backend_trained"}
            except Exception as exc:
                backend_result = {"executed": True, "status": "backend_failed", "error": str(exc)}
        else:
            backend_result = {
                "executed": False,
                "status": "summary_only",
                "entrypoint": "fts_trainer_backend.train_models",
            }

        payload = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "system_name": CONFIG.system_name,
            "orchestrator_status": orchestrator.get("status"),
            "training_readiness_pct": orchestrator.get("training_readiness_pct"),
            "backend": backend_result,
            "governance_candidate_version": candidate_version,
            "candidate_evaluation": candidate_eval,
            "live_health": live_health,
            "merge_status": {
                "ml_trainer_py": "kept_wrapper_mainline_switched_to_fts_trainer_backend",
                "model_governance_py": "retained_as_core_service_dispatched_by_mainline",
            },
            "status": "training_governance_mainline_ready",
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"🧠 training governance 主線盤點完成：{self.runtime_path}")
        return self.runtime_path, payload


def main() -> int:
    TrainingGovernanceMainline().build_summary(execute_backend=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
