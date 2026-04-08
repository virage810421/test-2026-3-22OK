# -*- coding: utf-8 -*-
import json
from pathlib import Path
from datetime import datetime
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ModelVersionRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "model_registry.json"

    def build(self):
        models_dir = PATHS.base_dir / "models"
        candidates = []
        if models_dir.exists():
            for p in sorted(models_dir.glob("*")):
                candidates.append({
                    "name": p.name,
                    "path": str(p),
                    "is_file": p.is_file(),
                    "is_dir": p.is_dir(),
                })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "models_dir": str(models_dir),
            "candidates": candidates,
            "candidate_count": len(candidates),
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧬 已輸出 model registry：{self.path}")
        return self.path, payload

class ModelSelectionGate:
    def __init__(self):
        self.path = PATHS.runtime_dir / "model_selection_gate.json"

    def evaluate(self, ai_status: dict, readiness: dict):
        failures = []
        warnings = []

        if not ai_status.get("all_core_scripts_present", False):
            failures.append({
                "type": "ai_core_scripts_missing",
                "message": "AI 訓練核心腳本未齊備"
            })

        if not ai_status.get("training_assets_present", False):
            warnings.append({
                "type": "training_assets_incomplete",
                "message": "訓練資料或 models 資產尚未齊備"
            })

        if readiness.get("total_signals", 0) == 0:
            warnings.append({
                "type": "zero_signal",
                "message": "目前 decision 輸出為 0 筆有效訊號"
            })

        gate = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "go_for_model_linkage": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
            "summary": {
                "failure_count": len(failures),
                "warning_count": len(warnings),
            }
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(gate, f, ensure_ascii=False, indent=2)
        log(
            f"🧪 Model Gate | go_for_model_linkage={gate['go_for_model_linkage']} | "
            f"failures={len(failures)} | warnings={len(warnings)}"
        )
        return self.path, gate
