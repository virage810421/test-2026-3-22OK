# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ModelArtifactCheckBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "model_artifact_checks.json"

    def build(self):
        models_dir = PATHS.base_dir / "models"
        expected_files = [
            models_dir / "selected_features.pkl",
            models_dir / "model_趨勢多頭.pkl",
            models_dir / "model_區間盤整.pkl",
            models_dir / "model_趨勢空頭.pkl",
            models_dir / "selected_features_long.pkl",
            models_dir / "selected_features_short.pkl",
            models_dir / "selected_features_range.pkl",
        ]
        directional_models = list(models_dir.glob('model_long_*.pkl')) + list(models_dir.glob('model_short_*.pkl')) + list(models_dir.glob('model_range_*.pkl'))
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "models_dir_exists": models_dir.exists(),
            "expected_model_files": [{"path": str(p), "exists": p.exists()} for p in expected_files],
            "existing_model_file_count": sum(1 for p in expected_files if p.exists()),
            "directional_model_count": len(directional_models),
            "directional_model_paths_preview": [str(p) for p in directional_models[:12]],
            "status": "artifact_check_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧾 已輸出 model artifact checks：{self.path}")
        return self.path, payload
