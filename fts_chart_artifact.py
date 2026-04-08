# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartArtifactRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_artifact_registry.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "artifact_policy": {
                "source_mode": "legacy_bridge_primary",
                "expected_artifacts": [
                    "advanced_chart output",
                    "research chart png/html artifact",
                    "decision-related chart summary"
                ],
                "governance": {
                    "registry_enabled": True,
                    "contract_defined": True,
                    "status": "governed_not_fully_replatformed"
                }
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🖼️ 已輸出 chart artifact registry：{self.path}")
        return self.path, payload
