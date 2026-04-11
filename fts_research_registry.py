# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class ResearchSelectionRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_selection_registry.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "research_selection_layer": {
                "role": "研究層/選股層候選輸出登錄，不直接覆蓋真倉",
                "current_status": "registered_and_isolated_from_live",
                "isolation_rules": [
                    "candidate params 與 approved params 分離",
                    "candidate features 不覆蓋 models/selected_features.pkl",
                    "alpha 候選需經 validation/OOT/promotion",
                    "研究模組不得直接寫 production config 或正式模型檔"
                ],
                "merged_old_modules": {
                    "research_only": [
                        "advanced_optimizer.py", "optimizer.py", "auto_optimizer.py",
                        "feature_selector.py", "alpha_miner.py"
                    ],
                    "serviceized_into_mainline": [
                        "market_language.py", "kline_cache.py", "param_storage.py"
                    ]
                },
                "artifact_root": str(PATHS.runtime_dir / 'research_lab'),
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔎 已輸出 research selection registry：{self.path}")
        return self.path, payload
