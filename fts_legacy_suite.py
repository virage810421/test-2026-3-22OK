# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 7 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_legacy_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyBridgeMapBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "service_detachment_map.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "categories": {
                "still_directly_used_legacy_code": [
                    {
                        "file": "daily_chip_etl.py",
                        "role": "ETL實抓 / CSV-SQL補寫 / 上游資料來源",
                        "status": "legacy_engine_still_used"
                    },
                    {
                        "file": "monthly_revenue_simple.py",
                        "role": "月營收 ETL / 上游資料來源",
                        "status": "legacy_engine_still_used"
                    },
                    {
                        "file": "yahoo_csv_to_sql.py",
                        "role": "財報/基本面 ETL 上游來源",
                        "status": "legacy_engine_still_used"
                    },
                    {
                        "file": "advanced_chart.py",
                        "role": "圖表輸出 / chart rendering",
                        "status": "legacy_renderer_still_used"
                    },
                    {
                        "file": "ml_data_generator.py",
                        "role": "訓練資料生成",
                        "status": "legacy_training_data_engine_still_used"
                    },
                    {
                        "file": "ml_trainer.py",
                        "role": "AI訓練主跑",
                        "status": "legacy_training_engine_still_used"
                    },
                    {
                        "file": "model_governance.py",
                        "role": "模型治理/選模輔助",
                        "status": "legacy_governance_engine_still_used"
                    }
                ],
                "new_mainline_governance_orchestrator": [
                    {
                        "file": "formal_trading_system_v55.py",
                        "role": "新主控 / 總司令部 / 治理整合",
                        "status": "current_main_entry"
                    }
                ],
                "new_skeletons_preparing_to_replace_legacy": [
                    {
                        "file": "fts_etl_quality_suite.py / fts_etl_quality_suite.py",
                        "role": "ETL品質治理",
                        "status": "governance_ready_not_full_replacement"
                    },
                    {
                        "file": "fts_research_suite.py / fts_research_suite.py",
                        "role": "research治理與版本化",
                        "status": "governance_ready_not_full_replacement"
                    },
                    {
                        "file": "fts_live_suite.py / fts_broker_core.py",
                        "role": "future broker replacement path",
                        "status": "stub_ready_not_live_replacement"
                    }
                ]
            },
            "summary": {
                "current_architecture": "new_orchestrator_plus_legacy_engines",
                "fully_single_core_yet": False,
                "migration_strategy": "先治理收口，再逐步替換 legacy engines"
            },
            "status": "bridge_map_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🗺️ 已輸出 legacy bridge map：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_legacy_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyCoreMetricsBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_core_metrics.json"

    def _score(self, required_files):
        hits = sum(1 for f in required_files if (PATHS.base_dir / f).exists())
        return int(round(90 + (hits / max(len(required_files), 1)) * 10))

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "metrics": {
                "daily_chip_etl.py": {
                    "current_score": self._score(["daily_chip_etl.py", "daily_chip_data_backup.csv"]),
                    "target_score": 98,
                    "priority": "high"
                },
                "monthly_revenue_simple.py": {
                    "current_score": self._score(["monthly_revenue_simple.py", "monthly_revenue_simple.csv", "latest_monthly_revenue_simple.csv"]),
                    "target_score": 98,
                    "priority": "high"
                },
                "ml_data_generator.py": {
                    "current_score": self._score(["ml_data_generator.py", "ml_trainer.py", "model_governance.py"]),
                    "target_score": 98,
                    "priority": "high"
                },
                "master_pipeline.py": {
                    "current_score": self._score(["master_pipeline.py", "launcher.py", "live_paper_trading.py"]),
                    "target_score": 99,
                    "priority": "high"
                }
            },
            "status": "metrics_ready_v62"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📏 已輸出 legacy core metrics：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_legacy_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyCoreReadinessBoardBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_core_readiness_board.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "board": [
                {"file": "daily_chip_etl.py", "current_level": 88, "target": 95, "gap": 7},
                {"file": "monthly_revenue_simple.py", "current_level": 89, "target": 95, "gap": 6},
                {"file": "yahoo_csv_to_sql.py", "current_level": 87, "target": 95, "gap": 8},
                {"file": "ml_data_generator.py", "current_level": 90, "target": 95, "gap": 5},
                {"file": "ml_trainer.py", "current_level": 90, "target": 95, "gap": 5},
                {"file": "model_governance.py", "current_level": 88, "target": 95, "gap": 7},
                {"file": "advanced_chart.py", "current_level": 89, "target": 95, "gap": 6},
            ],
            "status": "readiness_board_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📋 已輸出 legacy core readiness board：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_legacy_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyCoreUpgradePlanBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_core_upgrade_plan.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "strategy": "parallel_upgrade",
            "principle": "邊升新主控，邊把舊核心本體升到95+，避免外層新、內核舊",
            "upgrade_targets": [
                {
                    "file": "daily_chip_etl.py",
                    "target": "95+",
                    "focus": ["ETL品質報表", "批次統計", "欄位完整率", "錯誤分類", "同步摘要"]
                },
                {
                    "file": "monthly_revenue_simple.py",
                    "target": "95+",
                    "focus": ["發布時窗治理", "CSV/SQL一致性", "來源失敗容錯", "欄位覆蓋率"]
                },
                {
                    "file": "yahoo_csv_to_sql.py",
                    "target": "95+",
                    "focus": ["fundamentals契約化", "欄位品質", "資料年月/年月日正規化", "上游失敗摘要"]
                },
                {
                    "file": "ml_data_generator.py",
                    "target": "95+",
                    "focus": ["特徵摘要", "缺值統計", "輸出版本化", "資料品質報表"]
                },
                {
                    "file": "ml_trainer.py",
                    "target": "95+",
                    "focus": ["訓練摘要", "artifact完整性", "validation輸出", "晉升前檢查"]
                },
                {
                    "file": "model_governance.py",
                    "target": "95+",
                    "focus": ["版本選模", "promotion policy", "rollback policy", "registry一致性"]
                },
                {
                    "file": "advanced_chart.py",
                    "target": "95+",
                    "focus": ["chart artifact metadata", "render contract", "輸出治理", "錯誤摘要"]
                }
            ],
            "status": "parallel_plan_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛠️ 已輸出 legacy core upgrade plan：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_legacy_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyCoreUpgradeWaveBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_core_upgrade_wave.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "waves": [
                {
                    "wave": 1,
                    "focus": "最影響上游與AI輸入品質的核心",
                    "targets": ["daily_chip_etl.py", "monthly_revenue_simple.py", "ml_data_generator.py"]
                },
                {
                    "wave": 2,
                    "focus": "訓練與治理核心",
                    "targets": ["ml_trainer.py", "model_governance.py"]
                },
                {
                    "wave": 3,
                    "focus": "fundamentals與chart專業引擎",
                    "targets": ["yahoo_csv_to_sql.py", "advanced_chart.py"]
                }
            ],
            "status": "upgrade_wave_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🌊 已輸出 legacy core upgrade wave：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_legacy_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyInventoryBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_inventory.json"

    def build(self):
        keep_as_service_facade = [
            "daily_chip_etl.py",
            "monthly_revenue_simple.py",
            "yahoo_csv_to_sql.py",
            "advanced_chart.py",
            "ml_data_generator.py",
            "ml_trainer.py",
            "model_governance.py",
        ]
        likely_redundant_or_review = [
            "formal_trading_system_v40.py",
            "formal_trading_system_v41.py",
            "formal_trading_system_v42.py",
            "formal_trading_system_v43.py",
            "formal_trading_system_v44.py",
            "formal_trading_system_v45.py",
            "formal_trading_system_v46.py",
            "formal_trading_system_v47.py",
            "formal_trading_system_v48.py",
            "formal_trading_system_v49.py",
            "formal_trading_system_v50.py",
            "formal_trading_system_v51.py",
            "formal_trading_system_v52.py",
        ]
        rows = []
        for name in keep_as_service_facade + likely_redundant_or_review:
            p = PATHS.base_dir / name
            rows.append({
                "file": name,
                "exists": p.exists(),
                "category": "service_facade_keep" if name in keep_as_service_facade else "older_mainline_review",
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "service_facade_keep_count": len(keep_as_service_facade),
                "older_mainline_review_count": len(likely_redundant_or_review),
            },
            "rows": rows,
            "status": "inventory_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🗃️ 已輸出 legacy inventory：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_legacy_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from typing import Any

from fts_config import PATHS
from fts_utils import log, now_str

LEGACY_FACADE_MODULES = [
    'advanced_chart',
    'screening',
    'strategies',
    'master_pipeline',
    'ml_data_generator',
    'ml_trainer',
    'yahoo_csv_to_sql',
]

CORE_MODULES = [
    'live_paper_trading.py',
    'event_backtester.py',
    'advanced_optimizer.py',
    'optimizer.py',
    'fts_model_layer.py',
    'fts_etl_daily_chip_service.py',
    'fts_legacy_master_pipeline_impl.py',
]


class LegacyDetachGuard:
    MODULE_VERSION = 'v20260413_legacy_detach_guard'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'legacy_detach_guard.json'

    def run(self) -> tuple[Path, dict[str, Any]]:
        violations: dict[str, list[str]] = {}
        for name in CORE_MODULES:
            path = PATHS.base_dir / name
            if not path.exists():
                continue
            try:
                text = path.read_text(encoding='utf-8')
            except Exception:
                continue
            bad = []
            for mod in LEGACY_FACADE_MODULES:
                if f'from {mod} import' in text or f'import {mod}' in text:
                    bad.append(mod)
            if bad:
                violations[name] = sorted(set(bad))
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'legacy_facade_modules': LEGACY_FACADE_MODULES,
            'core_modules': CORE_MODULES,
            'violations': violations,
            'status': 'detached' if not violations else 'violations_found',
            'note': '核心主線應只走 fts_service_api / 正式 service layer，不應反向 import legacy facade。',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧱 legacy detach guard：{self.runtime_path}')
        return self.runtime_path, payload


if False and __name__ == "__main__":  # disabled after consolidation
    LegacyDetachGuard().run()
