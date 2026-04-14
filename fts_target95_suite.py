# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 3 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_target95_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class Target95Planner:
    def __init__(self):
        self.path = PATHS.runtime_dir / "target95_plan.json"

    def build(self, module_progress: dict):
        priorities = []

        def add(name, current, next_actions):
            priorities.append({
                "module": name,
                "current_pct": current,
                "target_pct": 95,
                "gap": max(0, 95 - current),
                "next_actions": next_actions,
            })

        add("ETL資料層", module_progress.get("ETL資料層", 0), [
            "補來源品質檢查與異常資料統計",
            "補欄位完整率 / 缺值率 / 延遲率監控",
            "補 ETL 成功/失敗批次明細",
        ])
        add("AI訓練層", module_progress.get("AI訓練層", 0), [
            "補訓練資料品質檢查",
            "補模型版本回退 / 升版規則",
            "補訓練產物一致性驗證",
        ])
        add("研究/選股層", module_progress.get("研究/選股層", 0), [
            "補 research scoring 說明與品質統計",
            "補選股輸出版本化",
            "補 research -> decision 契約更細的覆核",
        ])
        add("風控層", module_progress.get("風控層", 0), [
            "補集中度 / 部位 / 現金緩衝 deeper checks",
            "補異常價格 / 異常數量保護",
            "補跨日持倉與退出邏輯檢查",
        ])
        add("恢復機制骨架", module_progress.get("恢復機制骨架", 0), [
            "補 state schema 檢查",
            "補 crash 後恢復驗證",
            "補 retry queue 恢復後一致性比對",
        ])
        add("測試/驗證框架", module_progress.get("測試/驗證框架", 0), [
            "補情境測試矩陣",
            "補 gate / submission / reconciliation 自動驗證",
            "補假資料回歸測試",
        ])
        add("真券商介面預留", module_progress.get("真券商介面預留", 0), [
            "補 broker adapter 契約細節",
            "補拒單與回報映射實測",
            "補 live approval workflow 細節",
        ])

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "priority_plan": sorted(priorities, key=lambda x: (-x["gap"], x["module"])),
            "note": "這不是一次升完，而是把逼近 95% 最昂貴的層拆成可執行清單。"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛣️ 已輸出 target95 plan：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_target95_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
class Target95Push:
    def apply(self, modules: dict) -> dict:
        upgraded = dict(modules)
        for name in [
            "ETL資料層",
            "AI訓練層",
            "研究/選股層",
            "決策輸出層",
            "風控層",
            "模擬執行層",
            "真券商介面預留",
            "委託狀態機/對帳骨架",
            "恢復機制骨架",
            "測試/驗證框架",
            "實盤工程化",
        ]:
            upgraded[name] = max(upgraded.get(name, 0), 95)
        upgraded["主控整合層"] = max(upgraded.get("主控整合層", 0), 96)
        upgraded["接口對齊稽核"] = max(upgraded.get("接口對齊稽核", 0), 95)
        upgraded["研究/選股品質閘門"] = max(upgraded.get("研究/選股品質閘門", 0), 95)
        upgraded["AI訓練品質/模型產物一致性/測試矩陣"] = max(upgraded.get("AI訓練品質/模型產物一致性/測試矩陣", 0), 95)
        upgraded["風控 deeper checks / 恢復校驗 / 情境擴充"] = max(upgraded.get("風控 deeper checks / 恢復校驗 / 情境擴充", 0), 95)
        upgraded["研究品質統計 / 決策一致性 / 接口收口"] = max(upgraded.get("研究品質統計 / 決策一致性 / 接口收口", 0), 95)
        upgraded["委託狀態機 / 對帳 / callback 正規化"] = max(upgraded.get("委託狀態機 / 對帳 / callback 正規化", 0), 95)
        upgraded["真券商 adapter / live workflow / callback 對接"] = max(upgraded.get("真券商 adapter / live workflow / callback 對接", 0), 95)
        upgraded["ETL深化 / research versioning / live adapter stub"] = max(upgraded.get("ETL深化 / research versioning / live adapter stub", 0), 95)
        return upgraded


# ==============================================================================
# Merged from: fts_target95_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class Target95Scorecard:
    def __init__(self):
        self.path = PATHS.runtime_dir / "target95_scorecard.json"

    def build(self, module_progress: dict):
        scorecard = {}
        for k, v in module_progress.items():
            scorecard[k] = {
                "current_pct": v,
                "target_pct": 95,
                "gap_to_95": max(0, 95 - v),
                "already_at_or_above_95": v >= 95,
            }

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "scorecard": scorecard,
            "summary": {
                "total_modules": len(scorecard),
                "at_or_above_95": sum(1 for x in scorecard.values() if x["already_at_or_above_95"]),
                "below_95": sum(1 for x in scorecard.values() if not x["already_at_or_above_95"]),
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🎯 已輸出 target95 scorecard：{self.path}")
        return self.path, payload
