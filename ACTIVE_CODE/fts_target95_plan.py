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
