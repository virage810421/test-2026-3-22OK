# -*- coding: utf-8 -*-
from pathlib import Path
from fts_config import PATHS
from fts_utils import log

class UpstreamOrchestrator:
    """
    先不直接執行你的上游腳本，只做：
    1. 註冊
    2. 檢查存在
    3. 回報哪些上游任務已就位
    下一輪再接實際排程/執行
    """
    def check_tasks(self, task_registry_summary):
        result = {
            "ready": [],
            "missing": [],
        }
        for task in task_registry_summary:
            target = PATHS.base_dir / task["script"]
            # decision_builder 可能是 CSV，不一定是 .py
            exists = target.exists()
            row = {
                "stage": task["stage"],
                "name": task["name"],
                "script": task["script"],
                "exists": exists,
                "required": task["required"],
            }
            if exists:
                result["ready"].append(row)
            else:
                result["missing"].append(row)

        log(f"🛰️ 上游任務檢查 | ready={len(result['ready'])} | missing={len(result['missing'])}")
        for row in result["missing"][:20]:
            tag = "必要" if row["required"] else "可選"
            log(f"   - 缺少 [{tag}] {row['stage']} / {row['name']} / {row['script']}")
        return result
