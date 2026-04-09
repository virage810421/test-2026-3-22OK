# -*- coding: utf-8 -*-
import json
from pathlib import Path
from datetime import datetime
from fts_config import PATHS
from fts_utils import now_str, log

class TaskLogArchiver:
    def __init__(self):
        self.base_dir = PATHS.runtime_dir / "task_logs"
        self.base_dir.mkdir(exist_ok=True)

    def write_result(self, task_name: str, stage: str, payload: dict):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = f"{stage}_{task_name}".replace("/", "_").replace("\\", "_").replace(" ", "_")
        path = self.base_dir / f"{safe_name}_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🗃️ 已封存 task log：{path}")
        return path
