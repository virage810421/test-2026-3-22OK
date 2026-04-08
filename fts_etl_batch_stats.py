# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ETLBatchStatsBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "etl_batch_stats.json"

    def build(self, upstream_exec: dict):
        executed = upstream_exec.get("executed", [])
        failed = upstream_exec.get("failed", [])
        skipped = upstream_exec.get("skipped", [])

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "stats": {
                "executed_count": len(executed),
                "failed_count": len(failed),
                "skipped_count": len(skipped),
                "success_ratio": round(len(executed) / max(1, (len(executed) + len(failed))), 4),
            },
            "executed_preview": executed[:10],
            "failed_preview": failed[:10],
            "skipped_preview": skipped[:10],
            "status": "batch_stats_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"📦 已輸出 etl batch stats：{self.path}")
        return self.path, payload
