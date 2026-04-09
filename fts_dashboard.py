# -*- coding: utf-8 -*-
import json
from pathlib import Path
from datetime import datetime
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class HealthDashboardBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "health_dashboard.json"

    def _read_json_if_exists(self, path: Path, default):
        try:
            if path.exists():
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
        return default

    def _list_recent_task_logs(self, limit=10):
        task_log_dir = PATHS.runtime_dir / "task_logs"
        if not task_log_dir.exists():
            return []
        files = sorted(task_log_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]
        rows = []
        for p in files:
            rows.append({
                "file": str(p),
                "modified_at": datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "size_bytes": p.stat().st_size,
            })
        return rows

    def build(self, upstream_status=None, upstream_exec=None, retry_queue=None, readiness=None, execution_result=None, positions=None):
        heartbeat = self._read_json_if_exists(PATHS.runtime_dir / "heartbeat.json", {})
        retry_queue = retry_queue or self._read_json_if_exists(PATHS.runtime_dir / "retry_queue.json", {"items": []})
        architecture_map = self._read_json_if_exists(PATHS.runtime_dir / "architecture_map.json", {})
        task_registry = self._read_json_if_exists(PATHS.runtime_dir / "task_registry.json", {})
        recent_task_logs = self._list_recent_task_logs()

        dashboard = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "mode": CONFIG.mode,
            "broker_type": CONFIG.broker_type,
            "heartbeat": heartbeat,
            "architecture_map": architecture_map,
            "task_registry_summary": {
                "total": len(task_registry.get("tasks", [])),
                "etl": sum(1 for x in task_registry.get("tasks", []) if x.get("stage") == "etl"),
                "ai": sum(1 for x in task_registry.get("tasks", []) if x.get("stage") == "ai"),
                "decision": sum(1 for x in task_registry.get("tasks", []) if x.get("stage") == "decision"),
            },
            "upstream_status": upstream_status or {},
            "upstream_exec": upstream_exec or {},
            "retry_queue_summary": {
                "total": len(retry_queue.get("items", [])),
                "pending_retry": sum(1 for x in retry_queue.get("items", []) if x.get("status") == "pending_retry"),
                "resolved": sum(1 for x in retry_queue.get("items", []) if x.get("status") == "resolved"),
            },
            "execution_readiness": readiness or {},
            "execution_result": execution_result or {},
            "positions_summary": {
                "count": len(positions or []),
                "tickers": [p.get("ticker") for p in (positions or [])],
            },
            "recent_task_logs": recent_task_logs,
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(dashboard, f, ensure_ascii=False, indent=2)
        log(f"📊 已輸出 health dashboard：{self.path}")
        return self.path, dashboard
