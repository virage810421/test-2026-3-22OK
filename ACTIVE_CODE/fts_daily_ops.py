# -*- coding: utf-8 -*-
import json
from pathlib import Path
from datetime import datetime
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class DailyOpsSummaryBuilder:
    def __init__(self):
        self.summary_path = PATHS.runtime_dir / "daily_ops_summary.json"
        self.alerts_path = PATHS.runtime_dir / "alerts.json"

    def _flag_alerts(self, dashboard: dict):
        alerts = []

        heartbeat = dashboard.get("heartbeat", {})
        hb_stage = heartbeat.get("stage")
        if hb_stage == "crash":
            alerts.append({"level": "critical", "type": "heartbeat_crash", "message": "heartbeat 顯示上次執行發生 crash"})

        retry = dashboard.get("retry_queue_summary", {})
        if retry.get("pending_retry", 0) > 0:
            alerts.append({
                "level": "warning",
                "type": "pending_retry",
                "message": f"retry queue 尚有 {retry.get('pending_retry', 0)} 筆待補跑"
            })

        upstream_exec = dashboard.get("upstream_exec", {})
        if len(upstream_exec.get("failed", [])) > 0:
            alerts.append({
                "level": "warning",
                "type": "upstream_failed",
                "message": f"本輪上游任務失敗 {len(upstream_exec.get('failed', []))} 筆"
            })

        readiness = dashboard.get("execution_readiness", {})
        if readiness.get("total_signals", 0) == 0:
            alerts.append({
                "level": "warning",
                "type": "zero_signal",
                "message": "本輪有效訊號為 0"
            })

        execution_result = dashboard.get("execution_result", {})
        if execution_result.get("rejected", 0) > 0:
            alerts.append({
                "level": "info",
                "type": "rejected_orders",
                "message": f"本輪有 {execution_result.get('rejected', 0)} 筆委託被拒"
            })

        return alerts

    def build(self, dashboard: dict):
        alerts = self._flag_alerts(dashboard)

        summary = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "mode": CONFIG.mode,
            "broker_type": CONFIG.broker_type,
            "headline": {
                "pending_retry": dashboard.get("retry_queue_summary", {}).get("pending_retry", 0),
                "signals": dashboard.get("execution_readiness", {}).get("total_signals", 0),
                "filled": dashboard.get("execution_result", {}).get("filled", 0),
                "partial": dashboard.get("execution_result", {}).get("partially_filled", 0),
                "auto_exit": dashboard.get("execution_result", {}).get("auto_exit_signals", 0),
                "positions": dashboard.get("positions_summary", {}).get("count", 0),
                "recent_task_logs": len(dashboard.get("recent_task_logs", [])),
                "alerts": len(alerts),
            },
            "alerts": alerts,
        }

        with open(self.summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        with open(self.alerts_path, "w", encoding="utf-8") as f:
            json.dump({"generated_at": now_str(), "alerts": alerts}, f, ensure_ascii=False, indent=2)

        log(f"📝 已輸出 daily ops summary：{self.summary_path}")
        log(f"🚨 已輸出 alerts：{self.alerts_path}")
        return self.summary_path, self.alerts_path, summary
