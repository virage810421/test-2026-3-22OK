# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LaunchGatekeeper:
    def __init__(self):
        self.path = PATHS.runtime_dir / "launch_gate.json"

    def evaluate(self, upstream_status, upstream_exec, retry_queue, compat_info, readiness):
        failures = []
        warnings = []

        missing_required = [x for x in upstream_status.get("missing", []) if x.get("required")]
        if missing_required:
            failures.append({
                "type": "missing_required_upstream",
                "count": len(missing_required),
                "items": missing_required[:20],
            })

        failed_required = [x for x in upstream_exec.get("failed", []) if x.get("required")]
        if failed_required:
            failures.append({
                "type": "failed_required_upstream",
                "count": len(failed_required),
                "items": failed_required[:20],
            })

        pending_required_retry = [
            x for x in retry_queue.get("items", [])
            if x.get("required") and x.get("status") == "pending_retry"
        ]
        if pending_required_retry:
            failures.append({
                "type": "required_retry_queue_pending",
                "count": len(pending_required_retry),
                "items": pending_required_retry[:20],
            })

        if compat_info.get("row_count", 0) == 0:
            failures.append({
                "type": "empty_decision_after_normalize",
                "count": 1,
                "items": [],
            })

        if readiness.get("total_signals", 0) == 0:
            warnings.append({
                "type": "zero_signal",
                "count": 1,
                "items": [],
            })

        if compat_info.get("rows_with_price", 0) == 0:
            failures.append({
                "type": "decision_price_missing",
                "count": 1,
                "items": [],
            })

        if compat_info.get("rows_with_ticker", 0) == 0:
            failures.append({
                "type": "decision_ticker_missing",
                "count": 1,
                "items": [],
            })

        if compat_info.get("rows_with_action", 0) == 0:
            failures.append({
                "type": "decision_action_missing",
                "count": 1,
                "items": [],
            })

        gate = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "go_for_execution": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
            "summary": {
                "failure_count": len(failures),
                "warning_count": len(warnings),
                "signal_count": readiness.get("total_signals", 0),
            }
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(gate, f, ensure_ascii=False, indent=2)

        log(
            f"🚦 Launch Gate | go_for_execution={gate['go_for_execution']} | "
            f"failures={len(failures)} | warnings={len(warnings)}"
        )
        return gate
