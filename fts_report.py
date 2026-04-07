# -*- coding: utf-8 -*-
import json
from dataclasses import asdict
from datetime import datetime
from fts_config import PATHS, CONFIG
from fts_utils import now_str

class ReportBuilder:
    def __init__(self, progress_tracker, version_policy):
        self.progress_tracker = progress_tracker
        self.version_policy = version_policy

    def save(
        self,
        package_check,
        compat_info,
        readiness,
        test_results,
        accepted,
        rejected,
        execution_result,
        account_snapshot,
        positions,
        decision_path,
        stage_results=None,
        runtime_manifest=None,
        legacy_bridge_info=None,
        recovery_info=None,
    ):
        payload = {
            "run_time": now_str(),
            "system_name": CONFIG.system_name,
            "mode": CONFIG.mode,
            "broker_type": CONFIG.broker_type,
            "decision_path": str(decision_path),
            "progress_dashboard": self.progress_tracker.summary(),
            "version_policy": self.version_policy.summary(),
            "package_check": package_check,
            "compat_info": compat_info,
            "preflight_tests": test_results,
            "execution_readiness": readiness,
            "accepted_signals": [asdict(x) for x in accepted],
            "rejected_signals": [{"signal": asdict(s), "reason": reason} for s, reason in rejected],
            "execution_result": execution_result,
            "account_snapshot": asdict(account_snapshot),
            "positions": [asdict(x) for x in positions.values()],
            "stage_results": stage_results or {},
            "runtime_manifest": runtime_manifest or {},
            "legacy_bridge_info": legacy_bridge_info or {},
            "recovery_info": recovery_info or {},
        }
        filename = PATHS.log_dir / f"formal_trading_system_v17_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return filename
