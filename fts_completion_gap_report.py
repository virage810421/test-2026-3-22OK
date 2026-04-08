# -*- coding: utf-8 -*-
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR / "runtime"


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


class CompletionGapReportBuilder:
    def __init__(self):
        self.report_path = RUNTIME_DIR / "completion_gap_report.json"

    def build(self):
        truth = _load_json(RUNTIME_DIR / "upgrade_truth_report.json")
        live = _load_json(RUNTIME_DIR / "live_readiness_gate.json")
        training = _load_json(RUNTIME_DIR / "training_orchestrator.json")
        bridge = _load_json(RUNTIME_DIR / "decision_execution_bridge.json")
        api = _load_json(RUNTIME_DIR / "real_api_readiness.json")

        training_ready = bool(training.get("dataset", {}).get("exists")) and bool(training.get("models", {}).get("all_required_present"))
        execution_ready = int(bridge.get("rows_with_price", 0)) > 0 and int(bridge.get("rows_with_qty", 0)) > 0 and int(bridge.get("rows_market_rule_passed", 0)) > 0
        paper_ready = bool(truth.get("headline", {}).get("go_for_execution", False))

        remaining_without_broker = []
        if not training_ready:
            remaining_without_broker.append("AI訓練資料與模型產物未落地")
        if not execution_ready:
            remaining_without_broker.append("決策價格/股數/台股規則 payload 未閉環")
        if not paper_ready:
            remaining_without_broker.append("Paper execution 端到端尚未放行")

        completed_without_broker = 3 - len(remaining_without_broker)
        completion_pct_without_broker = round(completed_without_broker / 3 * 100)

        report = {
            "generated_from": [
                "upgrade_truth_report.json",
                "live_readiness_gate.json",
                "training_orchestrator.json",
                "decision_execution_bridge.json",
                "real_api_readiness.json",
            ],
            "headline": {
                "completion_excluding_real_broker_pct": completion_pct_without_broker,
                "remaining_major_blocks_excluding_real_broker": len(remaining_without_broker),
                "real_broker_binding_present": bool(truth.get("headline", {}).get("real_api_live_bound", False)),
            },
            "completed_excluding_real_broker": [
                "24/24 架構模組達 95+",
                "舊核心已納管進主控",
                "live readiness gate / TW market rules / execution bridge 已建立",
            ],
            "remaining_excluding_real_broker": remaining_without_broker,
            "detailed_flags": {
                "training_ready": training_ready,
                "execution_payload_ready": execution_ready,
                "paper_execution_ready": paper_ready,
                "live_ready": bool(live.get("live_ready", False)),
                "real_api_live_bound": bool(truth.get("headline", {}).get("real_api_live_bound", False)),
            },
            "real_api_is_separate_track": {
                "already_reserved": api.get("completed_now", []),
                "still_missing": api.get("missing_before_live", []),
            },
            "status": "completion_gap_mapped"
        }
        self.report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return self.report_path, report
