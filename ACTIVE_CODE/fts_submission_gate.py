# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class SubmissionContractGate:
    def __init__(self):
        self.path = PATHS.runtime_dir / "submission_contract_gate.json"

    def evaluate(self, accepted_signals):
        failures = []
        warnings = []

        if len(accepted_signals) == 0:
            warnings.append({
                "type": "no_accepted_signals",
                "message": "目前沒有 accepted signals 可供生成 broker payload"
            })

        for s in accepted_signals[:50]:
            missing = []
            if not getattr(s, "ticker", None):
                missing.append("ticker")
            if not getattr(s, "action", None):
                missing.append("action")
            if getattr(s, "target_qty", 0) <= 0:
                missing.append("target_qty")
            if getattr(s, "reference_price", 0) <= 0:
                missing.append("reference_price")

            if missing:
                failures.append({
                    "ticker": getattr(s, "ticker", ""),
                    "missing_fields": missing,
                })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "go_for_submission_contract": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
            "summary": {
                "accepted_signal_count": len(accepted_signals),
                "failure_count": len(failures),
                "warning_count": len(warnings),
            }
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(
            f"📦 Submission Contract Gate | go_for_submission_contract={payload['go_for_submission_contract']} | "
            f"failures={len(failures)} | warnings={len(warnings)}"
        )
        return self.path, payload
