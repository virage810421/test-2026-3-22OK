# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class DeepRiskCheckBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "deep_risk_checks.json"

    def build(self, accepted_signals):
        checks = []
        total_qty = sum(max(0, getattr(s, "target_qty", 0)) for s in accepted_signals) if accepted_signals else 0
        unique_tickers = len(set(getattr(s, "ticker", "") for s in accepted_signals if getattr(s, "ticker", "")))

        checks.append({
            "check": "accepted_signal_count",
            "value": len(accepted_signals),
            "status": "ok"
        })
        checks.append({
            "check": "unique_ticker_count",
            "value": unique_tickers,
            "status": "ok"
        })
        checks.append({
            "check": "total_target_qty",
            "value": total_qty,
            "status": "ok"
        })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "checks": checks,
            "status": "deepcheck_skeleton"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛡️ 已輸出 deep risk checks：{self.path}")
        return self.path, payload
