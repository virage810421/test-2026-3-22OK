# -*- coding: utf-8 -*-
import json
from collections import Counter
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class RiskLimitsPlusBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "risk_limits_plus.json"

    def build(self, accepted_signals):
        tickers = [getattr(s, "ticker", "") for s in accepted_signals if getattr(s, "ticker", "")]
        ticker_counts = Counter(tickers)
        duplicate_tickers = {k: v for k, v in ticker_counts.items() if v > 1}

        total_qty = sum(max(0, getattr(s, "target_qty", 0)) for s in accepted_signals)
        total_notional_proxy = sum(
            max(0, getattr(s, "target_qty", 0)) * max(0, getattr(s, "reference_price", 0))
            for s in accepted_signals
        )

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "accepted_signal_count": len(accepted_signals),
                "unique_ticker_count": len(set(tickers)),
                "duplicate_ticker_count": len(duplicate_tickers),
                "total_qty": total_qty,
                "total_notional_proxy": total_notional_proxy,
            },
            "duplicate_tickers": duplicate_tickers,
            "status": "deeper_risk_skeleton"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🛡️ 已輸出 risk limits plus：{self.path}")
        return self.path, payload
