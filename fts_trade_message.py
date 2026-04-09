# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class TradeMessageSummaryBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "trade_message_summary.json"

    def build(self, accepted_signals, rejected_pairs, execution_result: dict):
        accepted_rows = []
        for s in accepted_signals[:20]:
            accepted_rows.append({
                "ticker": getattr(s, "ticker", ""),
                "action": getattr(s, "action", ""),
                "target_qty": getattr(s, "target_qty", 0),
                "reference_price": getattr(s, "reference_price", 0),
                "strategy_name": getattr(s, "strategy_name", ""),
                "regime": getattr(s, "regime", ""),
            })

        rejected_rows = []
        for s, reason in rejected_pairs[:20]:
            rejected_rows.append({
                "ticker": getattr(s, "ticker", ""),
                "action": getattr(s, "action", ""),
                "reason": str(reason),
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "accepted_preview": accepted_rows,
            "rejected_preview": rejected_rows,
            "execution_summary": {
                "submitted": execution_result.get("submitted", 0),
                "filled": execution_result.get("filled", 0),
                "partially_filled": execution_result.get("partially_filled", 0),
                "rejected": execution_result.get("rejected", 0),
                "cancelled": execution_result.get("cancelled", 0),
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"💬 已輸出 trade message summary：{self.path}")
        return self.path, payload
