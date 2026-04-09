# -*- coding: utf-8 -*-
import json
from dataclasses import asdict
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class OrderPayloadBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "order_payload_preview.json"

    def build_preview(self, accepted_signals):
        payloads = []
        for s in accepted_signals[:20]:
            payloads.append({
                "ticker": s.ticker,
                "action": s.action,
                "target_qty": s.target_qty,
                "reference_price": s.reference_price,
                "order_type": "LIMIT",
                "time_in_force": "DAY",
                "strategy_name": s.strategy_name,
                "regime": s.regime,
                "expected_return": s.expected_return,
                "kelly_fraction": s.kelly_fraction,
            })

        out = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "preview_count": len(payloads),
            "payloads": payloads,
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        log(f"🧾 已輸出 order payload preview：{self.path}")
        return self.path, out
