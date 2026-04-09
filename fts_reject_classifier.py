# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class RejectReasonClassifier:
    def __init__(self):
        self.path = PATHS.runtime_dir / "reject_reason_classifier.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "categories": {
                "RISK_LIMIT": ["position limit", "risk limit", "exposure", "cash buffer"],
                "BAD_PAYLOAD": ["missing field", "invalid qty", "invalid price", "schema"],
                "BROKER_REJECT": ["broker reject", "rejected", "exchange reject"],
                "MARKET_RULE": ["price band", "tick rule", "trading halt"],
                "UNKNOWN": ["unknown", "unclassified"]
            },
            "status": "defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🚫 已輸出 reject reason classifier：{self.path}")
        return self.path, payload
