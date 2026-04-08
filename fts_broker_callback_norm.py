# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class BrokerCallbackNormalizer:
    def __init__(self):
        self.path = PATHS.runtime_dir / "broker_callback_normalization.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "required_fields": [
                "broker_order_id",
                "status",
            ],
            "optional_fields": [
                "filled_qty",
                "avg_fill_price",
                "symbol",
                "side",
                "reject_reason",
                "event_time",
            ],
            "normalized_status_map": {
                "NEW": ["NEW", "ACK"],
                "SUBMITTED": ["SUBMITTED", "ACCEPTED"],
                "PARTIALLY_FILLED": ["PARTIALLY_FILLED", "PARTIAL"],
                "FILLED": ["FILLED", "DONE"],
                "CANCELLED": ["CANCELLED", "CANCELED"],
                "REJECTED": ["REJECTED", "ERROR"],
            },
            "status": "callback_norm_defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📨 已輸出 broker callback normalization：{self.path}")
        return self.path, payload
