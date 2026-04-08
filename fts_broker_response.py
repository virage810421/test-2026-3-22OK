# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class BrokerResponseNormalizer:
    def __init__(self):
        self.path = PATHS.runtime_dir / "broker_response_contract.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "normalized_status_map": {
                "NEW": ["NEW", "PENDING_SUBMIT"],
                "SUBMITTED": ["SUBMITTED", "ACCEPTED"],
                "PARTIALLY_FILLED": ["PARTIALLY_FILLED", "PARTIAL"],
                "FILLED": ["FILLED", "DONE"],
                "CANCELLED": ["CANCELLED", "CANCELED"],
                "REJECTED": ["REJECTED", "ERROR"],
            },
            "required_response_fields": [
                "broker_order_id",
                "status",
            ],
            "optional_response_fields": [
                "filled_qty",
                "avg_fill_price",
                "reject_reason",
                "updated_at",
            ],
            "status": "normalized_contract_defined",
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📬 已輸出 broker response contract：{self.path}")
        return self.path, payload
