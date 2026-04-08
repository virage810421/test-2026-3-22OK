# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class BrokerAdapterContractBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "broker_adapter_contract.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "required_methods": [
                "place_order",
                "cancel_order",
                "get_order_status",
                "get_positions",
                "get_cash",
                "get_account_snapshot",
            ],
            "required_order_fields": [
                "ticker",
                "action",
                "target_qty",
                "reference_price",
            ],
            "optional_order_fields": [
                "order_type",
                "limit_price",
                "time_in_force",
                "strategy_name",
                "risk_tag",
            ],
            "status": "adapter_contract_defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔌 已輸出 broker adapter contract：{self.path}")
        return self.path, payload
