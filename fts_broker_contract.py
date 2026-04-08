# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class BrokerSubmissionContract:
    def __init__(self):
        self.path = PATHS.runtime_dir / "broker_submission_contract.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "submission_contract": {
                "required_fields": [
                    "ticker",
                    "action",
                    "target_qty",
                    "reference_price",
                ],
                "optional_fields": [
                    "order_type",
                    "time_in_force",
                    "strategy_name",
                    "regime",
                    "expected_return",
                    "kelly_fraction",
                ],
                "default_order_type": "LIMIT",
                "default_time_in_force": "DAY",
                "status": "defined_not_live_bound",
                "notes": "v36 先把真券商提交契約正式定義出來，尚未綁定特定券商 API。"
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📨 已輸出 broker submission contract：{self.path}")
        return self.path, payload
