
# -*- coding: utf-8 -*-
from __future__ import annotations
from fts_prelive_runtime import PATHS, now_str, write_json

class BrokerSubmissionContract:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'broker_submission_contract.json'

    def build(self):
        payload = {
            'generated_at': now_str(),
            'submission_contract': {
                'required_fields': ['ticker','action','target_qty','reference_price'],
                'optional_fields': ['order_type','time_in_force','strategy_name','regime','expected_return','kelly_fraction','direction_bucket','strategy_bucket','approved_pool_type','model_scope','range_confidence'],
                'default_order_type': 'LIMIT',
                'default_time_in_force': 'DAY',
                'status': 'directional_contract_defined',
            }
        }
        write_json(self.path, payload)
        return self.path, payload
