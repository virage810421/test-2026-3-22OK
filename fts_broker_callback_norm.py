
# -*- coding: utf-8 -*-
from __future__ import annotations
from fts_prelive_runtime import PATHS, now_str, write_json

class BrokerCallbackNormalizer:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'broker_callback_normalization.json'

    def build(self):
        payload = {
            'generated_at': now_str(),
            'required_fields': ['broker_order_id', 'status'],
            'optional_fields': ['filled_qty','avg_fill_price','symbol','side','reject_reason','event_time','direction_bucket','strategy_bucket','approved_pool_type','model_scope','range_confidence'],
            'normalized_status_map': {
                'NEW': ['NEW','ACK'],
                'SUBMITTED': ['SUBMITTED','ACCEPTED'],
                'PARTIALLY_FILLED': ['PARTIALLY_FILLED','PARTIAL'],
                'FILLED': ['FILLED','DONE'],
                'CANCELLED': ['CANCELLED','CANCELED'],
                'REJECTED': ['REJECTED','ERROR'],
            },
            'status': 'directional_callback_norm_defined',
        }
        write_json(self.path, payload)
        return self.path, payload
