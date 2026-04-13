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
                'required_fields': ['ticker', 'action', 'target_qty', 'reference_price', 'client_order_id'],
                'optional_fields': [
                    'order_type', 'time_in_force', 'strategy_name', 'regime', 'expected_return',
                    'kelly_fraction', 'direction_bucket', 'strategy_bucket', 'approved_pool_type',
                    'model_scope', 'range_confidence', 'note', 'session', 'market', 'price'
                ],
                'callback_required_fields': [
                    'broker_order_id', 'client_order_id', 'event_type', 'status', 'symbol',
                    'filled_qty', 'remaining_qty', 'timestamp'
                ],
                'reconciliation_required_fields': ['cash', 'positions', 'open_orders', 'fills'],
                'default_order_type': 'LIMIT',
                'default_time_in_force': 'DAY',
                'status': 'broker_contract_defined',
            },
            'capabilities_required': [
                'connect', 'place_order', 'cancel_order', 'replace_order', 'get_order_status',
                'query_open_orders', 'query_positions', 'query_cash', 'get_fills', 'poll_callbacks', 'reconcile'
            ],
        }
        write_json(self.path, payload)
        return self.path, payload
