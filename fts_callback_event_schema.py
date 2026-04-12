
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, safe_float, safe_int, write_json


class CallbackEventSchema:
    REQUIRED = ['broker_order_id', 'client_order_id', 'event_type', 'status', 'symbol', 'timestamp']

    def normalize(self, raw: dict[str, Any]) -> dict[str, Any]:
        payload = {
            'broker_order_id': str(raw.get('broker_order_id', '')).strip(),
            'client_order_id': str(raw.get('client_order_id', '')).strip(),
            'event_type': str(raw.get('event_type', raw.get('type', ''))).strip().upper(),
            'status': str(raw.get('status', '')).strip().upper(),
            'symbol': str(raw.get('symbol', raw.get('ticker', ''))).strip().upper(),
            'filled_qty': safe_int(raw.get('filled_qty', raw.get('cum_qty', 0)), 0),
            'remaining_qty': safe_int(raw.get('remaining_qty', raw.get('leaves_qty', 0)), 0),
            'avg_fill_price': safe_float(raw.get('avg_fill_price', raw.get('avg_price', 0.0)), 0.0),
            'timestamp': str(raw.get('timestamp', '')).strip(),
            'reject_code': str(raw.get('reject_code', '')).strip(),
            'reject_reason': str(raw.get('reject_reason', '')).strip(),
            'direction_bucket': str(raw.get('direction_bucket', raw.get('approved_pool_type', ''))).strip().upper(),
            'strategy_bucket': str(raw.get('strategy_bucket', '')).strip().upper(),
            'approved_pool_type': str(raw.get('approved_pool_type', '')).strip().upper(),
            'model_scope': str(raw.get('model_scope', '')).strip().upper(),
            'range_confidence': safe_float(raw.get('range_confidence', 0.0), 0.0),
            'raw_payload': raw,
        }
        missing = [k for k in self.REQUIRED if not payload.get(k)]
        payload['valid'] = len(missing) == 0
        payload['missing_fields'] = missing
        return payload

    def build_definition(self) -> tuple[str, dict[str, Any]]:
        payload = {'generated_at': now_str(), 'status': 'callback_event_schema_defined', 'required_fields': list(self.REQUIRED), 'directional_fields': ['direction_bucket','strategy_bucket','approved_pool_type','model_scope','range_confidence']}
        path = PATHS.runtime_dir / 'callback_event_schema.json'
        write_json(path, payload)
        return str(path), payload
