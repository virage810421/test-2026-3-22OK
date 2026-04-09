# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, write_json


class BrokerRequirementsContract:
    """券商開戶前先定義好 contract，之後只要把實際 API 對上即可。"""

    def build(self) -> tuple[str, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'status': 'broker_requirements_defined_pre_account',
            'must_have_capabilities': [
                'connect / auth / token refresh',
                'place_order / cancel_order / replace_order',
                'query_open_orders / query_fills / query_positions / query_cash',
                'callback receiver or polling fallback',
                'reject code mapping',
                'rate limit handling',
                '現股 / 零股 / 盤後 / 當沖 / 融資融券規則資訊',
            ],
            'request_contract': {
                'required_fields': ['symbol', 'side', 'order_type', 'price', 'qty', 'session', 'time_in_force', 'client_order_id'],
                'optional_fields': ['market', 'strategy_tag', 'regime', 'note'],
            },
            'callback_contract': {
                'required_fields': ['broker_order_id', 'client_order_id', 'event_type', 'status', 'symbol', 'filled_qty', 'remaining_qty', 'timestamp'],
                'optional_fields': ['avg_fill_price', 'reject_code', 'reject_reason', 'raw_payload'],
            },
            'account_contract': {
                'required_fields': ['cash', 'buying_power', 'positions', 'open_orders'],
            },
            'waiting_for_broker_specifics': [
                '券商 API 文件',
                '認證方式',
                'callback / websocket 規格',
                '真實錯誤碼表',
            ],
        }
        path = PATHS.runtime_dir / 'broker_requirements_contract.json'
        write_json(path, payload)
        return str(path), payload
