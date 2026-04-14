# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 8 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
from paper_broker import PaperBroker


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
# -*- coding: utf-8 -*-
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


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
# -*- coding: utf-8 -*-
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


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
# -*- coding: utf-8 -*-
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
                'reconciliation api',
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
            'fill_contract': {
                'required_fields': ['fill_id', 'broker_order_id', 'client_order_id', 'symbol', 'side', 'fill_qty', 'fill_price', 'fill_time'],
                'optional_fields': ['commission', 'tax', 'liquidity_flag'],
            },
            'account_contract': {
                'required_fields': ['cash', 'buying_power', 'positions', 'open_orders'],
            },
            'reconcile_contract': {
                'required_fields': ['as_of', 'cash', 'positions', 'open_orders', 'fills', 'callback_backlog'],
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


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
# -*- coding: utf-8 -*-
from fts_config import CONFIG
# merged-local import removed: from fts_broker_core import PaperBroker
from fts_broker_real_stub import RealBrokerStub
from fts_broker_api_adapter import ConfigurableBrokerAdapter


def create_broker():
    broker_type = str(getattr(CONFIG, 'broker_type', 'paper')).strip().lower()
    if broker_type in ('real', 'live', 'broker', 'adapter'):
        adapter = ConfigurableBrokerAdapter()
        _, probe = adapter.probe()
        if probe.get('ready_for_live_connect'):
            return adapter
        return RealBrokerStub(credentials={})
    return PaperBroker(CONFIG.starting_cash)
