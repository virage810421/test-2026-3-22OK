# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 3 files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
"""Consolidated module generated from 8 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
from paper_broker import PaperBroker  # public formal facade


# ==============================================================================
# Merged from: fts_broker_core.py
# ==============================================================================
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
from fts_config import CONFIG
# merged-local import removed: from fts_broker_core import PaperBroker
from fts_broker_real_stub import RealBrokerStub  # public formal facade
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


# ==============================================================================
# Merged from: fts_broker_approval.py
# ==============================================================================
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class BrokerApprovalGate:
    """
    v33:
    在未來接真券商前，先做雙重確認邏輯。
    預設 PAPER 一律可通過；
    若不是 PAPER，則要求更嚴格的 approval 條件。
    """
    def __init__(self):
        self.path = PATHS.runtime_dir / "broker_approval_gate.json"

    def evaluate(self, launch_gate: dict, live_safety_gate: dict):
        mode = getattr(CONFIG, "mode", "PAPER")
        broker_type = getattr(CONFIG, "broker_type", "paper")

        failures = []
        warnings = []

        if not launch_gate.get("go_for_execution", False):
            failures.append({
                "type": "launch_gate_blocked",
                "message": "launch gate 未通過"
            })

        if not live_safety_gate.get("paper_live_safe", False):
            failures.append({
                "type": "live_safety_blocked",
                "message": "live safety gate 未通過"
            })

        requires_explicit_approval = not (str(mode).upper() == "PAPER" and str(broker_type).lower() == "paper")

        if requires_explicit_approval:
            warnings.append({
                "type": "explicit_live_approval_required",
                "message": f"mode={mode}, broker_type={broker_type}，未來接真券商時必須加入人工審批/簽核"
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "mode": mode,
            "broker_type": broker_type,
            "requires_explicit_approval": requires_explicit_approval,
            "go_for_broker_submission": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(
            f"🧾 Broker Approval Gate | go_for_broker_submission={payload['go_for_broker_submission']} | "
            f"requires_explicit_approval={payload['requires_explicit_approval']} | "
            f"failures={len(failures)} | warnings={len(warnings)}"
        )
        return self.path, payload


# ==============================================================================
# Merged from: fts_broker_shadow_mutator.py
# ==============================================================================
from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json, normalize_key


_DEFAULT_LANES = ("LONG", "SHORT", "RANGE", "UNKNOWN")


class BrokerShadowLedgerMutator:
    def __init__(self):
        self.path = PATHS.state_dir / 'broker_side_ledger_shadow.json'

    def _empty_state(self) -> dict[str, Any]:
        return {
            'generated_at': now_str(),
            'lanes': {lane: {} for lane in _DEFAULT_LANES},
            'history': [],
        }

    def _coerce_state(self, raw: Any) -> dict[str, Any]:
        state = raw if isinstance(raw, dict) else {}
        # Legacy / malformed payloads may not contain lanes or history.
        lanes = state.get('lanes')
        history = state.get('history')

        # Migrate old flat payloads into UNKNOWN lane if possible.
        migrated_unknown: dict[str, Any] = {}
        if not isinstance(lanes, dict):
            for k, v in list(state.items()):
                if k in {'generated_at', 'history', 'status', 'updated_at', 'path'}:
                    continue
                if isinstance(v, dict) and ('order_id' in v or 'lane' in v or 'last_mutation_type' in v):
                    oid = str(v.get('order_id') or k).strip() or f'UNKNOWN-{len(migrated_unknown)+1}'
                    migrated_unknown[oid] = dict(v)
            lanes = {}

        fixed_lanes: dict[str, dict[str, Any]] = {}
        for lane in _DEFAULT_LANES:
            payload = lanes.get(lane) if isinstance(lanes, dict) else None
            fixed_lanes[lane] = payload if isinstance(payload, dict) else {}

        # Preserve any unexpected lane buckets instead of dropping them.
        if isinstance(lanes, dict):
            for lane, payload in lanes.items():
                lane_key = normalize_key(lane) or str(lane)
                if lane_key not in fixed_lanes:
                    fixed_lanes[lane_key] = payload if isinstance(payload, dict) else {}

        if migrated_unknown:
            fixed_lanes.setdefault('UNKNOWN', {}).update(migrated_unknown)

        if not isinstance(history, list):
            history = []

        return {
            'generated_at': str(state.get('generated_at') or now_str()),
            'lanes': fixed_lanes,
            'history': history,
        }

    def _load(self) -> dict[str, Any]:
        raw = load_json(self.path, default=self._empty_state())
        return self._coerce_state(raw)

    def mutate(
        self,
        lane: str,
        order_id: str,
        mutation_type: str,
        patch: dict[str, Any] | None = None,
        reason: str = '',
    ) -> tuple[str, dict[str, Any]]:
        lane = normalize_key(lane) or 'UNKNOWN'
        order_id = str(order_id or '').strip() or f'{lane}-unknown'
        patch = dict(patch or {})
        state = self._load()
        state['lanes'].setdefault(lane, {})
        current = state['lanes'][lane].get(order_id, {'order_id': order_id, 'lane': lane})
        current.update(patch)
        current['last_mutation_type'] = mutation_type
        current['last_mutation_reason'] = reason
        current['updated_at'] = now_str()
        state['lanes'][lane][order_id] = current
        state['history'].append(
            {
                'at': now_str(),
                'lane': lane,
                'order_id': order_id,
                'mutation_type': mutation_type,
                'reason': reason,
                'patch': patch,
            }
        )
        write_json(self.path, state)
        payload = {
            'generated_at': now_str(),
            'status': 'shadow_ledger_mutated',
            'lane': lane,
            'order_id': order_id,
            'mutation_type': mutation_type,
            'path': str(self.path),
        }
        return str(self.path), payload
