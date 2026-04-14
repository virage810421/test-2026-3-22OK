# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 6 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_execution_models.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from dataclasses import asdict
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class OrderPayloadBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "order_payload_preview.json"

    def build_preview(self, accepted_signals):
        payloads = []
        for s in accepted_signals[:20]:
            payloads.append({
                "ticker": s.ticker,
                "action": s.action,
                "target_qty": s.target_qty,
                "reference_price": s.reference_price,
                "order_type": "LIMIT",
                "time_in_force": "DAY",
                "strategy_name": s.strategy_name,
                "regime": s.regime,
                "expected_return": s.expected_return,
                "kelly_fraction": s.kelly_fraction,
            })

        out = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "preview_count": len(payloads),
            "payloads": payloads,
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        log(f"🧾 已輸出 order payload preview：{self.path}")
        return self.path, out


# ==============================================================================
# Merged from: fts_execution_models.py
# ==============================================================================
# -*- coding: utf-8 -*-
from typing import Any

from fts_prelive_runtime import PATHS, now_str, write_json, normalize_key

_ALLOWED = {
    'NEW': {'PENDING_SUBMIT', 'REJECTED', 'CANCELLED'},
    'PENDING_SUBMIT': {'SUBMITTED', 'REJECTED', 'CANCELLED'},
    'SUBMITTED': {'PARTIALLY_FILLED', 'FILLED', 'CANCEL_PENDING', 'CANCELLED', 'REJECTED'},
    'PARTIALLY_FILLED': {'PARTIALLY_FILLED', 'FILLED', 'CANCEL_PENDING', 'CANCELLED'},
    'CANCEL_PENDING': {'CANCELLED', 'PARTIALLY_FILLED', 'FILLED'},
    'FILLED': set(),
    'CANCELLED': set(),
    'REJECTED': set(),
}


class OrderStateMachine:
    def transition(self, current: str, target: str) -> dict[str, Any]:
        current = normalize_key(current) or 'NEW'
        target = normalize_key(target)
        ok = target in _ALLOWED.get(current, set()) or current == target
        return {
            'from': current,
            'to': target,
            'allowed': ok,
            'reason': 'ok' if ok else 'illegal_transition',
        }

    def build_definition(self) -> tuple[str, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'status': 'order_state_machine_defined',
            'states': sorted(_ALLOWED.keys()),
            'allowed_transitions': {k: sorted(v) for k, v in _ALLOWED.items()},
        }
        path = PATHS.runtime_dir / 'order_state_machine_definition.json'
        write_json(path, payload)
        return str(path), payload


# ==============================================================================
# Merged from: fts_execution_models.py
# ==============================================================================
# -*- coding: utf-8 -*-
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


# ==============================================================================
# Merged from: fts_execution_models.py
# ==============================================================================
# -*- coding: utf-8 -*-
from typing import Any

from fts_prelive_runtime import PATHS, now_str, append_jsonl, write_json
# merged-local import removed: from fts_execution_models import CallbackEventSchema
from fts_execution_state_machine import DirectionalExecutionStateMachine
from fts_execution_ledger import ExecutionLedger
from fts_broker_shadow_mutator import BrokerShadowLedgerMutator


class CallbackEventStore:
    def __init__(self):
        self.schema = CallbackEventSchema()
        self.events_path = PATHS.runtime_dir / 'callback_events.jsonl'
        self.summary_path = PATHS.runtime_dir / 'callback_event_store_summary.json'
        self.state_machine = DirectionalExecutionStateMachine()
        self.ledger = ExecutionLedger()
        self.shadow_mutator = BrokerShadowLedgerMutator()

    def record(self, raw_event: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        event = self.schema.normalize(raw_event)
        envelope = {'recorded_at': now_str(), 'event': event}
        append_jsonl(self.events_path, envelope)
        _sm_path, sm_payload = self.state_machine.transition(event)
        event_type = str(event.get('event_type', '')).lower()
        if event_type == 'fill':
            self.ledger.record_fill(event)
            self.shadow_mutator.mutate(event.get('direction_bucket', 'UNKNOWN'), event.get('broker_order_id') or event.get('client_order_id') or '', 'callback_fill', patch={'status': event.get('status', ''), 'last_fill_symbol': event.get('symbol', '')}, reason='callback_fill')
        else:
            self.ledger.record('callback_event', event)
        summary = {
            'generated_at': now_str(),
            'status': 'callback_event_recorded' if event.get('valid') else 'callback_event_invalid_recorded',
            'last_event_valid': bool(event.get('valid')),
            'last_event_type': event.get('event_type', ''),
            'last_event_status': event.get('status', ''),
            'last_lane': event.get('direction_bucket', ''),
            'state_machine_status': sm_payload.get('status', ''),
            'path': str(self.events_path),
        }
        write_json(self.summary_path, summary)
        return str(self.summary_path), summary


# ==============================================================================
# Merged from: fts_execution_models.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ExecutionCallbackFlowBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "execution_callback_flow.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "flow": [
                "submit_order_payload",
                "receive_broker_callback",
                "normalize_callback_status",
                "validate_callback_fields",
                "update_order_state_machine",
                "reconciliation_engine_check",
                "persist_state_and_report",
            ],
            "integration_points": {
                "callback_norm": "broker_callback_normalization.json",
                "state_machine": "order_state_machine.json",
                "reconciliation_engine": "reconciliation_engine.json",
            },
            "status": "callback_flow_defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔁 已輸出 execution callback flow：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_execution_models.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class TradeMessageSummaryBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "trade_message_summary.json"

    def build(self, accepted_signals, rejected_pairs, execution_result: dict):
        accepted_rows = []
        for s in accepted_signals[:20]:
            accepted_rows.append({
                "ticker": getattr(s, "ticker", ""),
                "action": getattr(s, "action", ""),
                "target_qty": getattr(s, "target_qty", 0),
                "reference_price": getattr(s, "reference_price", 0),
                "strategy_name": getattr(s, "strategy_name", ""),
                "regime": getattr(s, "regime", ""),
            })

        rejected_rows = []
        for s, reason in rejected_pairs[:20]:
            rejected_rows.append({
                "ticker": getattr(s, "ticker", ""),
                "action": getattr(s, "action", ""),
                "reason": str(reason),
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "accepted_preview": accepted_rows,
            "rejected_preview": rejected_rows,
            "execution_summary": {
                "submitted": execution_result.get("submitted", 0),
                "filled": execution_result.get("filled", 0),
                "partially_filled": execution_result.get("partially_filled", 0),
                "rejected": execution_result.get("rejected", 0),
                "cancelled": execution_result.get("cancelled", 0),
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"💬 已輸出 trade message summary：{self.path}")
        return self.path, payload
