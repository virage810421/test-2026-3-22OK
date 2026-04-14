# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import defaultdict
from typing import Any
import json

from fts_prelive_runtime import PATHS, now_str, append_jsonl, write_json, normalize_key
from fts_broker_core import BrokerShadowLedgerMutator
from fts_exception_policy import record_diagnostic


class ExecutionLedger:
    def __init__(self):
        self.events_path = PATHS.runtime_dir / 'execution_ledger_events.jsonl'
        self.summary_path = PATHS.runtime_dir / 'execution_ledger_summary.json'
        self.snapshot_path = PATHS.runtime_dir / 'execution_ledger_snapshot.json'
        self.shadow_mutator = BrokerShadowLedgerMutator()

    def _lane(self, payload: dict[str, Any]) -> str:
        return normalize_key(payload.get('direction_bucket') or payload.get('approved_pool_type') or payload.get('lane') or 'UNKNOWN') or 'UNKNOWN'

    def _order_key(self, payload: dict[str, Any]) -> str:
        return str(payload.get('client_order_id') or payload.get('broker_order_id') or payload.get('order_id') or '').strip()

    def record(self, event_type: str, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        lane = self._lane(payload)
        order_key = self._order_key(payload)
        envelope = {'recorded_at': now_str(), 'event_type': str(event_type), 'lane': lane, 'order_key': order_key, 'payload': payload}
        append_jsonl(self.events_path, envelope)
        return self.build_summary()

    def record_submission(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        return self.record('submission_event', payload)

    def record_fill(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        return self.record('fill_event', payload)

    def mutate_from_repair(self, lane: str, order_key: str, action: str, note: str = '', patch: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
        payload = {'direction_bucket': lane, 'order_id': order_key, 'repair_action': action, 'note': note, 'patch': patch or {}}
        self.record('repair_mutation', payload)
        if patch is not None:
            self.shadow_mutator.mutate(lane, order_key, action, patch=patch, reason=note)
        return self.build_summary()

    def build_summary(self) -> tuple[str, dict[str, Any]]:
        lane_event_counts = defaultdict(int)
        lane_mutation_counts = defaultdict(int)
        order_counts = defaultdict(int)
        per_lane_orders = defaultdict(dict)
        if self.events_path.exists():
            for raw in self.events_path.read_text(encoding='utf-8').splitlines():
                if not raw.strip():
                    continue
                try:
                    row = json.loads(raw)
                except Exception as exc:
                    record_diagnostic('execution_ledger', 'parse_event_jsonl_line', exc, severity='warning', fail_closed=False)
                    continue
                lane = normalize_key(row.get('lane')) or 'UNKNOWN'
                lane_event_counts[lane] += 1
                if row.get('event_type') == 'repair_mutation':
                    lane_mutation_counts[lane] += 1
                order_key = str(row.get('order_key') or '').strip()
                if order_key:
                    per_lane_orders[lane][order_key] = row
        for lane, orders in per_lane_orders.items():
            order_counts[lane] = len(orders)
        snapshot = {
            'generated_at': now_str(),
            'lane_orders': {k: list(v.keys()) for k, v in per_lane_orders.items()},
            'lane_order_counts': dict(order_counts),
            'lane_mutation_counts': dict(lane_mutation_counts),
            'broker_side_ledger_shadow_path': str(self.shadow_mutator.path),
        }
        write_json(self.snapshot_path, snapshot)
        payload = {
            'generated_at': now_str(),
            'status': 'execution_ledger_ready',
            'path': str(self.events_path),
            'lane_event_counts': dict(lane_event_counts),
            'lane_order_counts': dict(order_counts),
            'lane_mutation_counts': dict(lane_mutation_counts),
            'broker_side_ledger_shadow_path': str(self.shadow_mutator.path),
        }
        write_json(self.summary_path, payload)
        return str(self.summary_path), payload
