# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import defaultdict
from typing import Any
import json

from fts_prelive_runtime import PATHS, now_str, append_jsonl, write_json, load_json, normalize_key


class ExecutionLedger:
    def __init__(self):
        self.events_path = PATHS.runtime_dir / 'execution_ledger_events.jsonl'
        self.summary_path = PATHS.runtime_dir / 'execution_ledger_summary.json'
        self.snapshot_path = PATHS.runtime_dir / 'execution_ledger_snapshot.json'
        self.broker_shadow_path = PATHS.runtime_dir / 'broker_side_ledger_shadow.json'

    def _lane(self, payload: dict[str, Any]) -> str:
        return normalize_key(payload.get('direction_bucket') or payload.get('approved_pool_type') or payload.get('lane') or 'UNKNOWN') or 'UNKNOWN'

    def _order_key(self, payload: dict[str, Any]) -> str:
        return str(payload.get('client_order_id') or payload.get('broker_order_id') or payload.get('order_id') or '').strip()

    def _load_shadow(self) -> dict[str, Any]:
        return load_json(self.broker_shadow_path, default={'lanes': {'LONG': {}, 'SHORT': {}, 'RANGE': {}, 'UNKNOWN': {}}, 'history': []}) or {'lanes': {'LONG': {}, 'SHORT': {}, 'RANGE': {}, 'UNKNOWN': {}}, 'history': []}

    def _save_shadow(self, payload: dict[str, Any]) -> None:
        write_json(self.broker_shadow_path, payload)

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

    def record_callback(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        return self.record('callback_event', payload)

    def record_state_transition(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        return self.record('state_transition', payload)

    def record_reconciliation_issue(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        return self.record('reconciliation_issue', payload)

    def mutate_from_repair(self, lane: str, order_key: str, action: str, note: str = '', patch: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
        lane = normalize_key(lane) or 'UNKNOWN'
        payload = {'direction_bucket': lane, 'order_id': order_key, 'repair_action': action, 'note': note, 'patch': patch or {}}
        self.record('repair_mutation', payload)
        shadow = self._load_shadow()
        shadow['lanes'].setdefault(lane, {})
        current = shadow['lanes'][lane].get(order_key, {'order_id': order_key, 'lane': lane})
        current.update(patch or {})
        current['last_repair_action'] = action
        current['last_repair_note'] = note
        current['updated_at'] = now_str()
        shadow['lanes'][lane][order_key] = current
        shadow['history'].append({'at': now_str(), 'lane': lane, 'order_id': order_key, 'action': action, 'patch': patch or {}})
        self._save_shadow(shadow)
        return self.build_summary()

    def build_summary(self) -> tuple[str, dict[str, Any]]:
        lane_event_counts = defaultdict(int)
        lane_order_keys = defaultdict(set)
        lane_event_type_counts = defaultdict(lambda: defaultdict(int))
        if self.events_path.exists():
            for raw in self.events_path.read_text(encoding='utf-8').splitlines():
                if not raw.strip():
                    continue
                try:
                    row = json.loads(raw)
                except Exception:
                    continue
                lane = normalize_key(row.get('lane')) or 'UNKNOWN'
                lane_event_counts[lane] += 1
                et = str(row.get('event_type') or '')
                lane_event_type_counts[lane][et] += 1
                order_key = str(row.get('order_key') or '').strip()
                if order_key:
                    lane_order_keys[lane].add(order_key)
        snapshot = {
            'generated_at': now_str(),
            'lane_orders': {k: sorted(v) for k, v in lane_order_keys.items()},
            'lane_order_counts': {k: len(v) for k, v in lane_order_keys.items()},
            'lane_event_type_counts': {k: dict(v) for k, v in lane_event_type_counts.items()},
        }
        write_json(self.snapshot_path, snapshot)
        payload = {'generated_at': now_str(), 'status': 'execution_ledger_ready', 'path': str(self.events_path), 'lane_event_counts': dict(lane_event_counts), 'lane_order_counts': {k: len(v) for k, v in lane_order_keys.items()}, 'lane_event_type_counts': {k: dict(v) for k, v in lane_event_type_counts.items()}, 'broker_shadow_path': str(self.broker_shadow_path)}
        write_json(self.summary_path, payload)
        return str(self.summary_path), payload
