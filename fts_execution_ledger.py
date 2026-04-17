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

    def _fill_key(self, payload: dict[str, Any]) -> str:
        return str(payload.get('fill_id') or payload.get('execution_id') or payload.get('trade_id') or self._order_key(payload) or '').strip()

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ''):
                return default
            return float(value)
        except Exception:
            return default

    def _read_json(self, path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            record_diagnostic('execution_ledger', f'read_json_failed_{path.name}', exc, severity='warning', fail_closed=False)
            return {}

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

    def _fallback_positions_cash(self) -> tuple[list[dict[str, Any]], float | None, dict[str, str]]:
        positions: list[dict[str, Any]] = []
        cash: float | None = None
        sources: dict[str, str] = {}
        for path in [PATHS.runtime_dir / 'restart_recovery_snapshot.json', PATHS.state_dir / 'engine_state.json', PATHS.runtime_dir / 'execution_account_snapshot.json']:
            payload = self._read_json(path)
            if not positions and isinstance(payload.get('positions'), list):
                positions = [x for x in payload.get('positions', []) if isinstance(x, dict)]
                if positions:
                    sources['positions'] = str(path)
            if cash is None:
                for key in ('cash', 'cash_available', 'broker_cash', 'local_cash', 'available_cash'):
                    if key in payload and payload.get(key) not in (None, ''):
                        cash = self._safe_float(payload.get(key), 0.0)
                        sources['cash'] = str(path)
                        break
            if positions and cash is not None:
                break
        return positions, cash, sources

    def build_summary(self) -> tuple[str, dict[str, Any]]:
        lane_event_counts = defaultdict(int)
        lane_mutation_counts = defaultdict(int)
        order_counts = defaultdict(int)
        per_lane_orders = defaultdict(dict)
        orders_by_key: dict[str, dict[str, Any]] = {}
        fills_by_key: dict[str, dict[str, Any]] = {}
        latest_event = None

        if self.events_path.exists():
            for raw in self.events_path.read_text(encoding='utf-8').splitlines():
                if not raw.strip():
                    continue
                try:
                    row = json.loads(raw)
                except Exception as exc:
                    record_diagnostic('execution_ledger', 'parse_event_jsonl_line', exc, severity='warning', fail_closed=False)
                    continue
                if not isinstance(row, dict):
                    continue
                latest_event = row
                lane = normalize_key(row.get('lane')) or 'UNKNOWN'
                lane_event_counts[lane] += 1
                if row.get('event_type') == 'repair_mutation':
                    lane_mutation_counts[lane] += 1
                payload = row.get('payload') if isinstance(row.get('payload'), dict) else {}
                order_key = str(row.get('order_key') or self._order_key(payload) or '').strip()
                if order_key:
                    per_lane_orders[lane][order_key] = row
                    base = orders_by_key.get(order_key, {})
                    merged = {
                        **base,
                        **payload,
                        'order_id': order_key,
                        'client_order_id': str(payload.get('client_order_id') or base.get('client_order_id') or order_key),
                        'broker_order_id': str(payload.get('broker_order_id') or base.get('broker_order_id') or ''),
                        'lane': lane,
                        'status': str(payload.get('status') or base.get('status') or row.get('event_type') or ''),
                        'last_event_type': str(row.get('event_type') or ''),
                        'last_recorded_at': str(row.get('recorded_at') or ''),
                    }
                    orders_by_key[order_key] = merged
                fill_key = self._fill_key(payload)
                if str(row.get('event_type', '')).lower() == 'fill_event' and fill_key:
                    fbase = fills_by_key.get(fill_key, {})
                    fills_by_key[fill_key] = {
                        **fbase,
                        **payload,
                        'fill_id': fill_key,
                        'order_id': self._order_key(payload),
                        'client_order_id': str(payload.get('client_order_id') or fbase.get('client_order_id') or ''),
                        'broker_order_id': str(payload.get('broker_order_id') or fbase.get('broker_order_id') or ''),
                        'lane': lane,
                        'status': str(payload.get('status') or fbase.get('status') or 'FILLED'),
                        'recorded_at': str(row.get('recorded_at') or ''),
                    }

        for lane, orders in per_lane_orders.items():
            order_counts[lane] = len(orders)

        positions, cash, fallback_sources = self._fallback_positions_cash()
        orders = list(sorted(orders_by_key.values(), key=lambda x: str(x.get('last_recorded_at') or '')))
        fills = list(sorted(fills_by_key.values(), key=lambda x: str(x.get('recorded_at') or '')))

        sources = {
            'events_jsonl': str(self.events_path),
            'positions': fallback_sources.get('positions', ''),
            'cash': fallback_sources.get('cash', ''),
        }
        snapshot = {
            'generated_at': now_str(),
            'lane_orders': {k: list(v.keys()) for k, v in per_lane_orders.items()},
            'lane_order_counts': dict(order_counts),
            'lane_mutation_counts': dict(lane_mutation_counts),
            'broker_side_ledger_shadow_path': str(self.shadow_mutator.path),
            'orders': orders[-5000:],
            'fills': fills[-5000:],
            'positions': positions[:2000],
            'cash': cash,
            'latest_event': latest_event or {},
            'sources': sources,
        }
        write_json(self.snapshot_path, snapshot)
        payload = {
            'generated_at': now_str(),
            'module_version': 'v20260416_execution_ledger_local_evidence',
            'status': 'execution_ledger_ready',
            'path': str(self.events_path),
            'lane_event_counts': dict(lane_event_counts),
            'lane_order_counts': dict(order_counts),
            'lane_mutation_counts': dict(lane_mutation_counts),
            'broker_side_ledger_shadow_path': str(self.shadow_mutator.path),
            'order_count': int(len(orders)),
            'fill_count': int(len(fills)),
            'position_count': int(len(positions)),
            'cash': cash,
            'orders': orders[-5000:],
            'fills': fills[-5000:],
            'positions': positions[:2000],
            'local_side_complete': bool(orders or fills or positions or cash is not None),
            'sources': sources,
        }
        write_json(self.summary_path, payload)
        return str(self.summary_path), payload
