# -*- coding: utf-8 -*-
from __future__ import annotations

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
