# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import defaultdict
from typing import Any

from fts_prelive_runtime import PATHS, now_str, normalize_key, write_json
from fts_repair_workflow_engine import RepairWorkflowEngine


def _lane(row: dict[str, Any]) -> str:
    return normalize_key(row.get('direction_bucket') or row.get('approved_pool_type') or row.get('lane') or 'UNKNOWN') or 'UNKNOWN'


def _key(row: dict[str, Any]) -> str:
    return str(row.get('order_id') or row.get('client_order_id') or row.get('broker_order_id') or '').strip()


class ReconciliationEngine:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'reconciliation_engine.json'

    def reconcile(self, local_orders, broker_orders, local_fills, broker_fills, local_positions, broker_positions, local_cash, broker_cash):
        local_map = {_key(r): r for r in (local_orders or []) if _key(r)}
        broker_map = {_key(r): r for r in (broker_orders or []) if _key(r)}
        issues = []
        lane_issue_breakdown = defaultdict(int)
        for oid, row in local_map.items():
            if oid not in broker_map:
                lane = _lane(row)
                issues.append({'order_id': oid, 'lane': lane, 'type': 'missing_at_broker'})
                lane_issue_breakdown[lane] += 1
                continue
            if normalize_key(str(row.get('status', ''))) != normalize_key(str(broker_map[oid].get('status', ''))):
                lane = _lane(row)
                issues.append({'order_id': oid, 'lane': lane, 'type': 'status_mismatch'})
                lane_issue_breakdown[lane] += 1
        for oid, row in broker_map.items():
            if oid not in local_map:
                lane = _lane(row)
                issues.append({'order_id': oid, 'lane': lane, 'type': 'orphan_broker_order'})
                lane_issue_breakdown[lane] += 1
        payload = {
            'generated_at': now_str(),
            'status': 'reconciled',
            'order_issue_count': len(issues),
            'issues': issues,
            'lane_issue_breakdown': dict(lane_issue_breakdown),
            'cash_diff': float((broker_cash or 0) - (local_cash or 0)),
            'directional_order_mix': dict((lane, c) for lane, c in lane_issue_breakdown.items()),
            'directional_fill_mix': {},
        }
        repair_path, repair_payload = RepairWorkflowEngine().execute(payload)
        payload['repair_workflow'] = {'path': repair_path, 'payload': repair_payload}
        payload['directional_repair_actions'] = repair_payload.get('executed_actions', [])
        write_json(self.path, payload)
        return str(self.path), payload
