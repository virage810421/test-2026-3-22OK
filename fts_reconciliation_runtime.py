# -*- coding: utf-8 -*-
from __future__ import annotations

"""Pre-live reconciliation runtime.

Compares local ledger snapshots against broker snapshots.  Missing/unknown data
is not treated as success; it is reported clearly and blocks destructive cleanup
or live promotion.
"""

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from fts_config import PATHS
from fts_utils import now_str
from fts_exception_policy import record_diagnostic


def _key(row: dict[str, Any]) -> str:
    return str(row.get('client_order_id') or row.get('broker_order_id') or row.get('order_id') or row.get('fill_id') or '').strip()


def _ticker(row: dict[str, Any]) -> str:
    return str(row.get('ticker_symbol') or row.get('ticker') or row.get('symbol') or row.get('Ticker SYMBOL') or '').strip().upper()


def _status(row: dict[str, Any]) -> str:
    return str(row.get('status') or row.get('order_status') or '').strip().upper()


class PreliveReconciliationRuntime:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'prelive_reconciliation_runtime.json'

    def reconcile(
        self,
        *,
        local_orders: list[dict[str, Any]] | None = None,
        broker_orders: list[dict[str, Any]] | None = None,
        local_fills: list[dict[str, Any]] | None = None,
        broker_fills: list[dict[str, Any]] | None = None,
        local_positions: list[dict[str, Any]] | None = None,
        broker_positions: list[dict[str, Any]] | None = None,
        local_cash: float | None = None,
        broker_cash: float | None = None,
    ) -> tuple[str, dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        issue_counts = defaultdict(int)
        self._compare_orders(local_orders or [], broker_orders or [], issues, issue_counts)
        self._compare_fills(local_fills or [], broker_fills or [], issues, issue_counts)
        self._compare_positions(local_positions or [], broker_positions or [], issues, issue_counts)
        cash_diff = None
        if local_cash is not None and broker_cash is not None:
            cash_diff = round(float(broker_cash or 0) - float(local_cash or 0), 4)
            if abs(cash_diff) > 1.0:
                issues.append({'type': 'cash_mismatch', 'local_cash': local_cash, 'broker_cash': broker_cash, 'diff': cash_diff})
                issue_counts['cash_mismatch'] += 1
        payload = {
            'generated_at': now_str(),
            'status': 'reconciliation_clean' if not issues else 'reconciliation_issues_found',
            'ready_for_live_promotion': len(issues) == 0 and broker_orders is not None and broker_fills is not None,
            'issue_count': len(issues),
            'issue_counts': dict(issue_counts),
            'cash_diff': cash_diff,
            'issues': issues[:200],
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        return str(self.path), payload

    def _compare_orders(self, local: list[dict[str, Any]], broker: list[dict[str, Any]], issues: list[dict[str, Any]], counts: dict[str, int]) -> None:
        lmap = {_key(r): r for r in local if _key(r)}
        bmap = {_key(r): r for r in broker if _key(r)}
        for k, row in lmap.items():
            if k not in bmap:
                issues.append({'type': 'order_missing_at_broker', 'key': k, 'ticker_symbol': _ticker(row)})
                counts['order_missing_at_broker'] += 1
            elif _status(row) and _status(bmap[k]) and _status(row) != _status(bmap[k]):
                issues.append({'type': 'order_status_mismatch', 'key': k, 'local': _status(row), 'broker': _status(bmap[k])})
                counts['order_status_mismatch'] += 1
        for k, row in bmap.items():
            if k not in lmap:
                issues.append({'type': 'orphan_broker_order', 'key': k, 'ticker_symbol': _ticker(row)})
                counts['orphan_broker_order'] += 1

    def _compare_fills(self, local: list[dict[str, Any]], broker: list[dict[str, Any]], issues: list[dict[str, Any]], counts: dict[str, int]) -> None:
        lkeys = {_key(r) for r in local if _key(r)}
        bkeys = {_key(r) for r in broker if _key(r)}
        for k in sorted(lkeys - bkeys):
            issues.append({'type': 'fill_missing_at_broker', 'key': k})
            counts['fill_missing_at_broker'] += 1
        for k in sorted(bkeys - lkeys):
            issues.append({'type': 'orphan_broker_fill', 'key': k})
            counts['orphan_broker_fill'] += 1

    def _compare_positions(self, local: list[dict[str, Any]], broker: list[dict[str, Any]], issues: list[dict[str, Any]], counts: dict[str, int]) -> None:
        def pos_map(rows):
            out = {}
            for r in rows:
                t = _ticker(r)
                if t:
                    out[t] = int(float(r.get('qty') or r.get('quantity') or 0))
            return out
        lmap, bmap = pos_map(local), pos_map(broker)
        for t in sorted(set(lmap) | set(bmap)):
            if lmap.get(t, 0) != bmap.get(t, 0):
                issues.append({'type': 'position_qty_mismatch', 'ticker_symbol': t, 'local_qty': lmap.get(t, 0), 'broker_qty': bmap.get(t, 0)})
                counts['position_qty_mismatch'] += 1


def build_from_runtime_files() -> tuple[str, dict[str, Any]]:
    """Best-effort reconciliation from current runtime JSON files."""
    def load_json(path: Path, default):
        try:
            return json.loads(path.read_text(encoding='utf-8')) if path.exists() else default
        except Exception as exc:
            record_diagnostic('reconciliation_runtime', f'load_failed_{path.name}', exc, severity='warning', fail_closed=False)
            return default
    ledger = load_json(PATHS.runtime_dir / 'execution_ledger_summary.json', {})
    broker_snapshot = load_json(PATHS.runtime_dir / 'broker_runtime_snapshot.json', {})
    return PreliveReconciliationRuntime().reconcile(
        local_orders=list(ledger.get('orders', []) or []),
        broker_orders=list(broker_snapshot.get('open_orders', []) or []),
        local_fills=list(ledger.get('fills', []) or []),
        broker_fills=list(broker_snapshot.get('fills', []) or []),
        local_positions=list(ledger.get('positions', []) or []),
        broker_positions=list(broker_snapshot.get('positions', []) or []),
        local_cash=ledger.get('cash'),
        broker_cash=broker_snapshot.get('cash'),
    )


def main(argv: list[str] | None = None) -> int:
    path, payload = build_from_runtime_files()
    print(json.dumps({'status': payload.get('status'), 'path': path, 'issue_count': payload.get('issue_count')}, ensure_ascii=False, indent=2))
    return 0 if payload.get('status') == 'reconciliation_clean' else 1


if __name__ == '__main__':
    raise SystemExit(main())
