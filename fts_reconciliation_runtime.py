# -*- coding: utf-8 -*-
from __future__ import annotations

"""Pre-live reconciliation runtime.

Hardening v20260417b:
- Missing broker snapshot is never treated as a clean reconciliation.
- Paper/pre-live local evidence is reported separately from broker-live readiness.
- The output makes clear whether the gap is local ledger, broker snapshot, or
  actual mismatch.
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


def _qty(row: dict[str, Any]) -> int:
    try:
        return int(float(row.get('qty') or row.get('quantity') or row.get('shares') or row.get('進場股數') or 0))
    except Exception:
        return 0


class PreliveReconciliationRuntime:
    MODULE_VERSION = 'v20260417b_reconciliation_missing_snapshot_fail_closed'

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
        local_source_present: bool | None = None,
        broker_source_present: bool | None = None,
        broker_snapshot_path: str = '',
        local_ledger_path: str = '',
    ) -> tuple[str, dict[str, Any]]:
        lo = list(local_orders or [])
        bo = list(broker_orders or [])
        lf = list(local_fills or [])
        bf = list(broker_fills or [])
        lp = list(local_positions or [])
        bp = list(broker_positions or [])
        local_side_complete = bool(lo or lf or lp or local_cash is not None)
        if local_source_present is not None:
            local_side_complete = bool(local_source_present and local_side_complete)
        broker_side_available = bool(bo or bf or bp or broker_cash is not None)
        if broker_source_present is not None:
            broker_side_available = bool(broker_source_present and broker_side_available)

        issues: list[dict[str, Any]] = []
        issue_counts = defaultdict(int)
        if local_side_complete and broker_side_available:
            self._compare_orders(lo, bo, issues, issue_counts)
            self._compare_fills(lf, bf, issues, issue_counts)
            self._compare_positions(lp, bp, issues, issue_counts)
        elif local_side_complete and not broker_side_available:
            issues.append({'type': 'broker_snapshot_missing', 'detail': 'local runtime exists but broker-side snapshot/fills/cash are missing'})
            issue_counts['broker_snapshot_missing'] += 1
        elif broker_side_available and not local_side_complete:
            issues.append({'type': 'local_ledger_missing', 'detail': 'broker runtime exists but local execution ledger is missing'})
            issue_counts['local_ledger_missing'] += 1

        cash_diff = None
        if local_cash is not None and broker_cash is not None:
            cash_diff = round(float(broker_cash or 0) - float(local_cash or 0), 4)
            if abs(cash_diff) > 1.0:
                issues.append({'type': 'cash_mismatch', 'local_cash': local_cash, 'broker_cash': broker_cash, 'diff': cash_diff})
                issue_counts['cash_mismatch'] += 1
        elif local_side_complete and broker_side_available:
            # Both sides exist but one cash value is absent; report but do not hide it.
            issues.append({'type': 'cash_side_missing', 'local_cash_present': local_cash is not None, 'broker_cash_present': broker_cash is not None})
            issue_counts['cash_side_missing'] += 1

        if not local_side_complete and not broker_side_available:
            status = 'reconciliation_waiting_for_runtime_evidence'
        elif local_side_complete and not broker_side_available:
            status = 'reconciliation_waiting_for_broker_snapshot'
        elif broker_side_available and not local_side_complete:
            status = 'reconciliation_waiting_for_local_ledger'
        elif issues:
            status = 'reconciliation_issues_found'
        else:
            status = 'reconciliation_clean'

        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': status,
            'all_green': bool(status == 'reconciliation_clean'),
            'ready_for_live_promotion': bool(status == 'reconciliation_clean' and local_side_complete and broker_side_available),
            'paper_prelive_local_ready': bool(local_side_complete),
            'local_side_complete': bool(local_side_complete),
            'broker_snapshot_present': bool(broker_side_available),
            'broker_snapshot_required_for_live': True,
            'issue_count': len(issues),
            'issue_counts': dict(issue_counts),
            'cash_diff': cash_diff,
            'input_counts': {
                'local_orders': len(lo), 'broker_orders': len(bo),
                'local_fills': len(lf), 'broker_fills': len(bf),
                'local_positions': len(lp), 'broker_positions': len(bp),
            },
            'sources': {
                'local_ledger': local_ledger_path,
                'broker_snapshot': broker_snapshot_path,
            },
            'issues': issues[:300],
            'truthful_rule': '缺 broker snapshot 時不能回報 reconciliation_clean；只能回報等待 broker snapshot。',
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
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
                    out[t] = _qty(r)
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

    ledger_path = PATHS.runtime_dir / 'execution_ledger_summary.json'
    broker_snapshot_path = PATHS.runtime_dir / 'broker_runtime_snapshot.json'
    ledger = load_json(ledger_path, {})
    broker_snapshot = load_json(broker_snapshot_path, {})
    local_present = ledger_path.exists() and isinstance(ledger, dict)
    broker_present = broker_snapshot_path.exists() and isinstance(broker_snapshot, dict)
    return PreliveReconciliationRuntime().reconcile(
        local_orders=list(ledger.get('orders', []) or []) if isinstance(ledger, dict) else [],
        broker_orders=list(broker_snapshot.get('open_orders', broker_snapshot.get('orders', [])) or []) if isinstance(broker_snapshot, dict) else [],
        local_fills=list(ledger.get('fills', []) or []) if isinstance(ledger, dict) else [],
        broker_fills=list(broker_snapshot.get('fills', []) or []) if isinstance(broker_snapshot, dict) else [],
        local_positions=list(ledger.get('positions', []) or []) if isinstance(ledger, dict) else [],
        broker_positions=list(broker_snapshot.get('positions', []) or []) if isinstance(broker_snapshot, dict) else [],
        local_cash=ledger.get('cash') if isinstance(ledger, dict) else None,
        broker_cash=broker_snapshot.get('cash') if isinstance(broker_snapshot, dict) else None,
        local_source_present=local_present,
        broker_source_present=broker_present,
        local_ledger_path=str(ledger_path) if local_present else '',
        broker_snapshot_path=str(broker_snapshot_path) if broker_present else '',
    )


def main(argv: list[str] | None = None) -> int:
    path, payload = build_from_runtime_files()
    print(json.dumps({'status': payload.get('status'), 'path': path, 'issue_count': payload.get('issue_count'), 'broker_snapshot_present': payload.get('broker_snapshot_present')}, ensure_ascii=False, indent=2))
    return 0 if payload.get('status') == 'reconciliation_clean' else 1


if __name__ == '__main__':
    raise SystemExit(main())
