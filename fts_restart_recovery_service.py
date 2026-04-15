# -*- coding: utf-8 -*-
from __future__ import annotations

"""Restart recovery service for pre-live/live promotion.

It creates a durable state snapshot and rebuilds a restart plan.  The plan is
fail-closed for live trading: if local snapshot or broker snapshot is missing,
new orders should remain blocked until reconciliation is clean.
"""

import json
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str
from fts_exception_policy import record_diagnostic


class RestartRecoveryService:
    def __init__(self):
        self.snapshot_path = PATHS.state_dir / 'restart_recovery_snapshot.json'
        self.plan_path = PATHS.runtime_dir / 'restart_recovery_plan.json'

    @staticmethod
    def _load_runtime_json(path: Path) -> dict[str, Any]:
        try:
            return json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
        except Exception:
            return {}

    def create_snapshot_from_broker(self, broker_obj: Any | None = None, *, meta: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
        payload: dict[str, Any] = {'saved_at': now_str(), 'system_name': CONFIG.system_name, 'meta': meta or {}}
        if broker_obj is None:
            payload.update({'cash': None, 'positions': [], 'open_orders': [], 'fills': [], 'broker_snapshot_available': False})
        else:
            payload.update(self._snapshot_broker(broker_obj))
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.snapshot_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        return str(self.snapshot_path), payload

    def _snapshot_broker(self, broker_obj: Any) -> dict[str, Any]:
        out = {'broker_snapshot_available': True, 'snapshot_errors': []}
        try:
            cash = broker_obj.get_cash() if callable(getattr(broker_obj, 'get_cash', None)) else {}
            out['cash'] = cash.get('cash_available', cash.get('cash', cash)) if isinstance(cash, dict) else cash
        except Exception as exc:
            record_diagnostic('restart_recovery', 'cash_snapshot_failed', exc, severity='error', fail_closed=True)
            out['snapshot_errors'].append('cash_snapshot_failed')
            out['cash'] = None
        try:
            if callable(getattr(broker_obj, 'get_positions_detailed', None)):
                out['positions'] = broker_obj.get_positions_detailed()
            elif callable(getattr(broker_obj, 'get_positions_rows', None)):
                out['positions'] = broker_obj.get_positions_rows()
            elif callable(getattr(broker_obj, 'get_positions', None)):
                pos = broker_obj.get_positions()
                out['positions'] = list(pos.values()) if isinstance(pos, dict) else list(pos or [])
            else:
                out['positions'] = []
        except Exception as exc:
            record_diagnostic('restart_recovery', 'positions_snapshot_failed', exc, severity='error', fail_closed=True)
            out['snapshot_errors'].append('positions_snapshot_failed')
            out['positions'] = []
        try:
            if callable(getattr(broker_obj, 'get_open_orders_dicts', None)):
                out['open_orders'] = broker_obj.get_open_orders_dicts()
            elif callable(getattr(broker_obj, 'snapshot_orders', None)):
                out['open_orders'] = broker_obj.snapshot_orders()
            else:
                out['open_orders'] = []
        except Exception as exc:
            record_diagnostic('restart_recovery', 'open_orders_snapshot_failed', exc, severity='error', fail_closed=True)
            out['snapshot_errors'].append('open_orders_snapshot_failed')
            out['open_orders'] = []
        try:
            if callable(getattr(broker_obj, 'get_fill_history_dicts', None)):
                out['fills'] = broker_obj.get_fill_history_dicts()
            elif callable(getattr(broker_obj, 'snapshot_fills', None)):
                out['fills'] = broker_obj.snapshot_fills()
            else:
                out['fills'] = []
        except Exception as exc:
            record_diagnostic('restart_recovery', 'fills_snapshot_failed', exc, severity='error', fail_closed=True)
            out['snapshot_errors'].append('fills_snapshot_failed')
            out['fills'] = []
        return out

    def build_plan(self, *, broker_snapshot: dict[str, Any] | None = None, require_broker_snapshot: bool = True) -> tuple[str, dict[str, Any]]:
        local = self._load_snapshot()
        blockers: list[str] = []
        actions: list[str] = []
        if not local:
            blockers.append('missing_local_restart_snapshot')
        else:
            actions.extend(['restore_local_cash', 'restore_positions', 'restore_open_orders', 'replay_recent_fills'])
        if require_broker_snapshot and not broker_snapshot:
            blockers.append('missing_broker_snapshot')
        if broker_snapshot:
            actions.extend(['fetch_broker_open_orders', 'fetch_broker_positions', 'fetch_broker_cash', 'run_reconciliation_before_new_orders'])
        if local and local.get('snapshot_errors'):
            blockers.extend([f"snapshot_error_{x}" for x in local.get('snapshot_errors', [])])
        ledger_snapshot = self._load_runtime_json(PATHS.runtime_dir / 'execution_ledger_snapshot.json')
        recon_payload = self._load_runtime_json(PATHS.runtime_dir / 'reconciliation_engine.json')
        payload = {
            'generated_at': now_str(),
            'status': 'restart_recovery_ready' if not blockers else 'restart_recovery_blocked',
            'ready_to_resume_new_orders': not blockers,
            'blockers': blockers,
            'actions': actions,
            'local_snapshot_path': str(self.snapshot_path),
            'local_snapshot_found': bool(local),
            'broker_snapshot_found': bool(broker_snapshot),
            'execution_ledger_snapshot_found': bool(ledger_snapshot),
            'execution_ledger_snapshot': ledger_snapshot,
            'reconciliation_found': bool(recon_payload),
            'reconciliation_status': recon_payload.get('status') if isinstance(recon_payload, dict) else None,
            'cash': (local or {}).get('cash'),
            'positions': (local or {}).get('positions', []),
            'fills': (local or {}).get('fills', []),
        }
        self.plan_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        return str(self.plan_path), payload

    def _load_snapshot(self) -> dict[str, Any] | None:
        try:
            return json.loads(self.snapshot_path.read_text(encoding='utf-8')) if self.snapshot_path.exists() else None
        except Exception as exc:
            record_diagnostic('restart_recovery', 'load_restart_snapshot_failed', exc, severity='error', fail_closed=True)
            return None


def main(argv: list[str] | None = None) -> int:
    svc = RestartRecoveryService()
    # CLI mode is intentionally conservative: build a plan from existing snapshots only.
    path, payload = svc.build_plan(require_broker_snapshot=False)
    print(json.dumps({'status': payload.get('status'), 'path': path, 'blockers': payload.get('blockers')}, ensure_ascii=False, indent=2))
    return 0 if payload.get('status') == 'restart_recovery_ready' else 1


if __name__ == '__main__':
    raise SystemExit(main())
