# -*- coding: utf-8 -*-
from __future__ import annotations

"""Formal broker contract / dry-run audit.

This module does not pretend that a real broker is connected.  It verifies the
methods a live adapter must provide and, when no account is configured, uses
RealBrokerStub only as a paper_prelive_only simulator.
"""

import inspect
import json
from pathlib import Path
from typing import Any

from fts_config import PATHS
from fts_utils import now_str
from fts_exception_policy import record_diagnostic

REQUIRED_BROKER_METHODS: tuple[str, ...] = (
    'connect',
    'refresh_auth',
    'place_order',
    'cancel_order',
    'replace_order',
    'get_order_status',
    'get_fills',
    'get_positions',
    'get_cash',
    'poll_callbacks',
    'disconnect',
)


def _write(path: Path, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    return str(path), payload


def _method_status(adapter: Any, name: str) -> dict[str, Any]:
    fn = getattr(adapter, name, None)
    if not callable(fn):
        return {'method': name, 'present': False, 'callable': False, 'signature': '', 'status': 'missing'}
    try:
        sig = str(inspect.signature(fn))
    except Exception as exc:
        record_diagnostic('broker_contract', f'signature_failed_{name}', exc, severity='warning', fail_closed=False)
        sig = 'unknown'
    return {'method': name, 'present': True, 'callable': True, 'signature': sig, 'status': 'ready'}


def audit_adapter_contract(adapter: Any) -> dict[str, Any]:
    methods = [_method_status(adapter, name) for name in REQUIRED_BROKER_METHODS]
    missing = [m['method'] for m in methods if not m['present'] or not m['callable']]
    cap = {}
    if callable(getattr(adapter, 'capability_report', None)):
        try:
            cap = dict(adapter.capability_report() or {})
        except Exception as exc:
            record_diagnostic('broker_contract', 'capability_report_failed', exc, severity='warning', fail_closed=False)
            cap = {'status': 'capability_report_failed', 'error': repr(exc)}
    true_ready = bool(cap.get('true_broker_ready')) and bool(cap.get('broker_bound')) and not missing
    return {
        'generated_at': now_str(),
        'adapter_class': adapter.__class__.__name__,
        'required_methods': list(REQUIRED_BROKER_METHODS),
        'method_audit': methods,
        'missing_methods': missing,
        'capability_report': cap,
        'paper_prelive_ready': bool(cap.get('paper_prelive_ready', not missing)),
        'true_broker_ready': true_ready,
        'real_money_execution': bool(cap.get('real_money_execution', False)),
        'status': 'true_broker_contract_ready' if true_ready else ('paper_prelive_contract_ready' if not missing else 'broker_contract_not_ready'),
    }


def dry_run_paper_contract() -> dict[str, Any]:
    """Run a tiny paper broker smoke test without touching real money."""
    try:
        from fts_broker_real_stub import RealBrokerStub
        broker = RealBrokerStub()
        contract = audit_adapter_contract(broker)
        connect = broker.connect()
        order_payload = {
            'ticker': '2330.TW', 'side': 'BUY', 'qty': 1, 'price': 100.0,
            'client_order_id': 'PRELIVE-CONTRACT-SMOKE', 'strategy_name': 'contract_audit',
        }
        place = broker.place_order(order_payload)
        broker_order_id = str(place.get('broker_order_id') or place.get('order_id') or '')
        status = broker.get_order_status(broker_order_id) if broker_order_id else {'status': 'no_broker_order_id'}
        callbacks = broker.poll_callbacks(clear=True)
        cash = broker.get_cash()
        return {
            'generated_at': now_str(),
            'contract': contract,
            'connect': connect,
            'place_order': place,
            'order_status': status,
            'callbacks_count': len(callbacks),
            'callbacks': callbacks[-5:],
            'cash': cash,
            'status': 'paper_prelive_contract_smoke_ok' if place.get('ok', True) is not False else 'paper_prelive_contract_smoke_failed',
        }
    except Exception as exc:
        record_diagnostic('broker_contract', 'paper_contract_smoke_failed', exc, severity='error', fail_closed=True)
        return {'generated_at': now_str(), 'status': 'paper_contract_smoke_failed', 'error': repr(exc)}


def build_report() -> tuple[str, dict[str, Any]]:
    payload = dry_run_paper_contract()
    return _write(PATHS.runtime_dir / 'broker_contract_readiness.json', payload)


def main(argv: list[str] | None = None) -> int:
    path, payload = build_report()
    print(json.dumps({'status': payload.get('status'), 'path': path}, ensure_ascii=False, indent=2))
    return 0 if str(payload.get('status', '')).endswith('_ok') else 1


if __name__ == '__main__':
    raise SystemExit(main())
