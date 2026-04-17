# -*- coding: utf-8 -*-
from __future__ import annotations

"""TWAP3 runtime closure audit.

Hardening v20260417b:
- A TWAP3 plan must create child orders and a broker-neutral submission queue.
- Without a real broker adapter, broker submission/fill can be marked pending,
  but non-broker closure must still prove plan -> queue -> ledger evidence.
- CREATED/NEW children are not silently accepted; they must be queued or filled.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
    PATHS = _Paths()


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    tmp.replace(path)


def _status(row: dict[str, Any]) -> str:
    return str(row.get('status') or row.get('order_status') or '').upper().replace('-', '_').strip()


def _remaining(row: dict[str, Any]) -> int:
    try:
        return int(float(row.get('remaining_qty', row.get('qty', 0)) or 0))
    except Exception:
        return 0


def _filled(row: dict[str, Any]) -> int:
    try:
        return int(float(row.get('filled_qty', row.get('quantity_filled', 0)) or 0))
    except Exception:
        return 0


class TWAP3RuntimeClosure:
    MODULE_VERSION = 'v20260417b_twap3_plan_queue_ledger_closure_gate'

    def __init__(self) -> None:
        self.path = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'twap3_runtime_closure.json'

    def _ensure_queue(self) -> dict[str, Any]:
        try:
            from fts_twap3_child_order_engine import TWAP3ChildOrderEngine
            # mark_pending=True makes the state explicit for restart/recovery and
            # prevents CREATED orders from looking like an executed closure.
            return TWAP3ChildOrderEngine().build_broker_submission_queue(mark_pending=True)
        except Exception as exc:
            return {'status': 'twap3_submission_queue_error', 'queue_count': 0, 'error': repr(exc), 'orders': []}

    def build(self) -> tuple[Path, dict[str, Any]]:
        runtime_dir = Path(getattr(PATHS, 'runtime_dir', Path('runtime')))
        queue_payload = self._ensure_queue()
        state = _read_json(runtime_dir / 'twap3_child_order_state.json')
        ledger = _read_json(runtime_dir / 'execution_ledger_summary.json')
        parents = state.get('parents', {}) if isinstance(state.get('parents'), dict) else {}
        children = state.get('children', {}) if isinstance(state.get('children'), dict) else {}
        child_rows = [x for x in children.values() if isinstance(x, dict)]
        queue_orders = queue_payload.get('orders', []) if isinstance(queue_payload.get('orders'), list) else []
        queued_ids = {str(x.get('client_order_id') or '') for x in queue_orders if isinstance(x, dict)}
        ledger_orders = ledger.get('orders', []) if isinstance(ledger.get('orders'), list) else []
        ledger_fills = ledger.get('fills', []) if isinstance(ledger.get('fills'), list) else []
        ledger_order_ids = {str(x.get('client_order_id') or x.get('order_id') or '') for x in ledger_orders if isinstance(x, dict)}
        ledger_fill_ids = {str(x.get('client_order_id') or x.get('order_id') or x.get('fill_id') or '') for x in ledger_fills if isinstance(x, dict)}

        issues: list[dict[str, Any]] = []
        broker_pending: list[dict[str, Any]] = []
        for parent_id, parent in parents.items():
            plist = [x for x in parent.get('child_orders', []) if isinstance(x, dict)] if isinstance(parent, dict) else []
            if not plist:
                issues.append({'type': 'parent_without_child_orders', 'parent_order_id': str(parent_id)})

        for child in child_rows:
            st = _status(child)
            cid = str(child.get('child_order_id') or '')
            if not cid:
                issues.append({'type': 'child_missing_id', 'parent_order_id': child.get('parent_order_id')})
                continue
            rem = _remaining(child)
            filled = _filled(child)
            has_ledger = cid in ledger_order_ids or cid in ledger_fill_ids
            has_queue = cid in queued_ids
            if st in {'CREATED', 'NEW', 'PENDING_SUBMIT'} and rem > 0:
                if not has_queue and not has_ledger and not child.get('broker_order_id') and filled <= 0:
                    issues.append({'type': 'child_not_queued_submitted_or_filled', 'child_order_id': cid, 'parent_order_id': child.get('parent_order_id'), 'status': st})
                else:
                    broker_pending.append({'child_order_id': cid, 'parent_order_id': child.get('parent_order_id'), 'status': st, 'queued': bool(has_queue), 'ledger_seen': bool(has_ledger)})
            if st == 'REPLACED' and not child.get('replace_parent_child_id'):
                issues.append({'type': 'replace_chain_missing', 'child_order_id': cid})

        open_children = [c for c in child_rows if _status(c) not in {'FILLED','CANCELLED','CANCELED','REJECTED','EXPIRED'} and _remaining(c) > 0]
        queue_ready = bool(queue_orders) or not open_children
        nonbroker_ready = bool(not issues and queue_ready)
        broker_fully_closed = bool(nonbroker_ready and not broker_pending and not open_children)
        if not child_rows:
            status = 'twap3_no_runtime_activity'
        elif issues:
            status = 'twap3_runtime_closure_issues'
        elif broker_fully_closed:
            status = 'twap3_runtime_closed_loop_ready'
        else:
            status = 'twap3_nonbroker_closed_broker_pending'

        payload = {
            'generated_at': _now(),
            'module_version': self.MODULE_VERSION,
            'status': status,
            'nonbroker_closed_loop_ready': bool(nonbroker_ready),
            'broker_fully_closed': bool(broker_fully_closed),
            'parent_count': len(parents),
            'child_count': len(child_rows),
            'open_child_count': len(open_children),
            'queue_count': int(queue_payload.get('queue_count', 0) or 0),
            'ledger_order_count': len(ledger_orders),
            'ledger_fill_count': len(ledger_fills),
            'broker_pending_child_count': len(broker_pending),
            'issue_count': len(issues),
            'issues': issues[:300],
            'broker_pending_children': broker_pending[:300],
            'open_children': open_children[:300],
            'state_path': str(runtime_dir / 'twap3_child_order_state.json'),
            'queue_path': str(runtime_dir / 'twap3_broker_submission_queue.json'),
            'ledger_summary_path': str(runtime_dir / 'execution_ledger_summary.json') if ledger else '',
            'queue_payload_status': queue_payload.get('status'),
            'truthful_rule': 'TWAP3 非券商閉環至少要有 child plan + submission queue / ledger；真券商 fully closed 另需 broker callback/fill。',
        }
        _write_json(self.path, payload)
        return self.path, payload


def main(argv: list[str] | None = None) -> int:
    path, payload = TWAP3RuntimeClosure().build()
    print(json.dumps({'status': payload.get('status'), 'path': str(path), 'issues': payload.get('issue_count'), 'queue_count': payload.get('queue_count')}, ensure_ascii=False, indent=2))
    return 0 if payload.get('nonbroker_closed_loop_ready') else 1


if __name__ == '__main__':
    raise SystemExit(main())
