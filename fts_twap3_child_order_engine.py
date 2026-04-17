# -*- coding: utf-8 -*-
from __future__ import annotations

"""TWAP3 child-order engine.

Purpose:
- Turn an approved parent order into three executable child orders.
- Persist parent/child state so restart recovery can resume instead of duplicating.
- Provide callback ingestion helpers for SUBMITTED / PARTIALLY_FILLED / FILLED /
  CANCELLED / REJECTED / REPLACED events.

This module is broker-adapter neutral.  The real broker adapter should submit the
children from ``child_orders`` and feed callbacks back through
``apply_child_callback``.  Paper/live-pending modes can still use the generated
runtime evidence for reconciliation and promotion audits.
"""

import json
import math
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
    PATHS = _Paths()

try:
    from config import PARAMS  # type: ignore
except Exception:  # pragma: no cover
    PARAMS = {}

STATE_PATH = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'twap3_child_order_state.json'
PLAN_PATH = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'twap3_child_order_plan.json'

TERMINAL = {'FILLED', 'CANCELLED', 'CANCELED', 'REJECTED', 'EXPIRED'}
STATUS_ALIASES = {
    'ACK': 'SUBMITTED', 'ACCEPTED': 'SUBMITTED', 'WORKING': 'SUBMITTED', 'PARTIAL': 'PARTIALLY_FILLED',
    'PARTIALLYFILLED': 'PARTIALLY_FILLED', 'PARTIALLY_FILLED': 'PARTIALLY_FILLED', 'FILLED': 'FILLED',
    'DONE': 'FILLED', 'CANCELED': 'CANCELLED', 'CANCELLED': 'CANCELLED', 'REJECT': 'REJECTED',
    'REJECTED': 'REJECTED', 'NEW': 'NEW', 'CREATED': 'NEW', 'PENDING_SUBMIT': 'PENDING_SUBMIT',
    'PENDING_CANCEL': 'PENDING_CANCEL', 'REPLACED': 'REPLACED', 'REPRICE': 'REPLACED',
}


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == '':
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == '':
            return default
        return int(float(value))
    except Exception:
        return default


def _norm_status(value: Any) -> str:
    s = str(value or '').strip().upper().replace('-', '_').replace(' ', '_')
    return STATUS_ALIASES.get(s, s or 'UNKNOWN')


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(path)


@dataclass
class TWAP3ChildOrder:
    parent_order_id: str
    child_order_id: str
    ticker: str
    side: str
    child_seq: int
    child_count: int
    qty: int
    requested_qty: int
    filled_qty: int = 0
    remaining_qty: int = 0
    limit_price: float = 0.0
    reference_price: float = 0.0
    scheduled_at: str = ''
    status: str = 'CREATED'
    retry_count: int = 0
    reprice_count: int = 0
    replace_parent_child_id: str = ''
    broker_order_id: str = ''
    last_event_id: str = ''
    last_update: str = ''
    notes: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.remaining_qty:
            self.remaining_qty = max(0, int(self.qty) - int(self.filled_qty))
        if not self.last_update:
            self.last_update = _now()

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TWAP3ParentPlan:
    parent_order_id: str
    ticker: str
    side: str
    total_qty: int
    reference_price: float
    execution_style: str = 'TWAP3'
    status: str = 'CREATED'
    child_count: int = 3
    filled_qty: int = 0
    remaining_qty: int = 0
    participation_rate: float = 0.0
    liquidity_score: float = 0.0
    adv20: float = 0.0
    turnover_ratio: float = 0.0
    created_at: str = ''
    updated_at: str = ''
    child_orders: list[dict[str, Any]] = field(default_factory=list)
    resume_token: str = ''
    reprice_policy: dict[str, Any] = field(default_factory=dict)
    cancel_resume_policy: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.created_at:
            self.created_at = _now()
        if not self.updated_at:
            self.updated_at = self.created_at
        if not self.remaining_qty:
            self.remaining_qty = max(0, int(self.total_qty) - int(self.filled_qty))
        if not self.resume_token:
            self.resume_token = f'{self.parent_order_id}:{self.ticker}:{self.total_qty}'

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class TWAP3ChildOrderEngine:
    MODULE_VERSION = 'v20260417_twap3_child_order_executor'

    def __init__(self, state_path: Path | None = None, plan_path: Path | None = None) -> None:
        self.state_path = Path(state_path or STATE_PATH)
        self.plan_path = Path(plan_path or PLAN_PATH)

    def _state(self) -> dict[str, Any]:
        state = _load_json(self.state_path)
        if not state:
            state = {'module_version': self.MODULE_VERSION, 'generated_at': _now(), 'parents': {}, 'children': {}, 'events': []}
        state.setdefault('parents', {})
        state.setdefault('children', {})
        state.setdefault('events', [])
        return state

    def _persist(self, state: dict[str, Any], latest_plan: dict[str, Any] | None = None) -> None:
        state['module_version'] = self.MODULE_VERSION
        state['updated_at'] = _now()
        _write_json(self.state_path, state)
        if latest_plan is not None:
            _write_json(self.plan_path, latest_plan)

    @staticmethod
    def _split_qty(total_qty: int, child_count: int = 3) -> list[int]:
        total_qty = max(0, int(total_qty))
        child_count = max(1, int(child_count))
        if total_qty <= 0:
            return []
        base = total_qty // child_count
        rem = total_qty % child_count
        chunks = [base + (1 if i < rem else 0) for i in range(child_count)]
        return [q for q in chunks if q > 0]

    @staticmethod
    def _limit_price(reference_price: float, side: str, seq: int) -> float:
        px = max(float(reference_price), 0.0)
        tick = float(PARAMS.get('TWAP3_LIMIT_TICK_BUFFER', 0.001))
        side_u = str(side).upper()
        # Conservative: later slices can be slightly more patient/less marketable.
        offset = tick * max(seq - 1, 0)
        if side_u in {'BUY', 'LONG'}:
            return round(px * (1 + offset), 4)
        return round(px * (1 - offset), 4)

    def build_plan(
        self,
        *,
        ticker: str,
        side: str,
        total_qty: int,
        reference_price: float,
        liquidity_score: float = 0.0,
        adv20: float = 0.0,
        turnover_ratio: float = 0.0,
        parent_order_id: str | None = None,
        start_time: datetime | None = None,
        child_count: int = 3,
        interval_seconds: int | None = None,
    ) -> dict[str, Any]:
        total_qty = max(0, int(total_qty))
        if total_qty <= 0:
            return {
                'module_version': self.MODULE_VERSION,
                'status': 'blocked_zero_qty',
                'parent_order_id': parent_order_id or '',
                'ticker': ticker,
                'side': side,
                'total_qty': 0,
                'child_orders': [],
            }
        interval_seconds = int(interval_seconds or PARAMS.get('TWAP3_CHILD_INTERVAL_SECONDS', 900))
        parent_order_id = parent_order_id or f'TWAP3-{datetime.now().strftime("%Y%m%d%H%M%S")}-{uuid.uuid4().hex[:8]}'
        start_time = start_time or datetime.now()
        chunks = self._split_qty(total_qty, child_count=child_count)
        children: list[dict[str, Any]] = []
        for i, qty in enumerate(chunks, start=1):
            child = TWAP3ChildOrder(
                parent_order_id=parent_order_id,
                child_order_id=f'{parent_order_id}-C{i}',
                ticker=str(ticker),
                side=str(side).upper(),
                child_seq=i,
                child_count=len(chunks),
                qty=int(qty),
                requested_qty=int(qty),
                remaining_qty=int(qty),
                reference_price=float(reference_price),
                limit_price=self._limit_price(float(reference_price), side, i),
                scheduled_at=(start_time + timedelta(seconds=interval_seconds * (i - 1))).strftime('%Y-%m-%d %H:%M:%S'),
                status='CREATED',
            ).as_dict()
            children.append(child)
        participation_rate = 0.0
        if adv20 > 0 and reference_price > 0:
            participation_rate = float((total_qty * reference_price) / adv20)
        plan = TWAP3ParentPlan(
            parent_order_id=parent_order_id,
            ticker=str(ticker),
            side=str(side).upper(),
            total_qty=int(total_qty),
            reference_price=float(reference_price),
            child_count=len(children),
            child_orders=children,
            participation_rate=round(participation_rate, 8),
            liquidity_score=float(liquidity_score),
            adv20=float(adv20),
            turnover_ratio=float(turnover_ratio),
            reprice_policy={
                'enabled': True,
                'max_reprice_count_per_child': int(PARAMS.get('TWAP3_MAX_REPRICE_COUNT', 2)),
                'reprice_if_price_drift_pct': float(PARAMS.get('TWAP3_REPRICE_DRIFT_PCT', 0.004)),
            },
            cancel_resume_policy={
                'enabled': True,
                'resume_unfilled_children_on_restart': True,
                'terminal_state_lock': True,
                'duplicate_event_dedupe': True,
            },
        ).as_dict()
        state = self._state()
        state['parents'][parent_order_id] = plan
        for child in children:
            state['children'][child['child_order_id']] = child
        state['events'].append({'ts': _now(), 'event': 'PLAN_CREATED', 'parent_order_id': parent_order_id, 'child_count': len(children), 'total_qty': int(total_qty)})
        state['events'] = state['events'][-500:]
        self._persist(state, plan)
        return plan

    def register_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(plan, dict) or not plan.get('parent_order_id'):
            return {'status': 'ignored_invalid_plan'}
        state = self._state()
        parent_id = str(plan.get('parent_order_id'))
        state['parents'][parent_id] = plan
        for child in plan.get('child_orders', []) or []:
            if isinstance(child, dict) and child.get('child_order_id'):
                state['children'][str(child['child_order_id'])] = child
        state['events'].append({'ts': _now(), 'event': 'PLAN_REGISTERED', 'parent_order_id': parent_id, 'child_count': len(plan.get('child_orders', []) or [])})
        state['events'] = state['events'][-500:]
        self._persist(state, plan)
        return {'status': 'registered', 'parent_order_id': parent_id, 'child_count': len(plan.get('child_orders', []) or [])}

    def apply_child_callback(self, event: dict[str, Any]) -> dict[str, Any]:
        state = self._state()
        event_id = str(event.get('event_id') or event.get('callback_id') or event.get('execution_id') or uuid.uuid4().hex)
        child_id = str(event.get('child_order_id') or event.get('client_order_id') or '')
        if not child_id or child_id not in state.get('children', {}):
            payload = {'status': 'ignored_unknown_child', 'event_id': event_id, 'child_order_id': child_id, 'ts': _now()}
            state['events'].append(payload)
            state['events'] = state['events'][-500:]
            self._persist(state)
            return payload
        child = dict(state['children'][child_id])
        if child.get('last_event_id') == event_id:
            return {'status': 'duplicate_ignored', 'event_id': event_id, 'child_order_id': child_id}
        old_status = _norm_status(child.get('status'))
        if old_status in TERMINAL:
            return {'status': 'terminal_locked', 'child_order_id': child_id, 'old_status': old_status, 'event_id': event_id}

        new_status = _norm_status(event.get('status') or event.get('order_status') or old_status)
        fill_qty = _safe_int(event.get('fill_qty') or event.get('filled_qty_delta') or event.get('last_qty'), 0)
        cumulative = event.get('cumulative_filled_qty', event.get('filled_qty'))
        if cumulative not in (None, ''):
            filled_qty = min(_safe_int(cumulative, 0), _safe_int(child.get('qty'), 0))
        else:
            filled_qty = min(_safe_int(child.get('filled_qty'), 0) + max(fill_qty, 0), _safe_int(child.get('qty'), 0))
        remaining_qty = max(0, _safe_int(child.get('qty'), 0) - filled_qty)
        if filled_qty > 0 and remaining_qty > 0 and new_status not in {'CANCELLED', 'REJECTED', 'REPLACED'}:
            new_status = 'PARTIALLY_FILLED'
        if remaining_qty == 0 and filled_qty > 0:
            new_status = 'FILLED'

        if new_status == 'REPLACED':
            child['reprice_count'] = _safe_int(child.get('reprice_count'), 0) + 1
            child['replace_parent_child_id'] = child.get('replace_parent_child_id') or child_id
            if event.get('replacement_child_order_id'):
                child['child_order_id'] = str(event.get('replacement_child_order_id'))

        child.update({
            'status': new_status,
            'filled_qty': int(filled_qty),
            'remaining_qty': int(remaining_qty),
            'broker_order_id': str(event.get('broker_order_id') or child.get('broker_order_id') or ''),
            'last_event_id': event_id,
            'last_update': _now(),
        })
        state['children'][child_id] = child
        parent_id = str(child.get('parent_order_id') or '')
        if parent_id and parent_id in state.get('parents', {}):
            parent = dict(state['parents'][parent_id])
            child_rows = [dict(c) for c in parent.get('child_orders', [])]
            for idx, row in enumerate(child_rows):
                if row.get('child_order_id') == child_id:
                    child_rows[idx] = child
                    break
            total_filled = sum(_safe_int(c.get('filled_qty'), 0) for c in child_rows)
            total_remaining = sum(_safe_int(c.get('remaining_qty'), 0) for c in child_rows)
            statuses = {_norm_status(c.get('status')) for c in child_rows}
            if total_remaining == 0 and total_filled > 0:
                parent_status = 'FILLED'
            elif 'REJECTED' in statuses:
                parent_status = 'REVIEW_REJECTED_CHILD'
            elif any(s == 'PARTIALLY_FILLED' for s in statuses) or total_filled > 0:
                parent_status = 'PARTIALLY_FILLED'
            elif any(s in {'SUBMITTED', 'PENDING_SUBMIT'} for s in statuses):
                parent_status = 'WORKING'
            else:
                parent_status = parent.get('status', 'CREATED')
            parent.update({'child_orders': child_rows, 'filled_qty': int(total_filled), 'remaining_qty': int(total_remaining), 'status': parent_status, 'updated_at': _now()})
            state['parents'][parent_id] = parent
        payload = {'ts': _now(), 'event': 'CALLBACK_APPLIED', 'event_id': event_id, 'child_order_id': child_id, 'old_status': old_status, 'new_status': new_status, 'filled_qty': filled_qty, 'remaining_qty': remaining_qty}
        state['events'].append(payload)
        state['events'] = state['events'][-500:]
        self._persist(state, state['parents'].get(parent_id) if parent_id else None)
        return {'status': 'applied', **payload}


    def build_broker_submission_queue(self, *, mark_pending: bool = True) -> dict[str, Any]:
        """Build a broker-neutral queue for children that still need submission.

        This closes the TWAP3 gap where a plan existed but no runtime artifact told
        the broker adapter what to submit next.  In paper mode callers may still
        apply fill callbacks immediately, but true-live adapters should consume
        ``runtime/twap3_broker_submission_queue.json``.
        """
        state = self._state()
        queue: list[dict[str, Any]] = []
        now_s = _now()
        for child_id, child_raw in list(state.get('children', {}).items()):
            child = dict(child_raw)
            st = _norm_status(child.get('status'))
            if st in TERMINAL or _safe_int(child.get('remaining_qty'), 0) <= 0:
                continue
            if st in {'CREATED', 'NEW', 'PENDING_SUBMIT'}:
                order = {
                    'client_order_id': str(child_id),
                    'parent_order_id': str(child.get('parent_order_id') or ''),
                    'ticker': str(child.get('ticker') or ''),
                    'side': str(child.get('side') or '').upper(),
                    'qty': int(_safe_int(child.get('remaining_qty'), child.get('qty', 0))),
                    'limit_price': float(_safe_float(child.get('limit_price'), 0.0)),
                    'scheduled_at': str(child.get('scheduled_at') or ''),
                    'execution_style': 'TWAP3',
                    'child_seq': int(_safe_int(child.get('child_seq'), 0)),
                    'child_count': int(_safe_int(child.get('child_count'), 0)),
                    'submission_status': 'READY_FOR_BROKER_SUBMIT',
                }
                queue.append(order)
                if mark_pending and st in {'CREATED', 'NEW'}:
                    child['status'] = 'PENDING_SUBMIT'
                    child['last_update'] = now_s
                    state['children'][child_id] = child
                    parent_id = str(child.get('parent_order_id') or '')
                    if parent_id in state.get('parents', {}):
                        parent = dict(state['parents'][parent_id])
                        parent_children = []
                        for row in parent.get('child_orders', []) or []:
                            if isinstance(row, dict) and row.get('child_order_id') == child_id:
                                row = {**row, **child}
                            parent_children.append(row)
                        parent['child_orders'] = parent_children
                        parent['status'] = 'PENDING_SUBMIT'
                        parent['updated_at'] = now_s
                        state['parents'][parent_id] = parent
        payload = {
            'module_version': self.MODULE_VERSION,
            'generated_at': now_s,
            'status': 'twap3_submission_queue_ready' if queue else 'twap3_no_children_to_submit',
            'queue_count': len(queue),
            'orders': queue,
        }
        _write_json(Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'twap3_broker_submission_queue.json', payload)
        state['events'].append({'ts': now_s, 'event': 'SUBMISSION_QUEUE_BUILT', 'queue_count': len(queue)})
        state['events'] = state['events'][-500:]
        self._persist(state)
        return payload

    def mark_child_submitted(self, child_order_id: str, broker_order_id: str = '', *, event_id: str | None = None) -> dict[str, Any]:
        return self.apply_child_callback({
            'event_id': event_id or f'submit-{child_order_id}-{uuid.uuid4().hex[:8]}',
            'child_order_id': child_order_id,
            'status': 'SUBMITTED',
            'broker_order_id': broker_order_id,
            'filled_qty': 0,
        })

    def cancel_parent(self, parent_order_id: str, reason: str = '') -> dict[str, Any]:
        state = self._state()
        parent = state.get('parents', {}).get(parent_order_id)
        if not isinstance(parent, dict):
            return {'status': 'parent_not_found', 'parent_order_id': parent_order_id}
        cancelled = []
        for child in list(parent.get('child_orders', []) or []):
            if not isinstance(child, dict):
                continue
            cid = str(child.get('child_order_id') or '')
            if not cid:
                continue
            st = _norm_status(child.get('status'))
            if st in TERMINAL:
                continue
            result = self.apply_child_callback({'event_id': f'cancel-{cid}-{uuid.uuid4().hex[:8]}', 'child_order_id': cid, 'status': 'CANCELLED', 'note': reason})
            cancelled.append(result)
        state = self._state()
        if parent_order_id in state.get('parents', {}):
            p = dict(state['parents'][parent_order_id])
            p['status'] = 'CANCELLED'
            p['cancel_reason'] = reason
            p['updated_at'] = _now()
            state['parents'][parent_order_id] = p
            self._persist(state, p)
        return {'status': 'parent_cancelled', 'parent_order_id': parent_order_id, 'cancelled_children': cancelled}

    def resume_open_orders(self) -> dict[str, Any]:
        state = self._state()
        open_children = []
        for child in state.get('children', {}).values():
            st = _norm_status(child.get('status'))
            if st not in TERMINAL and _safe_int(child.get('remaining_qty'), 0) > 0:
                open_children.append(child)
        payload = {
            'module_version': self.MODULE_VERSION,
            'generated_at': _now(),
            'status': 'resume_required' if open_children else 'no_open_twap3_children',
            'open_child_count': len(open_children),
            'open_children': open_children[:200],
        }
        _write_json(Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'twap3_resume_report.json', payload)
        return payload

    def summarize(self) -> dict[str, Any]:
        state = self._state()
        parents = state.get('parents', {})
        children = state.get('children', {})
        status_counts: dict[str, int] = {}
        for child in children.values():
            st = _norm_status(child.get('status'))
            status_counts[st] = status_counts.get(st, 0) + 1
        payload = {
            'module_version': self.MODULE_VERSION,
            'generated_at': _now(),
            'parent_count': len(parents),
            'child_count': len(children),
            'child_status_counts': status_counts,
            'open_resume_required': any(_norm_status(c.get('status')) not in TERMINAL and _safe_int(c.get('remaining_qty'), 0) > 0 for c in children.values()),
        }
        _write_json(Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'twap3_child_order_summary.json', payload)
        return payload


def plan_twap3_order(**kwargs: Any) -> dict[str, Any]:
    return TWAP3ChildOrderEngine().build_plan(**kwargs)


def apply_child_callback(event: dict[str, Any]) -> dict[str, Any]:
    return TWAP3ChildOrderEngine().apply_child_callback(event)


def resume_open_orders() -> dict[str, Any]:
    return TWAP3ChildOrderEngine().resume_open_orders()


def build_broker_submission_queue(mark_pending: bool = True) -> dict[str, Any]:
    return TWAP3ChildOrderEngine().build_broker_submission_queue(mark_pending=mark_pending)


if __name__ == '__main__':
    print(json.dumps(TWAP3ChildOrderEngine().summarize(), ensure_ascii=False, indent=2))
