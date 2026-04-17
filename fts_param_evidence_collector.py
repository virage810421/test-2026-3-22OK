# -*- coding: utf-8 -*-
"""Parameter paper/shadow evidence collector v3.

Purpose
-------
Close the gap between AI candidate judgement and release-gate evidence.
This module reads real runtime artifacts and writes paper/shadow evidence back
into param_storage.release without ever promoting live or editing config.py.

Safety rules
------------
- Decision/order planning files are only planning evidence.
- Paper pass requires execution-like runtime evidence, not just a candidate score.
- Shadow pass requires shadow runtime evidence hard gate when available.
- Missing evidence is fail-closed.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from config import PARAMS  # type: ignore
except Exception:  # pragma: no cover
    PARAMS = {}

try:
    from fts_utils import now_str  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')

from param_storage import (
    load_candidate,
    load_latest_candidate,
    load_approved_params,
    update_release_evidence,
)

RUNTIME_DIR = Path('runtime')
REPORT_PATH = RUNTIME_DIR / 'param_runtime_evidence_report.json'


def _read_json(path: Path) -> Any:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


def _rows(payload: Any, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    out: list[dict[str, Any]] = []
    for k in keys:
        v = payload.get(k)
        if isinstance(v, list):
            out.extend([x for x in v if isinstance(x, dict)])
    return out


def _status(row: dict[str, Any]) -> str:
    return str(row.get('status') or row.get('order_status') or row.get('event_type') or '').upper().strip()


def _candidate_for_scope(scope: str, candidate_id: str | None = None) -> dict[str, Any] | None:
    if candidate_id:
        return load_candidate(candidate_id)
    # Prefer approved/paper/shadow candidates over raw candidates for evidence update.
    statuses = {
        'approved_for_research',
        'approved_for_rebuild_training_data',
        'approved_for_paper',
        'approved_for_shadow',
        'candidate',
        'candidate_only_not_live',
    }
    return load_latest_candidate(scope_name=scope, statuses=statuses)


def _previous_approved_version(scope: str, current_candidate_id: str | None = None) -> str | None:
    approved = load_approved_params(scope)
    if not approved:
        return None
    cand = str(approved.get('candidate_id') or '')
    if current_candidate_id and cand == str(current_candidate_id):
        return str(approved.get('rollback_to') or approved.get('version') or cand or '') or None
    return str(approved.get('version') or cand or '') or None


def collect_runtime_evidence(scope: str, candidate_id: str | None = None, write_back: bool = True) -> dict[str, Any]:
    scope = str(scope or 'strategy_signal::default')
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    candidate = _candidate_for_scope(scope, candidate_id=candidate_id)
    cid = str(candidate.get('candidate_id')) if candidate else None

    ledger = _read_json(RUNTIME_DIR / 'execution_ledger_summary.json')
    journal = _read_json(RUNTIME_DIR / 'execution_journal_summary.json')
    paper_broker = _read_json(RUNTIME_DIR / 'paper_broker_summary.json')
    paper_exec = _read_json(RUNTIME_DIR / 'paper_execution_summary.json')
    decision_gate = _read_json(RUNTIME_DIR / 'decision_execution_formal_gate.json')
    release_existing = candidate.get('release', {}) if isinstance(candidate, dict) else {}

    # Build truthful shadow evidence when module exists.  It fails closed when
    # there is only planning output.
    shadow_payload: dict[str, Any] = {}
    try:
        from fts_shadow_runtime_evidence import build_shadow_runtime_evidence  # type: ignore
        _, shadow_payload = build_shadow_runtime_evidence()
    except Exception:
        shadow_payload = _read_json(RUNTIME_DIR / 'shadow_runtime_evidence.json')

    orders = _rows(ledger, ('orders', 'paper_orders', 'submitted_orders'))
    fills = _rows(ledger, ('fills', 'paper_fills'))
    journal_events = _safe_int(journal.get('total_event_count_estimate', journal.get('event_count', 0)) if isinstance(journal, dict) else 0, 0)
    paper_orders = _safe_int(paper_broker.get('order_count', paper_broker.get('orders', 0)) if isinstance(paper_broker, dict) else 0, 0)
    paper_fills = _safe_int(paper_broker.get('fill_count', paper_broker.get('fills', 0)) if isinstance(paper_broker, dict) else 0, 0)
    paper_exec_orders = _safe_int(paper_exec.get('order_count', paper_exec.get('orders', 0)) if isinstance(paper_exec, dict) else 0, 0)
    decision_final_order_count = _safe_int(decision_gate.get('final_order_count', 0) if isinstance(decision_gate, dict) else 0, 0)

    submitted_like = sum(1 for row in orders if _status(row) in {'SUBMITTED', 'PENDING_SUBMIT', 'PARTIALLY_FILLED', 'FILLED', 'REJECTED', 'CANCELLED', 'CANCELED', 'PAPER_FILLED'})
    rejected_like = sum(1 for row in orders if _status(row) in {'REJECTED', 'REJECT'})
    reject_rate = (rejected_like / submitted_like) if submitted_like else None

    paper_activity_count = len(orders) + len(fills) + paper_orders + paper_fills + paper_exec_orders + journal_events
    planning_only_count = decision_final_order_count
    max_reject_rate = float(PARAMS.get('PARAM_EVIDENCE_MAX_REJECT_RATE', 0.20))
    min_paper_activity = int(PARAMS.get('PARAM_EVIDENCE_MIN_PAPER_ACTIVITY', 1))
    paper_pass = bool(paper_activity_count >= min_paper_activity and (reject_rate is None or reject_rate <= max_reject_rate))

    shadow_gate = shadow_payload.get('promotion_hard_gate', {}) if isinstance(shadow_payload, dict) else {}
    shadow_runtime_observed = bool(shadow_payload.get('runtime_observed', False)) if isinstance(shadow_payload, dict) else False
    shadow_pass = bool(shadow_runtime_observed and shadow_gate.get('pass', False))

    reasons: list[str] = []
    if not paper_pass:
        reasons.append('paper_runtime_evidence_missing_or_reject_rate_too_high')
    if not shadow_pass:
        if shadow_payload.get('planning_only'):
            reasons.append('shadow_planning_only_not_runtime')
        else:
            reasons.append('shadow_runtime_evidence_missing_or_failed')
    if not cid:
        reasons.append('candidate_missing_for_scope')

    rollback_to = release_existing.get('rollback_to') or _previous_approved_version(scope, current_candidate_id=cid)
    evidence_artifacts = {
        'execution_ledger_summary': str(RUNTIME_DIR / 'execution_ledger_summary.json') if ledger else '',
        'execution_journal_summary': str(RUNTIME_DIR / 'execution_journal_summary.json') if journal else '',
        'paper_broker_summary': str(RUNTIME_DIR / 'paper_broker_summary.json') if paper_broker else '',
        'paper_execution_summary': str(RUNTIME_DIR / 'paper_execution_summary.json') if paper_exec else '',
        'shadow_runtime_evidence': str(RUNTIME_DIR / 'shadow_runtime_evidence.json') if shadow_payload else '',
    }
    payload = {
        'generated_at': now_str(),
        'scope': scope,
        'candidate_id': cid,
        'status': 'evidence_collected' if cid else 'no_candidate',
        'paper_pass': bool(paper_pass),
        'shadow_pass': bool(shadow_pass),
        'rollback_to': rollback_to,
        'paper_activity_count': int(paper_activity_count),
        'planning_only_count': int(planning_only_count),
        'submitted_like_count': int(submitted_like),
        'rejected_like_count': int(rejected_like),
        'reject_rate': reject_rate,
        'shadow_runtime_observed': bool(shadow_runtime_observed),
        'shadow_status': shadow_payload.get('status') if isinstance(shadow_payload, dict) else None,
        'shadow_hard_gate': shadow_gate,
        'reasons': reasons,
        'evidence_artifacts': evidence_artifacts,
        'truthful_rule': 'planning files do not count as paper/shadow pass by themselves',
        'writes_production_config': False,
        'promotes_live': False,
    }
    if cid and write_back:
        update_release_evidence(
            cid,
            paper_pass=paper_pass,
            shadow_pass=shadow_pass,
            rollback_to=rollback_to,
            release_gate_pass=False,
            reasons=reasons,
        )
    REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def collect_all_scopes(scopes: list[str] | None = None) -> dict[str, Any]:
    scopes = scopes or ['trainer::default', 'label_policy::default', 'strategy_signal::default', 'execution_policy::default']
    rows = [collect_runtime_evidence(scope=s, write_back=True) for s in scopes]
    payload = {'generated_at': now_str(), 'status': 'all_scope_evidence_collected', 'results': rows}
    out = RUNTIME_DIR / 'param_runtime_evidence_all_scopes.json'
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope', default='strategy_signal::default')
    parser.add_argument('--candidate-id', default=None)
    parser.add_argument('--all-scopes', action='store_true')
    parser.add_argument('--no-write-back', action='store_true')
    args = parser.parse_args(argv)
    if args.all_scopes:
        payload = collect_all_scopes()
    else:
        payload = collect_runtime_evidence(args.scope, candidate_id=args.candidate_id, write_back=not args.no_write_back)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get('status') in {'evidence_collected', 'all_scope_evidence_collected'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
