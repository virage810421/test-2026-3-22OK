# -*- coding: utf-8 -*-
"""Central candidate AI judge.

Usage:
    python fts_candidate_ai_judge.py --scope trainer::default
    python fts_candidate_ai_judge.py --scope strategy_signal::default
    python fts_candidate_ai_judge.py --scope label_policy::default

The judge may approve for research/paper/shadow depending on config.  It never
promotes live; live promotion must go through fts_param_release_gate.py.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from config import PARAMS
from fts_utils import now_str
from param_storage import (
    approve_candidate,
    load_candidate,
    load_latest_candidate,
    mark_candidate_judgement,
    transition_candidate_status,
)

REPORT_PATH = Path('runtime') / 'candidate_ai_judge_report.json'
DECISIONS_PATH = Path('runtime') / 'candidate_ai_judge_decisions.json'


def _select_judge(scope: str):
    scope = str(scope or '')
    if scope.startswith('trainer::'):
        from fts_train_param_candidate_judge import judge_candidate
        return judge_candidate
    if scope.startswith('label_policy::'):
        from fts_label_policy_candidate_judge import judge_candidate
        return judge_candidate
    if scope.startswith('strategy_signal::'):
        from fts_strategy_param_candidate_judge import judge_candidate
        return judge_candidate
    if scope.startswith('execution_policy::'):
        from fts_execution_param_candidate_judge import judge_candidate
        return judge_candidate
    # Unknown scopes default to strategy-style conservative judgement.
    from fts_strategy_param_candidate_judge import judge_candidate
    return judge_candidate


def _auto_transition_allowed(recommended_status: str) -> bool:
    if recommended_status == 'approved_for_research':
        return bool(PARAMS.get('CANDIDATE_AI_AUTO_APPROVE_RESEARCH', True))
    if recommended_status == 'approved_for_rebuild_training_data':
        return bool(PARAMS.get('CANDIDATE_AI_AUTO_APPROVE_RESEARCH', True))
    if recommended_status == 'approved_for_paper':
        return bool(PARAMS.get('CANDIDATE_AI_AUTO_APPROVE_PAPER', True))
    if recommended_status == 'approved_for_shadow':
        return bool(PARAMS.get('CANDIDATE_AI_AUTO_APPROVE_SHADOW', True))
    if recommended_status == 'promoted_for_live':
        return bool(PARAMS.get('CANDIDATE_AI_AUTO_PROMOTE_LIVE', False))
    return False



def _apply_judgement(candidate: dict[str, Any], scope: str, auto_apply: bool = True) -> dict[str, Any]:
    """Judge one concrete candidate payload.

    Shared by judge_latest() and judge_candidate_by_id().  It preserves the
    existing safety contract: AI judge can approve non-live stages only; live
    promotion remains controlled by release gate.
    """
    judge_func = _select_judge(scope)
    judgement = judge_func(candidate)
    min_score = float(PARAMS.get('CANDIDATE_MIN_AI_SCORE', 75.0))
    require_hard = bool(PARAMS.get('CANDIDATE_MIN_HARD_GATE_PASS', True))
    hard_gate_pass = bool(judgement.get('hard_gate_pass', False))
    ai_score = float(judgement.get('ai_score', 0.0) or 0.0)
    recommended = str(judgement.get('recommended_status', 'rejected'))

    if ai_score < min_score:
        recommended = 'rejected'
        judgement['recommended_status'] = 'rejected'
        judgement.setdefault('reason', []).append('ai_score_below_minimum')
    if require_hard and not hard_gate_pass:
        recommended = 'rejected'
        judgement['recommended_status'] = 'rejected'
        judgement.setdefault('reason', []).append('hard_gate_failed')

    candidate_id = str(candidate.get('candidate_id'))
    applied_status = None
    approved_snapshot = None
    if auto_apply:
        if recommended == 'rejected':
            mark_candidate_judgement(candidate_id, judgement, status='rejected', note='AI judge rejected candidate')
            applied_status = 'rejected'
        elif _auto_transition_allowed(recommended):
            mark_candidate_judgement(candidate_id, judgement, status=recommended, note='AI judge accepted candidate for next non-live stage')
            applied_status = recommended
            if recommended in {
                'approved_for_research',
                'approved_for_rebuild_training_data',
                'approved_for_paper',
                'approved_for_shadow',
            }:
                approved_snapshot = approve_candidate(
                    candidate_id=candidate_id,
                    approver='candidate_ai_judge',
                    note='auto-approved by AI judge; live promotion still blocked',
                    status=recommended,
                )
        else:
            mark_candidate_judgement(candidate_id, judgement, status='candidate', note='AI judge recommendation not auto-applied by config')
            applied_status = 'candidate'

    payload = {
        'generated_at': now_str(),
        'status': 'judged',
        'scope': scope,
        'candidate_id': candidate_id,
        'ai_judge': judgement,
        'recommended_status': recommended,
        'applied_status': applied_status,
        'approved_snapshot_created': bool(approved_snapshot),
        'live_auto_promotion_allowed': bool(PARAMS.get('CANDIDATE_AI_AUTO_PROMOTE_LIVE', False)),
        'writes_production_config': False,
    }
    REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    rows = []
    if DECISIONS_PATH.exists():
        try:
            rows = json.loads(DECISIONS_PATH.read_text(encoding='utf-8'))
            if not isinstance(rows, list):
                rows = []
        except Exception:
            rows = []
    rows.append(payload)
    DECISIONS_PATH.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def judge_candidate_by_id(candidate_id: str, auto_apply: bool = True) -> dict[str, Any]:
    """Judge a specific candidate id.

    Optimizer modules call this after saving a candidate.  Missing helper used
    to cause auto_judge_failed, so this closes the candidate->judge branch while
    preserving the no-live-auto-promotion contract.
    """
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not bool(PARAMS.get('CANDIDATE_AI_JUDGE_ENABLED', True)):
        payload = {'generated_at': now_str(), 'candidate_id': str(candidate_id), 'status': 'disabled_by_config'}
        REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return payload
    candidate = load_candidate(str(candidate_id))
    if not candidate:
        payload = {'generated_at': now_str(), 'candidate_id': str(candidate_id), 'status': 'candidate_not_found'}
        REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return payload
    scope = str(candidate.get('scope_name') or candidate.get('scope') or 'trainer::default')
    return _apply_judgement(candidate=candidate, scope=scope, auto_apply=auto_apply)

def judge_latest(scope: str, auto_apply: bool = True) -> dict[str, Any]:
    scope = str(scope or 'trainer::default')
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not bool(PARAMS.get('CANDIDATE_AI_JUDGE_ENABLED', True)):
        payload = {'generated_at': now_str(), 'scope': scope, 'status': 'disabled_by_config'}
        REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return payload

    candidate = load_latest_candidate(scope_name=scope)
    if not candidate:
        payload = {'generated_at': now_str(), 'scope': scope, 'status': 'no_candidate'}
        REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return payload

    return _apply_judgement(candidate=candidate, scope=scope, auto_apply=auto_apply)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope', default='trainer::default')
    parser.add_argument('--no-apply', action='store_true', help='judge only; do not change candidate status')
    args = parser.parse_args()
    payload = judge_latest(scope=args.scope, auto_apply=not args.no_apply)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
