# -*- coding: utf-8 -*-
"""Parameter release gate v3.

Only this gate may transition a candidate to promoted_for_live.  It refreshes
paper/shadow runtime evidence first, then checks AI judgement, hard gates,
rollback availability and live-promotion configuration.

Default remains fail-closed: live auto-promotion is disabled unless explicitly
forced or PARAM_RELEASE_ALLOW_LIVE_AUTO_PROMOTION=True.
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
    approve_candidate,
    load_candidate,
    load_latest_candidate,
    transition_candidate_status,
    update_release_evidence,
)

REPORT_PATH = Path('runtime') / 'param_release_gate_report.json'

_PRELIVE_STATUSES = {
    'approved_for_research',
    'approved_for_rebuild_training_data',
    'approved_for_paper',
    'approved_for_shadow',
    'candidate',
    'candidate_only_not_live',
}


def _refresh_evidence(scope: str, candidate_id: str | None = None, enabled: bool = True) -> dict[str, Any]:
    if not enabled:
        return {'status': 'evidence_refresh_disabled'}
    try:
        from fts_param_evidence_collector import collect_runtime_evidence  # type: ignore
        return collect_runtime_evidence(scope=scope, candidate_id=candidate_id, write_back=True)
    except Exception as exc:
        return {'status': 'evidence_refresh_failed', 'error': repr(exc)}


def _load_target(scope: str, candidate_id: str | None = None) -> dict[str, Any] | None:
    if candidate_id:
        return load_candidate(candidate_id)
    return load_latest_candidate(scope_name=scope, statuses=_PRELIVE_STATUSES)


def run_release_gate(
    scope: str,
    candidate_id: str | None = None,
    force: bool = False,
    refresh_evidence: bool | None = None,
) -> dict[str, Any]:
    scope = str(scope or 'strategy_signal::default')
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if refresh_evidence is None:
        refresh_evidence = bool(PARAMS.get('PARAM_RELEASE_AUTO_REFRESH_EVIDENCE', True))
    evidence = _refresh_evidence(scope, candidate_id=candidate_id, enabled=bool(refresh_evidence))
    candidate = _load_target(scope, candidate_id=candidate_id)
    if not candidate:
        payload = {
            'generated_at': now_str(),
            'scope': scope,
            'status': 'no_candidate',
            'evidence': evidence,
            'writes_production_config': False,
        }
        REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return payload

    cid = str(candidate.get('candidate_id'))
    # Reload after evidence refresh in case release fields were updated.
    reloaded = load_candidate(cid)
    if reloaded:
        candidate = reloaded
    ai = candidate.get('ai_judge', {}) or {}
    rel = candidate.get('release', {}) or {}
    score = float(ai.get('ai_score', 0.0) or 0.0)
    reasons: list[str] = []

    if not bool(ai.get('hard_gate_pass', False)):
        reasons.append('ai_hard_gate_not_passed')
    if score < float(PARAMS.get('PARAM_RELEASE_MIN_AI_SCORE', 75.0)):
        reasons.append('ai_score_below_release_floor')
    if bool(PARAMS.get('PARAM_RELEASE_REQUIRE_PAPER', True)) and not bool(rel.get('paper_pass', False)):
        reasons.append('paper_pass_required')
    if bool(PARAMS.get('PARAM_RELEASE_REQUIRE_SHADOW', True)) and not bool(rel.get('shadow_pass', False)):
        reasons.append('shadow_pass_required')
    if str(candidate.get('status')) not in {'approved_for_shadow', 'approved_for_paper', 'approved_for_research', 'approved_for_rebuild_training_data'}:
        reasons.append('candidate_not_in_approved_pre_live_status')
    if not rel.get('rollback_to'):
        reasons.append('rollback_version_missing')
    if bool(ai.get('attempted_protected_key_edit', False)):
        reasons.append('protected_key_edit_attempted')
    if not bool(PARAMS.get('PARAM_RELEASE_ALLOW_LIVE_AUTO_PROMOTION', False)) and not force:
        reasons.append('live_auto_promotion_disabled_by_config')

    passed = len(reasons) == 0
    approved: dict[str, Any] | None = None
    if passed:
        update_release_evidence(cid, promoted_for_live=True, release_gate_pass=True, reasons=['release_gate_pass'])
        transition_candidate_status(cid, 'promoted_for_live', note='release gate promoted candidate for live mount')
        approved = approve_candidate(
            cid,
            approver='param_release_gate',
            note='release gate promoted; live mount still requires explicit scope switch and live runtime stage',
            status='promoted_for_live',
        )
    else:
        update_release_evidence(cid, release_gate_pass=False, reasons=reasons)

    payload = {
        'generated_at': now_str(),
        'scope': scope,
        'candidate_id': cid,
        'candidate_status': candidate.get('status'),
        'status': 'promoted_for_live' if passed else 'blocked',
        'passed': bool(passed),
        'reasons': reasons,
        'force': bool(force),
        'evidence_refresh': evidence,
        'ai_score': score,
        'paper_pass': bool(rel.get('paper_pass', False)),
        'shadow_pass': bool(rel.get('shadow_pass', False)),
        'rollback_to': rel.get('rollback_to'),
        'approved_snapshot_created': bool(approved),
        'writes_production_config': False,
        'live_mount_still_requires_scope_switch': True,
        'live_mount_requires_FTS_PARAM_MOUNT_STAGE_live': True,
    }
    REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope', default='strategy_signal::default')
    parser.add_argument('--candidate-id', default=None)
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--no-refresh-evidence', action='store_true')
    args = parser.parse_args(argv)
    payload = run_release_gate(
        args.scope,
        candidate_id=args.candidate_id,
        force=args.force,
        refresh_evidence=not args.no_refresh_evidence,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get('passed') else 1


if __name__ == '__main__':
    raise SystemExit(main())
