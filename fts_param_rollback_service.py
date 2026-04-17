# -*- coding: utf-8 -*-
"""Parameter rollback service v4.

Creates a rollback plan for a promoted parameter snapshot.  Applying rollback is
blocked by default.  When applied, it marks the current candidate as
rollback_required and re-approves the rollback target snapshot as
promoted_for_live; it still does not edit config.py and does not place orders.
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

REPORT_PATH = Path('runtime') / 'param_rollback_plan.json'


def _load_approved(scope: str) -> dict[str, Any]:
    try:
        from param_storage import load_approved_params  # type: ignore
        payload = load_approved_params(scope)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {'_error': repr(exc)}


def build_rollback_plan(scope: str, apply: bool = False, force: bool = False) -> dict[str, Any]:
    from param_storage import approve_candidate, load_candidate, transition_candidate_status  # type: ignore

    scope = str(scope or 'strategy_signal::default')
    current = _load_approved(scope)
    current_cid = str(current.get('candidate_id') or '')
    release = current.get('release', {}) if isinstance(current.get('release', {}), dict) else {}
    rollback_to = str(release.get('rollback_to') or '')
    reasons: list[str] = []
    if not current:
        reasons.append('current_approved_snapshot_missing')
    if str(current.get('status') or '') != 'promoted_for_live':
        reasons.append('current_snapshot_not_promoted_for_live')
    if not rollback_to:
        reasons.append('rollback_to_missing')
    target = load_candidate(rollback_to) if rollback_to else None
    if rollback_to and not target:
        reasons.append('rollback_target_candidate_missing')
    if target and (target.get('scope') != scope and target.get('scope_name') != scope):
        reasons.append('rollback_target_scope_mismatch')

    ready = len(reasons) == 0
    applied = False
    apply_allowed = bool(PARAMS.get('PARAM_ROLLBACK_ALLOW_APPLY', False)) or bool(force)
    apply_status = 'dry_run_plan_only'
    if apply:
        if not ready:
            apply_status = 'blocked_plan_not_ready'
        elif not apply_allowed:
            apply_status = 'blocked_apply_disabled_by_config'
        else:
            if current_cid:
                transition_candidate_status(current_cid, 'rollback_required', note='rollback plan applied; previous live snapshot retired')
            approve_candidate(
                rollback_to,
                approver='param_rollback_service',
                note=f'rollback applied from {current_cid}',
                status='promoted_for_live',
            )
            applied = True
            apply_status = 'rollback_snapshot_reapproved_promoted_for_live'

    payload = {
        'generated_at': now_str(),
        'scope': scope,
        'ready': bool(ready),
        'status': 'rollback_ready' if ready else 'rollback_blocked',
        'current_candidate_id': current_cid or None,
        'rollback_to': rollback_to or None,
        'reasons': reasons,
        'apply_requested': bool(apply),
        'applied': bool(applied),
        'apply_status': apply_status,
        'writes_production_config': False,
        'places_orders': False,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope', default='strategy_signal::default')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args(argv)
    payload = build_rollback_plan(args.scope, apply=args.apply, force=args.force)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get('ready') else 1


if __name__ == '__main__':
    raise SystemExit(main())
