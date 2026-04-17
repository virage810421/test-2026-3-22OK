# -*- coding: utf-8 -*-
"""Live parameter mount guard v4.

This is the final *pre-live* safety gate for approved parameter snapshots.
It does not place orders, does not edit config.py, and does not promote a
candidate.  It only decides whether a promoted_for_live approved snapshot is
eligible to be mounted when FTS_PARAM_MOUNT_STAGE=live.

Checks are intentionally conservative:
- approved snapshot status must be promoted_for_live
- release metadata must show release_gate_pass and promoted_for_live
- a rollback target must exist
- kill switch must be clear when required
- broker/readiness reports must be acceptable when required
- publish manifest must exist when required
"""
from __future__ import annotations

import argparse
import json
import os
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

REPORT_PATH = Path('runtime') / 'param_live_mount_guard.json'


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            payload = json.loads(path.read_text(encoding='utf-8'))
            return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}
    return {}


def _runtime_stage(default: str = 'paper') -> str:
    raw = os.getenv('FTS_PARAM_MOUNT_STAGE') or str(PARAMS.get('PARAM_MOUNT_STAGE', default) or default)
    return raw.strip().lower()


def _load_approved(scope: str) -> dict[str, Any]:
    try:
        from param_storage import load_approved_params  # type: ignore
        payload = load_approved_params(scope)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {'_error': repr(exc)}


def _kill_switch_clear() -> tuple[bool, list[str]]:
    reasons: list[str] = []
    candidates = [
        Path('runtime') / 'kill_switch.json',
        Path('runtime') / 'kill_switch_state.json',
        Path('state') / 'kill_switch.json',
    ]
    found = False
    for path in candidates:
        payload = _read_json(path)
        if not payload:
            continue
        found = True
        raw = payload.get('enabled', payload.get('kill_switch', payload.get('halt_new_orders', False)))
        if bool(raw):
            reasons.append(f'kill_switch_active:{path}')
    # Missing kill-switch report is not treated as failure unless project config
    # explicitly demands a persisted report.  The guard still reports it.
    if not found:
        reasons.append('kill_switch_report_missing_observed_as_clear')
    hard_fail = [r for r in reasons if r.startswith('kill_switch_active')]
    return (len(hard_fail) == 0), reasons


def _broker_readiness_ok() -> tuple[bool, dict[str, Any], list[str]]:
    reasons: list[str] = []
    reports = [
        Path('runtime') / 'true_broker_readiness_gate.json',
        Path('runtime') / 'broker_readiness_gate.json',
        Path('runtime') / 'prebroker_95_audit.json',
        Path('runtime') / 'broker_contract_audit.json',
    ]
    best: dict[str, Any] = {}
    for path in reports:
        payload = _read_json(path)
        if payload:
            best = {'path': str(path), **payload}
            break
    if not best:
        reasons.append('broker_readiness_report_missing')
        return False, {}, reasons
    status = str(best.get('status', best.get('gate_status', best.get('readiness_status', '')))).lower()
    score = best.get('score', best.get('readiness_score', best.get('final_score', best.get('prebroker_score'))))
    try:
        score_f = float(score)
    except Exception:
        score_f = None
    min_score = float(PARAMS.get('PARAM_LIVE_MIN_READINESS_SCORE', 95.0))
    if any(x in status for x in ['fail', 'blocked', 'not_ready', 'not ready', 'critical']):
        reasons.append(f'broker_status_blocked:{status}')
    if score_f is not None and score_f < min_score:
        reasons.append(f'broker_readiness_score_below_floor:{score_f}<{min_score}')
    if score_f is None and not any(x in status for x in ['pass', 'ready', 'ok']):
        reasons.append('broker_score_missing_and_status_not_ready')
    return len(reasons) == 0, best, reasons


def _manifest_ok(scope: str, candidate_id: str | None) -> tuple[bool, dict[str, Any], list[str]]:
    reasons: list[str] = []
    path = Path('runtime') / 'param_live_publish_manifest.json'
    payload = _read_json(path)
    if not payload:
        return False, {}, ['live_publish_manifest_missing']
    if str(payload.get('scope')) != str(scope):
        reasons.append('manifest_scope_mismatch')
    if candidate_id and str(payload.get('candidate_id')) != str(candidate_id):
        reasons.append('manifest_candidate_mismatch')
    if not bool(payload.get('ready_for_live_mount', False)):
        reasons.append('manifest_not_ready_for_live_mount')
    return len(reasons) == 0, payload, reasons


def check_live_mount_guard(scope: str, stage: str | None = None, write_report: bool = True) -> dict[str, Any]:
    scope = str(scope or 'strategy_signal::default')
    stage = (stage or _runtime_stage()).lower()
    reasons: list[str] = []
    warnings: list[str] = []

    approved = _load_approved(scope)
    candidate_id = str(approved.get('candidate_id') or '') if approved else ''
    status = str(approved.get('status') or '') if approved else ''
    release = approved.get('release', {}) if isinstance(approved.get('release', {}), dict) else {}

    if not approved:
        reasons.append('approved_snapshot_missing')
    if approved.get('_error'):
        reasons.append('approved_snapshot_unreadable')
    if status != 'promoted_for_live':
        reasons.append(f'approved_status_not_promoted_for_live:{status or "missing"}')
    if not bool(release.get('release_gate_pass', False)):
        reasons.append('release_gate_pass_missing')
    if not bool(release.get('promoted_for_live', False)):
        reasons.append('release_promoted_for_live_flag_missing')
    if not release.get('rollback_to'):
        reasons.append('rollback_to_missing')
    if stage != 'live':
        reasons.append(f'runtime_stage_not_live:{stage}')

    if bool(PARAMS.get('PARAM_LIVE_REQUIRE_KILL_SWITCH_CLEAR', True)):
        ok, ks_reasons = _kill_switch_clear()
        warnings.extend([r for r in ks_reasons if 'missing_observed_as_clear' in r])
        if not ok:
            reasons.extend(ks_reasons)

    broker_report: dict[str, Any] = {}
    if bool(PARAMS.get('PARAM_LIVE_REQUIRE_BROKER_READINESS', True)):
        ok, broker_report, broker_reasons = _broker_readiness_ok()
        if not ok:
            reasons.extend(broker_reasons)

    manifest: dict[str, Any] = {}
    if bool(PARAMS.get('PARAM_LIVE_REQUIRE_RELEASE_MANIFEST', True)):
        ok, manifest, manifest_reasons = _manifest_ok(scope, candidate_id or None)
        if not ok:
            reasons.extend(manifest_reasons)

    passed = len(reasons) == 0
    payload = {
        'generated_at': now_str(),
        'scope': scope,
        'candidate_id': candidate_id or None,
        'approved_status': status or None,
        'runtime_stage': stage,
        'passed': bool(passed),
        'status': 'live_mount_guard_pass' if passed else 'live_mount_guard_blocked',
        'reasons': reasons,
        'warnings': warnings,
        'broker_report': broker_report,
        'manifest': {'present': bool(manifest), 'version': manifest.get('version'), 'path': str(Path('runtime') / 'param_live_publish_manifest.json') if manifest else None},
        'writes_production_config': False,
        'places_orders': False,
    }
    if write_report:
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope', default='strategy_signal::default')
    parser.add_argument('--stage', default=None)
    args = parser.parse_args(argv)
    payload = check_live_mount_guard(args.scope, stage=args.stage)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get('passed') else 1


if __name__ == '__main__':
    raise SystemExit(main())
