# -*- coding: utf-8 -*-
"""Approved parameter mount helper v2.

Purpose:
- Keep candidate/approved storage separated from production config.
- Mount only approved snapshots when an explicit config switch is enabled.
- Respect runtime stage so paper/shadow can test approved params without
  accidentally allowing unpromoted live usage.

This module never writes config.py and never promotes live.
"""
from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable

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

MOUNT_REPORT_PATH = Path('runtime') / 'approved_param_mount_report.json'

_SCOPE_RULES = {
    'trainer': {
        'scope_key': 'TRAIN_APPROVED_PARAM_SCOPE',
        'switch_key': 'TRAIN_USE_APPROVED_PARAMS',
        'default_scope': 'trainer::default',
        'allowed': {'approved_for_research', 'promoted_for_live'},
    },
    'label_policy': {
        'scope_key': 'LABEL_APPROVED_PARAM_SCOPE',
        'switch_key': 'LABEL_USE_APPROVED_POLICY',
        'default_scope': 'label_policy::default',
        'allowed': {'approved_for_rebuild_training_data', 'promoted_for_live'},
    },
    'strategy_signal': {
        'scope_key': 'STRATEGY_APPROVED_PARAM_SCOPE',
        'switch_key': 'STRATEGY_USE_APPROVED_PARAMS',
        'default_scope': 'strategy_signal::default',
        'stage_sensitive': True,
    },
    'execution_policy': {
        'scope_key': 'EXECUTION_APPROVED_PARAM_SCOPE',
        'switch_key': 'EXECUTION_USE_APPROVED_PARAMS',
        'default_scope': 'execution_policy::default',
        'stage_sensitive': True,
    },
}

_STAGE_ALLOWED = {
    'research': {'approved_for_research', 'approved_for_paper', 'approved_for_shadow', 'promoted_for_live'},
    'paper': {'approved_for_paper', 'approved_for_shadow', 'promoted_for_live'},
    'shadow': {'approved_for_shadow', 'promoted_for_live'},
    'live': {'promoted_for_live'},
}


def _params_copy(base_params: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        return dict(base_params or PARAMS or {})
    except Exception:
        return {}


def runtime_stage(default: str = 'paper') -> str:
    """Return current param mount stage.

    Use FTS_PARAM_MOUNT_STAGE=paper|shadow|live|research to control which
    approved statuses may mount.  Default is paper because this helper is used
    primarily before real-broker live deployment.
    """
    raw = os.getenv('FTS_PARAM_MOUNT_STAGE') or str(PARAMS.get('PARAM_MOUNT_STAGE', default) or default)
    stage = raw.strip().lower()
    return stage if stage in _STAGE_ALLOWED else default


def _write_mount_report(entry: dict[str, Any]) -> None:
    try:
        MOUNT_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        if MOUNT_REPORT_PATH.exists():
            try:
                rows = json.loads(MOUNT_REPORT_PATH.read_text(encoding='utf-8'))
                if not isinstance(rows, list):
                    rows = []
            except Exception:
                rows = []
        rows.append(entry)
        MOUNT_REPORT_PATH.write_text(json.dumps(rows[-200:], ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        pass


def _load_approved(scope_name: str) -> dict[str, Any]:
    try:
        from param_storage import load_approved_params  # type: ignore
        payload = load_approved_params(scope_name)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {'_mount_error': repr(exc)}


def allowed_statuses_for_mode(mode: str, stage: str | None = None) -> set[str]:
    rule = _SCOPE_RULES.get(str(mode), {})
    if rule.get('stage_sensitive'):
        return set(_STAGE_ALLOWED.get(stage or runtime_stage(), _STAGE_ALLOWED['paper']))
    return set(rule.get('allowed') or {'promoted_for_live'})


def resolve_approved_params_for_scope(
    base_params: dict[str, Any] | None,
    scope_name: str,
    enabled: bool,
    allowed_statuses: Iterable[str] | None = None,
    mount_context: str = '',
) -> dict[str, Any]:
    """Return base params plus approved params if explicitly enabled and valid.

    This function never reads candidate snapshots and never mutates config.py.
    """
    effective = _params_copy(base_params)
    scope = str(scope_name or 'default')
    allowed = set(allowed_statuses or {'promoted_for_live'})
    report = {
        'generated_at': now_str(),
        'context': mount_context,
        'scope': scope,
        'enabled': bool(enabled),
        'allowed_statuses': sorted(allowed),
        'mounted': False,
        'status': 'disabled',
        'params_mounted': [],
    }
    if not enabled:
        _write_mount_report(report)
        effective['_approved_param_mount'] = {'enabled': False, 'scope': scope, 'mounted': False}
        return effective

    approved = _load_approved(scope)
    if not approved or approved.get('_mount_error'):
        report['status'] = 'approved_snapshot_missing_or_unreadable'
        if approved.get('_mount_error'):
            report['error'] = approved.get('_mount_error')
        _write_mount_report(report)
        effective['_approved_param_mount'] = {'enabled': True, 'scope': scope, 'mounted': False, 'reason': report['status']}
        return effective

    status = str(approved.get('status') or '')
    if status not in allowed:
        report['status'] = 'approved_status_not_allowed_for_context'
        report['approved_status'] = status
        _write_mount_report(report)
        effective['_approved_param_mount'] = {'enabled': True, 'scope': scope, 'mounted': False, 'reason': report['status'], 'approved_status': status}
        return effective

    params = approved.get('params', {}) if isinstance(approved.get('params', {}), dict) else {}
    protected_prefixes = {'CANDIDATE_', 'PARAM_RELEASE_', 'LIVE_REQUIRE_', 'MODEL_MIN_OOT_', 'MODEL_MIN_PROMOTION_', 'KILL_SWITCH'}
    safe_params = {}
    rejected_keys = []
    for k, v in params.items():
        key = str(k)
        if any(key.startswith(prefix) for prefix in protected_prefixes):
            rejected_keys.append(key)
            continue
        safe_params[key] = v
    effective.update(deepcopy(safe_params))
    report.update({
        'mounted': bool(safe_params),
        'status': 'mounted' if safe_params else 'no_safe_params_to_mount',
        'approved_status': status,
        'candidate_id': approved.get('candidate_id'),
        'version': approved.get('version'),
        'params_mounted': sorted(safe_params.keys()),
        'protected_keys_rejected': sorted(rejected_keys),
    })
    _write_mount_report(report)
    effective['_approved_param_mount'] = {
        'enabled': True,
        'scope': scope,
        'mounted': bool(safe_params),
        'approved_status': status,
        'candidate_id': approved.get('candidate_id'),
        'version': approved.get('version'),
        'params_mounted': sorted(safe_params.keys()),
    }
    return effective


def get_effective_params_for_mode(mode: str, base_params: dict[str, Any] | None = None, stage: str | None = None) -> dict[str, Any]:
    mode = str(mode)
    rule = _SCOPE_RULES.get(mode)
    effective = _params_copy(base_params)
    if not rule:
        return effective
    scope = str(effective.get(rule['scope_key'], rule['default_scope']))
    enabled = bool(effective.get(rule['switch_key'], False))
    allowed = allowed_statuses_for_mode(mode, stage=stage)
    return resolve_approved_params_for_scope(
        base_params=effective,
        scope_name=scope,
        enabled=enabled,
        allowed_statuses=allowed,
        mount_context=mode,
    )


def build_mount_summary() -> dict[str, Any]:
    stage = runtime_stage()
    summary = {
        'generated_at': now_str(),
        'runtime_stage': stage,
        'modes': {},
        'status': 'approved_param_mount_summary_ready',
        'note': 'Only approved snapshots are eligible; candidate snapshots never mount directly.',
    }
    for mode in _SCOPE_RULES:
        params = get_effective_params_for_mode(mode, dict(PARAMS), stage=stage)
        summary['modes'][mode] = params.get('_approved_param_mount', {})
    out = Path('runtime') / 'approved_param_mount_summary.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return summary


def main() -> None:
    payload = build_mount_summary()
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
