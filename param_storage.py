# -*- coding: utf-8 -*-
"""Research-only parameter candidate storage with AI judge/release metadata.

Contract:
- Candidate snapshots never write production config.
- Approved snapshots are still inert until the corresponding explicit mount
  switch is enabled in config.py.
- promoted_for_live can only be set by release-gate style code; candidate AI
  judgement alone must not promote live.
"""
from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

from fts_utils import now_str
from fts_research_lab import ResearchLab

_LAB = ResearchLab()
_AREA = 'param_storage'

CANDIDATE_STATUSES = {
    'candidate',
    'candidate_only_not_live',  # backward compatibility
}
APPROVED_STATUSES = {
    'approved_snapshot_only',  # backward compatibility
    'approved_for_research',
    'approved_for_rebuild_training_data',
    'approved_for_paper',
    'approved_for_shadow',
    'promoted_for_live',
}
TERMINAL_STATUSES = {'rejected', 'rollback_required'}


def _safe_scope(scope_name: str) -> str:
    return str(scope_name or 'default').replace('/', '_').replace('\\', '_').replace(':', '_')


def _candidate_file(candidate_id: str) -> str:
    return f'candidate_params_{candidate_id}.json'


def _approved_file(scope_name: str) -> str:
    return f'approved_params_{_safe_scope(scope_name)}.json'


def _runtime_release_default() -> dict[str, Any]:
    return {
        'paper_pass': False,
        'shadow_pass': False,
        'promoted_for_live': False,
        'rollback_to': None,
        'release_gate_pass': False,
        'release_gate_reason': [],
    }


def _default_ai_judge() -> dict[str, Any]:
    return {
        'enabled': False,
        'ai_score': 0.0,
        'hard_gate_pass': False,
        'recommended_status': 'candidate',
        'reason': [],
    }


def _read_json_path(path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _write_candidate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    candidate_id = str(payload.get('candidate_id') or '')
    if not candidate_id:
        raise ValueError('candidate_id is required')
    artifact = _LAB.write_json_artifact(_AREA, _candidate_file(candidate_id), payload)
    return {'payload': payload, 'artifact_path': str(artifact)}


def _refresh_registry_status(candidate_id: str, status: str, artifact_path: str | None = None) -> None:
    path = _LAB.registry_path(_AREA)
    rows = _LAB.load_registry(_AREA)
    changed = False
    for row in rows:
        if str(row.get('candidate_id')) == str(candidate_id):
            row['status'] = status
            row['updated_at'] = now_str()
            if artifact_path:
                row['artifact_path'] = str(artifact_path)
            changed = True
    if changed:
        path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')


def save_candidate_params(
    scope_name: str,
    best_params: dict[str, Any],
    metrics: dict[str, Any] | None = None,
    source_module: str = 'unknown',
    note: str = '',
    status: str = 'candidate',
    ai_judge: dict[str, Any] | None = None,
    release: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Save a research candidate.  Never writes production config."""
    candidate_id = now_str().replace(':', '').replace('-', '').replace('T', '_').replace(' ', '_')
    payload = {
        'candidate_id': candidate_id,
        'scope': str(scope_name),
        'scope_name': str(scope_name),
        'version': candidate_id,
        'source_module': str(source_module),
        'generated_at': now_str(),
        'updated_at': now_str(),
        'note': str(note),
        'metrics': metrics or {},
        'params': deepcopy(best_params or {}),
        'status': status if status else 'candidate',
        'ai_judge': deepcopy(ai_judge) if isinstance(ai_judge, dict) else _default_ai_judge(),
        'release': deepcopy(release) if isinstance(release, dict) else _runtime_release_default(),
        'writes_production_config': False,
    }
    artifact = _LAB.write_json_artifact(_AREA, _candidate_file(candidate_id), payload)
    registry_entry = {
        'candidate_id': candidate_id,
        'scope_name': str(scope_name),
        'scope': str(scope_name),
        'source_module': str(source_module),
        'generated_at': payload['generated_at'],
        'updated_at': payload['updated_at'],
        'status': payload['status'],
        'artifact_path': str(artifact),
        'metric_keys': sorted(list((metrics or {}).keys())),
        'writes_production_config': False,
    }
    _LAB.append_registry(_AREA, registry_entry)
    return payload


def load_candidate(candidate_id: str) -> dict[str, Any] | None:
    path = _LAB.area(_AREA) / _candidate_file(candidate_id)
    if not path.exists():
        return None
    payload = _read_json_path(path)
    return payload if payload else None


def load_all_candidates(scope_name: str | None = None, include_terminal: bool = False) -> list[dict[str, Any]]:
    rows = _LAB.load_registry(_AREA)
    out: list[dict[str, Any]] = []
    for row in rows:
        status = str(row.get('status') or '')
        if (not include_terminal) and status in TERMINAL_STATUSES:
            continue
        if scope_name is not None and row.get('scope_name') != scope_name and row.get('scope') != scope_name:
            continue
        path = row.get('artifact_path')
        payload = _read_json_path(__import__('pathlib').Path(path)) if path else {}
        if payload:
            out.append(payload)
    return out


def load_latest_candidate(scope_name: str | None = None, statuses: set[str] | None = None) -> dict[str, Any] | None:
    statuses = statuses or CANDIDATE_STATUSES
    rows = _LAB.load_registry(_AREA)
    rows = [r for r in rows if str(r.get('status')) in statuses]
    if scope_name is not None:
        rows = [r for r in rows if r.get('scope_name') == scope_name or r.get('scope') == scope_name]
    if not rows:
        return None
    path = rows[-1].get('artifact_path')
    if not path:
        return None
    payload = _read_json_path(__import__('pathlib').Path(path))
    return payload if payload else None


def transition_candidate_status(candidate_id: str, status: str, note: str = '') -> dict[str, Any]:
    payload = load_candidate(candidate_id)
    if not payload:
        raise FileNotFoundError(f'candidate not found: {candidate_id}')
    if str(payload.get('status')) == 'promoted_for_live' and status != 'rollback_required':
        raise ValueError('promoted_for_live candidate can only transition to rollback_required')
    payload['status'] = str(status)
    payload['updated_at'] = now_str()
    if note:
        payload.setdefault('status_notes', []).append({'at': now_str(), 'note': str(note)})
    result = _write_candidate_payload(payload)
    _refresh_registry_status(candidate_id, str(status), result.get('artifact_path'))
    return payload


def mark_candidate_judgement(
    candidate_id: str,
    ai_judge: dict[str, Any],
    status: str | None = None,
    note: str = '',
) -> dict[str, Any]:
    payload = load_candidate(candidate_id)
    if not payload:
        raise FileNotFoundError(f'candidate not found: {candidate_id}')
    payload['ai_judge'] = deepcopy(ai_judge or {})
    payload['ai_judge'].setdefault('enabled', True)
    payload['updated_at'] = now_str()
    if status:
        payload['status'] = str(status)
    if note:
        payload.setdefault('status_notes', []).append({'at': now_str(), 'note': str(note)})
    result = _write_candidate_payload(payload)
    _refresh_registry_status(candidate_id, str(payload.get('status')), result.get('artifact_path'))
    return payload


def update_release_evidence(
    candidate_id: str,
    paper_pass: bool | None = None,
    shadow_pass: bool | None = None,
    promoted_for_live: bool | None = None,
    rollback_to: str | None = None,
    release_gate_pass: bool | None = None,
    reasons: list[str] | None = None,
) -> dict[str, Any]:
    payload = load_candidate(candidate_id)
    if not payload:
        raise FileNotFoundError(f'candidate not found: {candidate_id}')
    release = payload.setdefault('release', _runtime_release_default())
    if paper_pass is not None:
        release['paper_pass'] = bool(paper_pass)
    if shadow_pass is not None:
        release['shadow_pass'] = bool(shadow_pass)
    if promoted_for_live is not None:
        release['promoted_for_live'] = bool(promoted_for_live)
    if rollback_to is not None:
        release['rollback_to'] = rollback_to
    if release_gate_pass is not None:
        release['release_gate_pass'] = bool(release_gate_pass)
    if reasons is not None:
        release['release_gate_reason'] = list(reasons)
    payload['updated_at'] = now_str()
    result = _write_candidate_payload(payload)
    _refresh_registry_status(candidate_id, str(payload.get('status')), result.get('artifact_path'))
    return payload


def approve_candidate(
    candidate_id: str,
    approver: str = 'manual',
    note: str = '',
    status: str = 'approved_for_research',
) -> dict[str, Any]:
    """Create an approved snapshot for a candidate.

    Approved snapshots are inert until a corresponding explicit config switch is
    enabled.  This function does not promote live.
    """
    payload = load_candidate(candidate_id)
    if not payload:
        raise FileNotFoundError(f'candidate not found: {candidate_id}')
    scope_name = payload.get('scope_name') or payload.get('scope') or 'default'
    approved = {
        'approved_at': now_str(),
        'approved_by': str(approver),
        'approval_note': str(note),
        'candidate_id': candidate_id,
        'scope_name': scope_name,
        'scope': scope_name,
        'version': payload.get('version', candidate_id),
        'source_module': payload.get('source_module'),
        'metrics': payload.get('metrics', {}),
        'params': payload.get('params', {}),
        'status': status,
        'ai_judge': payload.get('ai_judge', _default_ai_judge()),
        'release': payload.get('release', _runtime_release_default()),
        'writes_production_config': False,
        'live_effect': 'none_until_explicitly_loaded_by_scope_mount_switch',
    }
    artifact = _LAB.write_json_artifact(_AREA, _approved_file(str(scope_name)), approved)
    _LAB.append_registry(_AREA, {
        'candidate_id': candidate_id,
        'scope_name': scope_name,
        'scope': scope_name,
        'source_module': payload.get('source_module'),
        'generated_at': approved['approved_at'],
        'updated_at': approved['approved_at'],
        'status': approved['status'],
        'artifact_path': str(artifact),
        'approved_by': str(approver),
        'writes_production_config': False,
    })
    transition_candidate_status(candidate_id, status, note=f'approved_by={approver}; {note}')
    return approved


def approve_latest_candidate(scope_name: str, approver: str = 'auto', note: str = '', status: str = 'approved_for_research') -> dict[str, Any] | None:
    latest = load_latest_candidate(scope_name=scope_name)
    if not latest:
        return None
    return approve_candidate(candidate_id=str(latest.get('candidate_id')), approver=approver, note=note, status=status)


def load_approved_params(scope_name: str = 'default') -> dict[str, Any]:
    path = _LAB.area(_AREA) / _approved_file(scope_name)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
        if str(payload.get('status')) in APPROVED_STATUSES:
            return payload
        return {}
    except Exception:
        return {}


def resolve_params_for_context(
    base_params: dict[str, Any] | None = None,
    scope_name: str | None = None,
    sector_name: str | None = None,
    regime: str | None = None,
    strategy_name: str | None = None,
) -> dict[str, Any]:
    effective = deepcopy(base_params or {})
    resolution_chain: list[str] = []
    for scope in [
        'default',
        f'sector::{sector_name}' if sector_name else None,
        f'regime::{regime}' if regime else None,
        f'strategy::{strategy_name}' if strategy_name else None,
        scope_name,
    ]:
        if not scope:
            continue
        approved = load_approved_params(scope)
        params = approved.get('params', {}) if isinstance(approved, dict) else {}
        if params:
            effective.update(params)
            resolution_chain.append(str(scope))
    effective['_approved_resolution_chain'] = resolution_chain
    return effective


def summary() -> dict[str, Any]:
    rows = _LAB.load_registry(_AREA)
    counts: dict[str, int] = {}
    for r in rows:
        status = str(r.get('status') or 'unknown')
        counts[status] = counts.get(status, 0) + 1
    latest_candidate = load_latest_candidate()
    return {
        'generated_at': now_str(),
        'status_counts': counts,
        'candidate_count': sum(counts.get(s, 0) for s in CANDIDATE_STATUSES),
        'approved_count': sum(counts.get(s, 0) for s in APPROVED_STATUSES),
        'latest_candidate_scope': latest_candidate.get('scope_name') if latest_candidate else None,
        'separation_guarantee': 'candidate and approved snapshots are separated from production config',
        'status': 'param_storage_ai_governance_ready',
    }


# Legacy compatibility helpers

def save_sector_params(sector_name, best_params):
    return save_candidate_params(scope_name=f'sector::{sector_name}', best_params=best_params, source_module='legacy_save_sector_params')


def load_all_params():
    return {'approved': load_approved_params('default'), 'summary': summary()}
