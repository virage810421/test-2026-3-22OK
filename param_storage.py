# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

from fts_utils import now_str
from fts_research_lab import ResearchLab

_LAB = ResearchLab()
_AREA = 'param_storage'


def _candidate_file(candidate_id: str) -> str:
    return f'candidate_params_{candidate_id}.json'


def _approved_file(scope_name: str) -> str:
    safe_scope = str(scope_name).replace('/', '_').replace('\\', '_')
    return f'approved_params_{safe_scope}.json'


def save_candidate_params(scope_name: str, best_params: dict[str, Any], metrics: dict[str, Any] | None = None,
                          source_module: str = 'unknown', note: str = '') -> dict[str, Any]:
    candidate_id = now_str().replace(':', '').replace('-', '').replace('T', '_').replace(' ', '_')
    payload = {
        'candidate_id': candidate_id,
        'scope_name': str(scope_name),
        'source_module': str(source_module),
        'generated_at': now_str(),
        'note': str(note),
        'metrics': metrics or {},
        'params': deepcopy(best_params or {}),
        'status': 'candidate_only_not_live',
        'writes_production_config': False,
    }
    artifact = _LAB.write_json_artifact(_AREA, _candidate_file(candidate_id), payload)
    registry_entry = {
        'candidate_id': candidate_id,
        'scope_name': str(scope_name),
        'source_module': str(source_module),
        'generated_at': payload['generated_at'],
        'status': payload['status'],
        'artifact_path': str(artifact),
        'metric_keys': sorted(list((metrics or {}).keys())),
    }
    _LAB.append_registry(_AREA, registry_entry)
    return payload


def approve_candidate(candidate_id: str, approver: str = 'manual', note: str = '') -> dict[str, Any]:
    area = _LAB.area(_AREA)
    candidate_path = area / _candidate_file(candidate_id)
    if not candidate_path.exists():
        raise FileNotFoundError(f'candidate not found: {candidate_id}')
    payload = json.loads(candidate_path.read_text(encoding='utf-8'))
    scope_name = payload.get('scope_name', 'default')
    approved = {
        'approved_at': now_str(),
        'approved_by': str(approver),
        'approval_note': str(note),
        'candidate_id': candidate_id,
        'scope_name': scope_name,
        'source_module': payload.get('source_module'),
        'metrics': payload.get('metrics', {}),
        'params': payload.get('params', {}),
        'status': 'approved_snapshot_only',
        'live_effect': 'none_until_explicitly_loaded_by_training_or_live_mount_switch',
    }
    artifact = _LAB.write_json_artifact(_AREA, _approved_file(scope_name), approved)
    _LAB.append_registry(_AREA, {
        'candidate_id': candidate_id,
        'scope_name': scope_name,
        'source_module': payload.get('source_module'),
        'generated_at': approved['approved_at'],
        'status': approved['status'],
        'artifact_path': str(artifact),
        'approved_by': str(approver),
    })
    return approved


def approve_latest_candidate(scope_name: str, approver: str = 'auto', note: str = '') -> dict[str, Any] | None:
    latest = load_latest_candidate(scope_name=scope_name)
    if not latest:
        return None
    return approve_candidate(candidate_id=str(latest.get('candidate_id')), approver=approver, note=note)


def load_all_candidates() -> list[dict[str, Any]]:
    rows = []
    for item in _LAB.load_registry(_AREA):
        if item.get('status') == 'candidate_only_not_live':
            rows.append(item)
    return rows


def load_latest_candidate(scope_name: str | None = None) -> dict[str, Any] | None:
    rows = _LAB.load_registry(_AREA)
    rows = [r for r in rows if r.get('status') == 'candidate_only_not_live']
    if scope_name is not None:
        rows = [r for r in rows if r.get('scope_name') == scope_name]
    if not rows:
        return None
    path = rows[-1].get('artifact_path')
    try:
        return json.loads(open(path, 'r', encoding='utf-8').read()) if path else None
    except Exception:
        return None


def load_approved_params(scope_name: str = 'default') -> dict[str, Any]:
    path = _LAB.area(_AREA) / _approved_file(scope_name)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
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
    candidate_count = sum(1 for r in rows if r.get('status') == 'candidate_only_not_live')
    approved_count = sum(1 for r in rows if r.get('status') == 'approved_snapshot_only')
    latest_candidate = load_latest_candidate()
    return {
        'generated_at': now_str(),
        'candidate_count': candidate_count,
        'approved_count': approved_count,
        'latest_candidate_scope': latest_candidate.get('scope_name') if latest_candidate else None,
        'separation_guarantee': 'candidate and approved snapshots are separated from production config',
        'status': 'param_storage_safe_registry_ready',
    }


def save_sector_params(sector_name, best_params):
    return save_candidate_params(scope_name=f'sector::{sector_name}', best_params=best_params, source_module='legacy_save_sector_params')


def load_all_params():
    return {'approved': load_approved_params('default'), 'summary': summary()}
