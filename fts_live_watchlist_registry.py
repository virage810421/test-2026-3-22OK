# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

from fts_utils import now_str
from fts_research_lab import ResearchLab

_LAB = ResearchLab()
_AREA = 'live_watchlist_registry'


def _candidate_file(candidate_id: str) -> str:
    return f'candidate_live_watchlist_{candidate_id}.json'


def _approved_file() -> str:
    return 'approved_live_watchlist.json'


def _approved_version_file(candidate_id: str) -> str:
    return f'approved_live_watchlist_{candidate_id}.json'


def save_candidate(payload: dict[str, Any], source_module: str = 'fts_live_watchlist_promoter') -> dict[str, Any]:
    candidate_id = now_str().replace(':', '').replace('-', '').replace('T', '_').replace(' ', '_')
    body = dict(payload or {})
    body.update({
        'candidate_id': candidate_id,
        'source_module': source_module,
        'generated_at': body.get('generated_at') or now_str(),
        'status': 'candidate_live_watchlist_ready',
        'writes_live_directly': False,
    })
    artifact = _LAB.write_json_artifact(_AREA, _candidate_file(candidate_id), body)
    _LAB.append_registry(_AREA, {
        'generated_at': body['generated_at'],
        'candidate_id': candidate_id,
        'status': body['status'],
        'artifact_path': str(artifact),
        'source_module': source_module,
        'ticker_count': len(body.get('rows', [])),
    })
    return body


def load_latest_candidate() -> dict[str, Any] | None:
    rows = [r for r in _LAB.load_registry(_AREA) if r.get('status') == 'candidate_live_watchlist_ready']
    if not rows:
        return None
    path = rows[-1].get('artifact_path')
    try:
        return json.loads(open(path, 'r', encoding='utf-8').read()) if path else None
    except Exception:
        return None


def approve_candidate(candidate_id: str, approver: str = 'auto', note: str = '') -> dict[str, Any]:
    candidate_path = _LAB.area(_AREA) / _candidate_file(candidate_id)
    if not candidate_path.exists():
        raise FileNotFoundError(f'candidate not found: {candidate_id}')
    payload = json.loads(candidate_path.read_text(encoding='utf-8'))
    approved = {
        'approved_at': now_str(),
        'approved_by': approver,
        'approval_note': note,
        'candidate_id': candidate_id,
        'rows': payload.get('approved_rows', payload.get('rows', [])),
        'candidate_summary': payload.get('candidate_summary', {}),
        'status': 'approved_live_watchlist_ready',
        'live_effect': 'mountable_if_loader_enabled',
    }
    current_artifact = _LAB.write_json_artifact(_AREA, _approved_file(), approved)
    versioned_artifact = _LAB.write_json_artifact(_AREA, _approved_version_file(candidate_id), approved)
    _LAB.append_registry(_AREA, {
        'generated_at': approved['approved_at'],
        'candidate_id': candidate_id,
        'status': approved['status'],
        'artifact_path': str(current_artifact),
        'versioned_artifact_path': str(versioned_artifact),
        'approved_by': approver,
        'ticker_count': len(approved.get('rows', [])),
    })
    return approved


def approve_latest_candidate(approver: str = 'auto', note: str = '') -> dict[str, Any] | None:
    latest = load_latest_candidate()
    if not latest:
        return None
    return approve_candidate(str(latest.get('candidate_id')), approver=approver, note=note)


def load_approved() -> dict[str, Any]:
    path = _LAB.area(_AREA) / _approved_file()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def rollback_to_previous_approved() -> dict[str, Any]:
    rows = [r for r in _LAB.load_registry(_AREA) if r.get('status') == 'approved_live_watchlist_ready']
    if len(rows) < 2:
        return {'status': 'no_previous_approved_version'}
    prev = rows[-2]
    src = prev.get('versioned_artifact_path') or prev.get('artifact_path')
    if not src:
        return {'status': 'rollback_source_missing'}
    payload = json.loads(open(src, 'r', encoding='utf-8').read())
    _LAB.write_json_artifact(_AREA, _approved_file(), payload)
    _LAB.append_registry(_AREA, {
        'generated_at': now_str(),
        'candidate_id': payload.get('candidate_id'),
        'status': 'approved_live_watchlist_rollback',
        'artifact_path': str(_LAB.area(_AREA) / _approved_file()),
        'rollback_target': str(src),
        'ticker_count': len(payload.get('rows', [])),
    })
    return payload


def summary() -> dict[str, Any]:
    rows = _LAB.load_registry(_AREA)
    approved = load_approved()
    return {
        'generated_at': now_str(),
        'candidate_count': sum(1 for r in rows if r.get('status') == 'candidate_live_watchlist_ready'),
        'approved_count': sum(1 for r in rows if r.get('status') == 'approved_live_watchlist_ready'),
        'current_approved_ticker_count': len(approved.get('rows', [])) if isinstance(approved, dict) else 0,
        'status': 'live_watchlist_registry_ready',
    }
