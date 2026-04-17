# -*- coding: utf-8 -*-
"""Live publish manifest builder v4.

Creates a reviewable live-publish manifest for a promoted parameter snapshot.
It does not mount live parameters and does not place orders.  With --apply it
only copies the manifest into state/ as an active reviewed manifest, and this is
still blocked unless PARAM_LIVE_PUBLISH_ALLOW_APPLY=True or --force is supplied.
"""
from __future__ import annotations

import argparse
import hashlib
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

REPORT_PATH = Path('runtime') / 'param_live_publish_manifest.json'
ACTIVE_PATH = Path('state') / 'param_live_publish_manifest_active.json'


def _sha(obj: Any) -> str:
    blob = json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()


def _load_approved(scope: str) -> dict[str, Any]:
    try:
        from param_storage import load_approved_params  # type: ignore
        payload = load_approved_params(scope)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {'_error': repr(exc)}


def build_live_publish_manifest(scope: str, apply: bool = False, force: bool = False) -> dict[str, Any]:
    scope = str(scope or 'strategy_signal::default')
    approved = _load_approved(scope)
    reasons: list[str] = []
    status = str(approved.get('status') or '')
    release = approved.get('release', {}) if isinstance(approved.get('release', {}), dict) else {}
    params = approved.get('params', {}) if isinstance(approved.get('params', {}), dict) else {}

    if not approved:
        reasons.append('approved_snapshot_missing')
    if approved.get('_error'):
        reasons.append('approved_snapshot_unreadable')
    if status != 'promoted_for_live':
        reasons.append(f'approved_status_not_promoted_for_live:{status or "missing"}')
    if not bool(release.get('release_gate_pass', False)):
        reasons.append('release_gate_pass_missing')
    if not bool(release.get('paper_pass', False)):
        reasons.append('paper_pass_missing')
    if not bool(release.get('shadow_pass', False)):
        reasons.append('shadow_pass_missing')
    if not release.get('rollback_to'):
        reasons.append('rollback_to_missing')

    ready = len(reasons) == 0
    manifest = {
        'generated_at': now_str(),
        'scope': scope,
        'version': approved.get('version'),
        'candidate_id': approved.get('candidate_id'),
        'approved_status': status or None,
        'ready_for_live_mount': bool(ready),
        'blocked_reasons': reasons,
        'params_sha256': _sha(params),
        'approved_snapshot_sha256': _sha(approved),
        'release': release,
        'ai_judge': approved.get('ai_judge', {}),
        'safe_publish_contract': {
            'writes_production_config': False,
            'places_orders': False,
            'requires_live_mount_guard': True,
            'requires_explicit_scope_switch': True,
            'requires_FTS_PARAM_MOUNT_STAGE_live': True,
        },
        'rollback_to': release.get('rollback_to'),
        'params_preview': params,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')

    apply_allowed = bool(PARAMS.get('PARAM_LIVE_PUBLISH_ALLOW_APPLY', False)) or bool(force)
    if apply:
        if not ready:
            manifest['apply_status'] = 'blocked_manifest_not_ready'
        elif not apply_allowed:
            manifest['apply_status'] = 'blocked_apply_disabled_by_config'
        else:
            ACTIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
            ACTIVE_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
            manifest['apply_status'] = 'active_manifest_written'
            manifest['active_manifest_path'] = str(ACTIVE_PATH)
            REPORT_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    else:
        manifest['apply_status'] = 'dry_run_manifest_only'
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope', default='strategy_signal::default')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args(argv)
    payload = build_live_publish_manifest(args.scope, apply=args.apply, force=args.force)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get('ready_for_live_mount') else 1


if __name__ == '__main__':
    raise SystemExit(main())
