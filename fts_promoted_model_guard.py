# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config import PARAMS
from fts_config import PATHS, CONFIG
from model_governance import load_registry, get_best_version_entry


REQUIRED_PROMOTED_ARTIFACTS = [
    'selected_features.pkl',
    'model_趨勢多頭.pkl',
    'model_區間盤整.pkl',
    'model_趨勢空頭.pkl',
]


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _is_live_context() -> bool:
    mode = str(getattr(CONFIG, 'mode', 'PAPER') or 'PAPER').upper()
    broker = str(getattr(CONFIG, 'broker_type', 'paper') or 'paper').lower()
    manual = bool(getattr(CONFIG, 'live_manual_arm', False))
    return mode == 'LIVE' or broker in {'real', 'live', 'broker'} or manual


class PromotedModelGuard:
    MODULE_VERSION = 'v95_promoted_model_guard'

    def __init__(self) -> None:
        self.runtime_path = PATHS.runtime_dir / 'promoted_model_guard.json'
        self.backend_path = PATHS.runtime_dir / 'trainer_backend_report.json'
        self.live_gate_path = PATHS.runtime_dir / 'model_live_signal_gate.json'
        self.live_release_path = PATHS.runtime_dir / 'live_release_gate.json'
        self.readiness_path = PATHS.runtime_dir / 'live_readiness_gate.json'

    def build(self) -> tuple[Path, dict[str, Any]]:
        registry = load_registry() or {}
        best_entry = get_best_version_entry() or {}
        backend = _load_json(self.backend_path)
        live_gate = _load_json(self.live_gate_path)
        live_release = _load_json(self.live_release_path)
        readiness = _load_json(self.readiness_path)
        best_dir = PATHS.model_dir / 'best'
        live_context = _is_live_context()
        live_only_use_promoted = bool(PARAMS.get('LIVE_ONLY_USE_PROMOTED_MODEL', True))
        block_live_on_unpromoted = bool(PARAMS.get('MODEL_BLOCK_LIVE_ON_UNPROMOTED', True))
        best_dir_artifacts = []
        for name in REQUIRED_PROMOTED_ARTIFACTS:
            p = best_dir / name
            best_dir_artifacts.append({
                'name': name,
                'path': str(p),
                'exists': p.exists(),
                'size_bytes': p.stat().st_size if p.exists() else 0,
            })
        best_dir_ready = all(x['exists'] and int(x.get('size_bytes', 0)) > 0 for x in best_dir_artifacts)
        allow_live_signal = bool(live_gate.get('allow_live_signal', False))
        promotion_ready = bool(backend.get('promotion_ready', live_gate.get('promotion_ready', False)))
        promoted_best = str((backend.get('promotion') or {}).get('status') or live_gate.get('status') or '') in {'promoted_best', 'live_signal_allowed'}
        best_version = registry.get('best_version') or best_entry.get('version')
        reasons: list[str] = []
        if live_only_use_promoted and not allow_live_signal:
            reasons.append('model_live_signal_gate_blocked')
        if live_only_use_promoted and not promotion_ready:
            reasons.append('promotion_ready_false')
        if live_only_use_promoted and not best_version:
            reasons.append('best_version_missing')
        if live_only_use_promoted and not best_dir_ready:
            reasons.append('best_dir_artifacts_incomplete')
        can_bind_live_promoted_model = bool((not live_only_use_promoted) or (allow_live_signal and promotion_ready and best_version and best_dir_ready))
        desired_artifact_root = best_dir if (live_context and live_only_use_promoted) else PATHS.model_dir
        payload = {
            'generated_at': __import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'module_version': self.MODULE_VERSION,
            'live_context': live_context,
            'live_only_use_promoted_model': live_only_use_promoted,
            'block_live_on_unpromoted': block_live_on_unpromoted,
            'allow_live_signal': allow_live_signal,
            'promotion_ready': promotion_ready,
            'promoted_best_status_seen': promoted_best,
            'best_version': best_version,
            'current_version': registry.get('current_version'),
            'active_deployment': registry.get('active_deployment'),
            'best_entry': best_entry,
            'best_dir': str(best_dir),
            'best_dir_artifacts': best_dir_artifacts,
            'best_dir_ready': best_dir_ready,
            'desired_artifact_root': str(desired_artifact_root),
            'can_bind_live_promoted_model': can_bind_live_promoted_model,
            'live_release_allow_live': bool(live_release.get('allow_live', False)),
            'live_readiness_live_ready': bool(readiness.get('live_ready', False)),
            'reasons': reasons,
            'status': 'promoted_model_ready' if can_bind_live_promoted_model else 'promoted_model_blocked',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.runtime_path, payload


def read_promoted_model_guard(force_refresh: bool = False) -> dict[str, Any]:
    path = PATHS.runtime_dir / 'promoted_model_guard.json'
    if force_refresh or (not path.exists()):
        _, payload = PromotedModelGuard().build()
        return payload
    return _load_json(path)


def resolve_runtime_model_dir(force_refresh: bool = False) -> Path:
    payload = read_promoted_model_guard(force_refresh=force_refresh)
    target = str(payload.get('desired_artifact_root') or str(PATHS.model_dir))
    try:
        return Path(target)
    except Exception:
        return PATHS.model_dir
