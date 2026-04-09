# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, write_json, append_jsonl

MODELS_DIR = Path(getattr(PATHS, 'model_dir', Path('models')))
VERSIONS_DIR = MODELS_DIR / 'versions'
CURRENT_DIR = MODELS_DIR / 'current'
BEST_DIR = MODELS_DIR / 'best'
REGISTRY_PATH = MODELS_DIR / 'model_registry.json'
EVENTS_PATH = MODELS_DIR / 'governance_events.jsonl'
DEPLOYMENT_STATUS_PATH = MODELS_DIR / 'deployment_status.json'
DEFAULT_TRACKED_FILES = [
    'selected_features.pkl',
    'model_趨勢多頭.pkl',
    'model_區間盤整.pkl',
    'model_趨勢空頭.pkl',
]


def ensure_dirs() -> None:
    for d in [MODELS_DIR, VERSIONS_DIR, CURRENT_DIR, BEST_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def load_registry() -> dict[str, Any]:
    ensure_dirs()
    default = {
        'versions': [],
        'current_version': None,
        'best_version': None,
        'last_candidate_version': None,
        'last_shadow_version': None,
        'active_deployment': None,
    }
    data = load_json(REGISTRY_PATH, default)
    if not isinstance(data, dict):
        return default
    for k, v in default.items():
        data.setdefault(k, v)
    return data


def save_registry(registry: dict[str, Any]) -> None:
    write_json(REGISTRY_PATH, registry)


def create_version_tag(prefix: str = 'model') -> str:
    return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _copy_if_exists(src: Path, dst: Path) -> bool:
    if not src.exists():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def _artifact_hash(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _tracked_payload(version_dir: Path, tracked_files: list[str]) -> list[dict[str, Any]]:
    rows = []
    for name in tracked_files:
        p = version_dir / name
        rows.append({
            'name': name,
            'exists': p.exists(),
            'size_bytes': p.stat().st_size if p.exists() else 0,
            'sha256': _artifact_hash(p) if p.exists() else None,
        })
    return rows


def snapshot_current_models(version_tag: str, metrics: dict[str, Any] | None = None, note: str = '', tracked_files: list[str] | None = None) -> dict[str, Any]:
    ensure_dirs()
    tracked = tracked_files or DEFAULT_TRACKED_FILES
    version_dir = VERSIONS_DIR / version_tag
    version_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for filename in tracked:
        src = MODELS_DIR / filename
        dst = version_dir / filename
        if _copy_if_exists(src, dst):
            copied.append(filename)
    registry = load_registry()
    entry = {
        'version': version_tag,
        'timestamp': now_str(),
        'files': copied,
        'artifacts': _tracked_payload(version_dir, tracked),
        'metrics': metrics or {},
        'note': note,
        'status': 'snapshot',
    }
    registry['versions'] = [x for x in registry.get('versions', []) if x.get('version') != version_tag]
    registry['versions'].append(entry)
    registry['current_version'] = version_tag
    save_registry(registry)
    for filename in tracked:
        _copy_if_exists(version_dir / filename, CURRENT_DIR / filename)
    append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'snapshot', 'version': version_tag, 'note': note, 'metrics': metrics or {}})
    return entry


def promote_best_version(version_tag: str) -> dict[str, Any]:
    ensure_dirs()
    registry = load_registry()
    registry['best_version'] = version_tag
    save_registry(registry)
    version_dir = VERSIONS_DIR / version_tag
    for filename in DEFAULT_TRACKED_FILES:
        _copy_if_exists(version_dir / filename, BEST_DIR / filename)
    payload = {'time': now_str(), 'event': 'promote_best', 'version': version_tag}
    append_jsonl(EVENTS_PATH, payload)
    return payload


def restore_version(version_tag: str) -> dict[str, Any]:
    ensure_dirs()
    version_dir = VERSIONS_DIR / version_tag
    restored = []
    for filename in DEFAULT_TRACKED_FILES:
        if _copy_if_exists(version_dir / filename, MODELS_DIR / filename):
            restored.append(filename)
            _copy_if_exists(version_dir / filename, CURRENT_DIR / filename)
    registry = load_registry()
    registry['current_version'] = version_tag
    save_registry(registry)
    payload = {'time': now_str(), 'event': 'restore', 'version': version_tag, 'restored': restored}
    append_jsonl(EVENTS_PATH, payload)
    return payload


def get_best_version_entry() -> dict[str, Any] | None:
    registry = load_registry()
    best_version = registry.get('best_version')
    if not best_version:
        return None
    for row in registry.get('versions', []):
        if row.get('version') == best_version:
            return row
    return None



class ModelGovernanceManager:
    """Safe promotion / rollback / shadow management for model lifecycle."""

    def __init__(self):
        ensure_dirs()
        self.runtime_path = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'model_governance_status.json'

    def _collect_artifact_status(self, tracked_files: list[str] | None = None) -> dict[str, Any]:
        tracked = tracked_files or DEFAULT_TRACKED_FILES
        rows = []
        for name in tracked:
            p = MODELS_DIR / name
            rows.append({
                'name': name,
                'exists': p.exists(),
                'size_bytes': p.stat().st_size if p.exists() else 0,
                'sha256': _artifact_hash(p) if p.exists() else None,
            })
        return {
            'tracked_files': tracked,
            'all_present': all(r['exists'] for r in rows),
            'files': rows,
        }

    def register_candidate(
        self,
        version_tag: str | None = None,
        metrics: dict[str, Any] | None = None,
        walk_forward: dict[str, Any] | None = None,
        shadow_result: dict[str, Any] | None = None,
        feature_schema: dict[str, Any] | None = None,
        note: str = '',
    ) -> dict[str, Any]:
        version_tag = version_tag or create_version_tag('candidate')
        entry = snapshot_current_models(version_tag, metrics=metrics, note=note)
        registry = load_registry()
        registry['last_candidate_version'] = version_tag
        registry['active_deployment'] = registry.get('current_version')
        save_registry(registry)
        payload = {
            'time': now_str(),
            'event': 'register_candidate',
            'version': version_tag,
            'metrics': metrics or {},
            'walk_forward': walk_forward or {},
            'shadow_result': shadow_result or {},
            'feature_schema': feature_schema or {},
            'note': note,
        }
        append_jsonl(EVENTS_PATH, payload)
        entry.update(payload)
        return entry

    def evaluate_candidate(
        self,
        metrics: dict[str, Any] | None = None,
        walk_forward: dict[str, Any] | None = None,
        shadow_result: dict[str, Any] | None = None,
        thresholds: dict[str, Any] | None = None,
        rollback_version: str | None = None,
    ) -> dict[str, Any]:
        metrics = metrics or {}
        walk_forward = walk_forward or {}
        shadow_result = shadow_result or {}
        thresholds = thresholds or {
            'min_win_rate': 0.50,
            'min_profit_factor': 1.10,
            'max_drawdown_pct': 0.12,
            'min_walk_forward_score': 60,
            'max_shadow_return_drift_pct': 0.08,
        }
        failures = []
        warnings = []

        if metrics.get('win_rate', 0) < thresholds['min_win_rate']:
            failures.append('win_rate_below_threshold')
        if metrics.get('profit_factor', 0) < thresholds['min_profit_factor']:
            failures.append('profit_factor_below_threshold')
        if metrics.get('max_drawdown_pct', 1) > thresholds['max_drawdown_pct']:
            failures.append('drawdown_above_threshold')
        if walk_forward.get('score', 0) < thresholds['min_walk_forward_score']:
            failures.append('walk_forward_not_strong_enough')
        if shadow_result.get('return_drift_pct', 0) > thresholds['max_shadow_return_drift_pct']:
            warnings.append('shadow_drift_above_preferred_band')
        if not rollback_version:
            failures.append('rollback_version_missing')

        artifacts = self._collect_artifact_status()
        if not artifacts['all_present']:
            failures.append('artifacts_incomplete')

        decision = {
            'generated_at': now_str(),
            'system_name': getattr(CONFIG, 'system_name', 'FTS'),
            'thresholds': thresholds,
            'metrics': metrics,
            'walk_forward': walk_forward,
            'shadow_result': shadow_result,
            'artifacts': artifacts,
            'rollback_version': rollback_version,
            'go_for_shadow': len([x for x in failures if x != 'rollback_version_missing']) == 0,
            'go_for_promote': len(failures) == 0,
            'failures': failures,
            'warnings': warnings,
            'status': 'candidate_ready' if len(failures) == 0 else 'candidate_blocked',
        }
        write_json(self.runtime_path, decision)
        append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'evaluate_candidate', 'decision': decision['status'], 'failures': failures, 'warnings': warnings})
        return decision

    def mark_shadow_result(self, version_tag: str, result: dict[str, Any]) -> dict[str, Any]:
        registry = load_registry()
        registry['last_shadow_version'] = version_tag
        save_registry(registry)
        payload = {'time': now_str(), 'event': 'shadow_result', 'version': version_tag, 'result': result}
        append_jsonl(EVENTS_PATH, payload)
        return payload

    def promote_to_current(self, version_tag: str, operator: str = 'system', note: str = '') -> dict[str, Any]:
        restored = restore_version(version_tag)
        registry = load_registry()
        registry['current_version'] = version_tag
        registry['active_deployment'] = version_tag
        save_registry(registry)
        deployment = {
            'promoted_at': now_str(),
            'version': version_tag,
            'operator': operator,
            'note': note,
            'restored': restored.get('restored', []),
            'status': 'live_candidate_promoted',
        }
        write_json(DEPLOYMENT_STATUS_PATH, deployment)
        append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'promote_to_current', 'version': version_tag, 'operator': operator, 'note': note})
        return deployment

    def rollback(self, version_tag: str | None = None, reason: str = '') -> dict[str, Any]:
        registry = load_registry()
        target = version_tag or registry.get('best_version') or registry.get('current_version')
        restored = restore_version(target)
        deployment = {
            'rolled_back_at': now_str(),
            'target_version': target,
            'reason': reason,
            'status': 'rolled_back',
            'restored': restored.get('restored', []),
        }
        write_json(DEPLOYMENT_STATUS_PATH, deployment)
        append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'rollback', 'target_version': target, 'reason': reason})
        return deployment

    def evaluate_live_health(self, live_metrics: dict[str, Any], thresholds: dict[str, Any] | None = None) -> dict[str, Any]:
        thresholds = thresholds or {
            'min_live_win_rate': 0.42,
            'max_consecutive_losses': 5,
            'max_reject_rate': 0.20,
            'max_fill_slippage_bps': 35,
        }
        triggers = []
        if live_metrics.get('win_rate', 1.0) < thresholds['min_live_win_rate']:
            triggers.append('live_win_rate_break')
        if live_metrics.get('consecutive_losses', 0) > thresholds['max_consecutive_losses']:
            triggers.append('consecutive_losses_break')
        if live_metrics.get('reject_rate', 0.0) > thresholds['max_reject_rate']:
            triggers.append('reject_rate_break')
        if live_metrics.get('avg_slippage_bps', 0.0) > thresholds['max_fill_slippage_bps']:
            triggers.append('slippage_break')
        payload = {
            'generated_at': now_str(),
            'thresholds': thresholds,
            'live_metrics': live_metrics,
            'rollback_recommended': len(triggers) > 0,
            'triggers': triggers,
            'status': 'rollback_recommended' if triggers else 'live_health_ok',
        }
        append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'live_health_eval', 'status': payload['status'], 'triggers': triggers})
        return payload
