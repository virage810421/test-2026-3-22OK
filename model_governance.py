# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, load_json, write_json, append_jsonl

MODELS_DIR = Path(getattr(PATHS, 'model_dir', Path('models')))
VERSIONS_DIR = MODELS_DIR / 'versions'
CURRENT_DIR = MODELS_DIR / 'current'
BEST_DIR = MODELS_DIR / 'best'
REGISTRY_PATH = MODELS_DIR / 'model_registry.json'
EVENTS_PATH = MODELS_DIR / 'governance_events.jsonl'
DEPLOYMENT_STATUS_PATH = MODELS_DIR / 'deployment_status.json'
DEFAULT_TRACKED_FILES = [
    'selected_features.pkl',
    'selected_features_long.pkl',
    'selected_features_short.pkl',
    'selected_features_range.pkl',
    'selected_features_exit.pkl',
    'model_趨勢多頭.pkl',
    'model_區間盤整.pkl',
    'model_趨勢空頭.pkl',
    'exit_model_defend.pkl',
    'exit_model_reduce.pkl',
    'exit_model_confirm.pkl',
    'model_long_*.pkl',
    'model_short_*.pkl',
    'model_range_*.pkl',
]
VALIDATED_MANIFEST_NAME = 'validated_artifacts.json'
VALIDATED_MANIFEST_NAME = 'validated_artifacts.json'


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


def _expand_tracked_files(base_dir: Path, tracked_files: list[str] | None = None) -> list[str]:
    resolved: list[str] = []
    for pattern in list(tracked_files or DEFAULT_TRACKED_FILES):
        s = str(pattern).strip()
        if not s:
            continue
        if any(ch in s for ch in '*?['):
            for p in sorted(base_dir.glob(s)):
                if p.is_file() and p.name not in resolved:
                    resolved.append(p.name)
        else:
            if (base_dir / s).exists() and (base_dir / s).is_file() and s not in resolved:
                resolved.append(s)
    return resolved


def _validated_manifest_path(version_dir: Path) -> Path:
    return version_dir / VALIDATED_MANIFEST_NAME


def _read_validated_manifest(version_dir: Path) -> dict[str, Any]:
    return load_json(_validated_manifest_path(version_dir), {})


def _manifest_allowed_files(version_dir: Path, manifest: dict[str, Any] | None = None) -> list[str]:
    payload = manifest or _read_validated_manifest(version_dir)
    allowed: list[str] = []
    if not isinstance(payload, dict):
        return allowed
    for row in payload.get('artifacts', []) or []:
        if not isinstance(row, dict):
            continue
        name = str(row.get('name', '')).strip()
        if not name:
            continue
        if bool(row.get('allow_promote', False)) and (version_dir / name).exists() and name not in allowed:
            allowed.append(name)
    return allowed


def write_validated_artifacts_manifest(version_tag: str, artifacts: list[dict[str, Any]], metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    ensure_dirs()
    version_dir = VERSIONS_DIR / version_tag
    version_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for row in artifacts or []:
        if not isinstance(row, dict):
            continue
        name = str(row.get('name', '')).strip()
        if not name:
            continue
        exists = (version_dir / name).exists()
        item = dict(row)
        item['name'] = name
        item['exists'] = bool(exists)
        item['allow_promote'] = bool(item.get('allow_promote', False) and exists)
        rows.append(item)
    payload = {
        'generated_at': now_str(),
        'version': version_tag,
        'strict_whitelist': True,
        'artifacts': rows,
        'allow_promote_files': [r['name'] for r in rows if r.get('allow_promote')],
        'metadata': metadata or {},
        'status': 'validated_artifacts_manifest_ready',
    }
    write_json(_validated_manifest_path(version_dir), payload)
    append_jsonl(EVENTS_PATH, {
        'time': now_str(),
        'event': 'write_validated_manifest',
        'version': version_tag,
        'allow_promote_count': len(payload['allow_promote_files']),
    })
    return payload


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
    resolved_tracked = _expand_tracked_files(MODELS_DIR, tracked)
    resolved_tracked = _expand_tracked_files(MODELS_DIR, tracked)
    version_dir = VERSIONS_DIR / version_tag
    version_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for filename in resolved_tracked:
        src = MODELS_DIR / filename
        dst = version_dir / filename
        if _copy_if_exists(src, dst):
            copied.append(filename)
    registry = load_registry()
    entry = {
        'version': version_tag,
        'timestamp': now_str(),
        'files': copied,
        'tracked_files_requested': list(tracked),
        'tracked_files_resolved': list(resolved_tracked),
        'artifacts': _tracked_payload(version_dir, resolved_tracked),
        'metrics': metrics or {},
        'note': note,
        'status': 'snapshot',
    }
    registry['versions'] = [x for x in registry.get('versions', []) if x.get('version') != version_tag]
    registry['versions'].append(entry)
    registry['current_version'] = version_tag
    save_registry(registry)
    for filename in resolved_tracked:
        _copy_if_exists(version_dir / filename, CURRENT_DIR / filename)
    append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'snapshot', 'version': version_tag, 'note': note, 'metrics': metrics or {}})
    return entry


def promote_best_version(version_tag: str) -> dict[str, Any]:
    ensure_dirs()
    registry = load_registry()
    registry['best_version'] = version_tag
    save_registry(registry)
    version_dir = VERSIONS_DIR / version_tag
    manifest = _read_validated_manifest(version_dir)
    manifest_files = _manifest_allowed_files(version_dir, manifest)
    files_to_copy = manifest_files or _expand_tracked_files(version_dir, DEFAULT_TRACKED_FILES)
    copied = []
    for filename in files_to_copy:
        if _copy_if_exists(version_dir / filename, BEST_DIR / filename):
            copied.append(filename)
    payload = {
        'time': now_str(),
        'event': 'promote_best',
        'version': version_tag,
        'source': 'validated_manifest' if manifest_files else 'fallback_tracked_files',
        'validated_manifest_present': bool(manifest),
        'copied': copied,
    }
    append_jsonl(EVENTS_PATH, payload)
    return payload


def restore_version(version_tag: str) -> dict[str, Any]:
    ensure_dirs()
    version_dir = VERSIONS_DIR / version_tag
    manifest = _read_validated_manifest(version_dir)
    manifest_files = _manifest_allowed_files(version_dir, manifest)
    files_to_restore = manifest_files or _expand_tracked_files(version_dir, DEFAULT_TRACKED_FILES)
    restored = []
    for filename in files_to_restore:
        if _copy_if_exists(version_dir / filename, MODELS_DIR / filename):
            restored.append(filename)
            _copy_if_exists(version_dir / filename, CURRENT_DIR / filename)
    registry = load_registry()
    registry['current_version'] = version_tag
    save_registry(registry)
    payload = {
        'time': now_str(),
        'event': 'restore',
        'version': version_tag,
        'restored': restored,
        'source': 'validated_manifest' if manifest_files else 'fallback_tracked_files',
        'validated_manifest_present': bool(manifest),
    }
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
    def __init__(self):
        ensure_dirs()
        self.runtime_path = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'model_governance_status.json'

    def _collect_artifact_status(self, tracked_files: list[str] | None = None) -> dict[str, Any]:
        tracked = tracked_files or DEFAULT_TRACKED_FILES
        resolved = _expand_tracked_files(MODELS_DIR, tracked)
        rows = []
        for name in resolved:
            p = MODELS_DIR / name
            rows.append({
                'name': name,
                'exists': p.exists(),
                'size_bytes': p.stat().st_size if p.exists() else 0,
                'sha256': _artifact_hash(p) if p.exists() else None,
            })
        return {'tracked_files': tracked, 'resolved_files': resolved, 'all_present': all(r['exists'] for r in rows), 'files': rows}

    def register_candidate(self, version_tag: str | None = None, metrics: dict[str, Any] | None = None, note: str = '') -> dict[str, Any]:
        version_tag = version_tag or create_version_tag('candidate')
        entry = snapshot_current_models(version_tag, metrics=metrics, note=note)
        registry = load_registry()
        registry['last_candidate_version'] = version_tag
        registry['active_deployment'] = registry.get('current_version')
        save_registry(registry)
        append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'register_candidate', 'version': version_tag, 'metrics': metrics or {}, 'note': note})
        return entry

    def evaluate_candidate(self, metrics: dict[str, Any] | None = None, walk_forward: dict[str, Any] | None = None,
                           shadow_result: dict[str, Any] | None = None, thresholds: dict[str, Any] | None = None,
                           rollback_version: str | None = None) -> dict[str, Any]:
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
            'failures': failures,
            'warnings': warnings,
            'rollback_version': rollback_version,
            'candidate_ready': len(failures) == 0,
            'status': 'candidate_ready' if len(failures) == 0 else 'candidate_blocked',
        }
        write_json(self.runtime_path, decision)
        append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'evaluate_candidate', 'status': decision['status'], 'failures': failures, 'warnings': warnings})
        return decision

    def evaluate_live_health(self, live_metrics: dict[str, Any], thresholds: dict[str, Any] | None = None) -> dict[str, Any]:
        thresholds = thresholds or {
            'min_live_win_rate': 0.42,
            'max_consecutive_losses': 5,
            'max_reject_rate': 0.20,
            'max_fill_slippage_bps': 35,
            'min_trades_required': 20,
        }
        triggers = []
        warnings = []
        trade_count = int(live_metrics.get('trade_count', live_metrics.get('num_trades', 0)) or 0)
        enough_sample = trade_count >= int(thresholds.get('min_trades_required', 20))
        if enough_sample:
            if live_metrics.get('win_rate', 1.0) < thresholds['min_live_win_rate']:
                triggers.append('live_win_rate_break')
            if live_metrics.get('reject_rate', 0.0) > thresholds['max_reject_rate']:
                triggers.append('reject_rate_break')
        else:
            warnings.append('sample_too_small_for_winrate_rejectrate_guard')
        if live_metrics.get('consecutive_losses', 0) > thresholds['max_consecutive_losses']:
            triggers.append('consecutive_losses_break')
        if live_metrics.get('avg_slippage_bps', 0.0) > thresholds['max_fill_slippage_bps']:
            triggers.append('slippage_break')
        payload = {
            'generated_at': now_str(),
            'thresholds': thresholds,
            'live_metrics': live_metrics,
            'trade_count': trade_count,
            'sample_gate_passed': enough_sample,
            'warnings': warnings,
            'rollback_recommended': len(triggers) > 0,
            'triggers': triggers,
            'status': 'rollback_recommended' if triggers else 'live_health_ok',
        }
        append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'live_health_eval', 'status': payload['status'], 'triggers': triggers, 'warnings': warnings, 'sample_gate_passed': enough_sample, 'trade_count': trade_count})
        return payload

    def evaluate_training_integrity(self, training_report: dict[str, Any], thresholds: dict[str, Any] | None = None) -> dict[str, Any]:
        thresholds = thresholds or {
            'min_oot_hit_rate': 0.50,
            'min_oot_profit_factor': 1.00,
            'max_overfit_gap': 0.15,
            'max_feature_to_sample_ratio': 0.35,
        }
        failures = []
        warnings = []
        leakage_guards = training_report.get('leakage_guards', {}) or {}
        if not leakage_guards.get('feature_selection_train_only', False):
            failures.append('feature_selection_not_train_only')
        if not leakage_guards.get('purged_walk_forward', False):
            failures.append('purged_walk_forward_missing')
        if not leakage_guards.get('out_of_time_holdout', False):
            failures.append('out_of_time_holdout_missing')
        oot = training_report.get('out_of_time', {}) or {}
        if float(oot.get('hit_rate', 0.0) or 0.0) < thresholds['min_oot_hit_rate']:
            warnings.append('oot_hit_rate_soft')
        if float(oot.get('profit_factor', 0.0) or 0.0) < thresholds['min_oot_profit_factor']:
            warnings.append('oot_profit_factor_soft')
        overfit_gap = float(training_report.get('overfit_gap', 0.0) or 0.0)
        if overfit_gap > thresholds['max_overfit_gap']:
            failures.append('overfit_gap_too_large')
        ratio = float(training_report.get('feature_to_sample_ratio', 0.0) or 0.0)
        if ratio > thresholds['max_feature_to_sample_ratio']:
            warnings.append('feature_to_sample_ratio_high')
        payload = {
            'generated_at': now_str(),
            'thresholds': thresholds,
            'training_report': training_report,
            'failures': failures,
            'warnings': warnings,
            'promotion_ready': len(failures) == 0,
            'status': 'training_integrity_ok' if len(failures) == 0 else 'training_integrity_blocked',
        }
        append_jsonl(EVENTS_PATH, {'time': now_str(), 'event': 'training_integrity_eval', 'status': payload['status'], 'failures': failures, 'warnings': warnings})
        return payload
