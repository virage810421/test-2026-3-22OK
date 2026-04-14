# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config import PARAMS
from fts_config import PATHS
from fts_utils import now_str, log
from fts_research_lab import ResearchLab
from fts_approved_artifact_loader import ApprovedArtifactLoader
from param_storage import approve_latest_candidate, load_latest_candidate, load_approved_params
from fts_research_suite import auto_approve_latest_alpha_candidate
from model_governance import load_registry, get_best_version_entry


class ApprovedPipeline:
    MODULE_VERSION = 'v85_approved_pipeline'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'approved_pipeline_status.json'
        self.lab = ResearchLab()
        self.loader = ApprovedArtifactLoader()

    def _load_backend_report(self) -> dict[str, Any]:
        path = PATHS.runtime_dir / 'trainer_backend_report.json'
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except Exception:
                return {}
        return {}

    def _auto_approve_default_params(self) -> dict[str, Any]:
        latest = load_latest_candidate('default')
        if not latest:
            return {'status': 'no_default_candidate'}
        metrics = latest.get('metrics', {}) or {}
        score = float(metrics.get('Score', metrics.get('Composite', 0.0)) or 0.0)
        ev = float(metrics.get('Test_EV', 0.0) or 0.0)
        if score < 0.0 and ev <= 0:
            return {'status': 'candidate_below_floor', 'candidate_id': latest.get('candidate_id')}
        approved = approve_latest_candidate('default', approver='approved_pipeline_auto', note='approved pipeline auto-approve default candidate')
        return {'status': 'approved', 'candidate_id': latest.get('candidate_id'), 'approved_scope': approved.get('scope_name') if approved else 'default'}

    def run(self, auto_capture_features: bool = True, auto_approve_params: bool = True, auto_approve_alpha: bool = True) -> tuple[Path, dict[str, Any]]:
        scope = str(PARAMS.get('APPROVED_DEFAULT_SCOPE', 'default'))
        backend = self._load_backend_report()
        feature_stage = {'status': 'disabled'}
        if auto_capture_features and bool(PARAMS.get('APPROVED_AUTO_CAPTURE_SELECTED_FEATURES', True)):
            feature_stage = self.loader.capture_selected_features_snapshot(scope=scope, approver='approved_pipeline_auto')
        param_stage = {'status': 'disabled'}
        if auto_approve_params:
            param_stage = self._auto_approve_default_params()
        alpha_stage = {'status': 'disabled'}
        if auto_approve_alpha and bool(PARAMS.get('APPROVED_ALPHA_AUTO_PROMOTION', True)):
            alpha_stage = auto_approve_latest_alpha_candidate(approver='approved_pipeline_auto') or {'status': 'no_alpha_candidate'}
        mount_path, mount_status = self.loader.build_mount_status(
            scope=scope,
            live_enabled=bool(PARAMS.get('APPROVED_PARAMS_USE_IN_LIVE', False) or PARAMS.get('APPROVED_FEATURE_SNAPSHOT_USE_IN_LIVE', True)),
            training_enabled=bool(PARAMS.get('APPROVED_PARAMS_USE_IN_TRAINING', True) or PARAMS.get('APPROVED_FEATURE_SNAPSHOT_USE_IN_TRAINING', True)),
        )
        registry = load_registry()
        best_entry = get_best_version_entry() or {}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'scope': scope,
            'backend_status': backend.get('status'),
            'backend_promotion': backend.get('promotion', {}),
            'feature_snapshot_stage': feature_stage,
            'param_approval_stage': param_stage,
            'alpha_approval_stage': alpha_stage,
            'approved_params_present': bool(load_approved_params(scope)),
            'best_model_version': best_entry.get('version'),
            'model_registry_best_version': registry.get('best_version'),
            'model_registry_current_version': registry.get('current_version'),
            'mount_status_path': str(mount_path),
            'mount_status': mount_status,
            'status': 'approved_pipeline_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'✅ approved pipeline ready: {self.runtime_path}')
        return self.runtime_path, payload
