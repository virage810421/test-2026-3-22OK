# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import pickle
import shutil
from pathlib import Path
from typing import Any

from fts_config import PATHS
from fts_utils import now_str
from fts_research_lab import ResearchLab
from param_storage import load_approved_params, summary as param_storage_summary


class ApprovedArtifactLoader:
    MODULE_VERSION = 'v85_approved_artifact_loader'

    def __init__(self):
        self.lab = ResearchLab()
        self.area_name = 'approved_pipeline'
        self.root = self.lab.area(self.area_name)
        self.mount_status_path = PATHS.runtime_dir / 'approved_artifact_mount_status.json'

    def _feature_meta_path(self, scope: str = 'default') -> Path:
        safe = str(scope).replace('/', '_').replace('\\', '_')
        return self.root / f'approved_feature_snapshot_{safe}.json'

    def _feature_pkl_path(self, scope: str = 'default') -> Path:
        safe = str(scope).replace('/', '_').replace('\\', '_')
        return self.root / f'approved_feature_snapshot_{safe}.pkl'

    def capture_selected_features_snapshot(self, source_path: str | Path | None = None, scope: str = 'default', approver: str = 'auto') -> dict[str, Any]:
        src = Path(source_path) if source_path else (PATHS.model_dir / 'selected_features.pkl')
        if not src.exists():
            return {'status': 'source_missing', 'source_path': str(src)}
        dst = self._feature_pkl_path(scope)
        shutil.copy2(src, dst)
        features = self.load_approved_selected_features(scope)
        payload = {
            'generated_at': now_str(),
            'approved_at': now_str(),
            'approved_by': approver,
            'scope': scope,
            'source_path': str(src),
            'approved_feature_snapshot_path': str(dst),
            'feature_count': len(features),
            'status': 'approved_feature_snapshot_ready',
        }
        self._feature_meta_path(scope).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        self.lab.append_registry(self.area_name, {
            'generated_at': payload['generated_at'],
            'scope': scope,
            'artifact_path': str(dst),
            'feature_count': len(features),
            'status': payload['status'],
        })
        return payload

    def load_approved_selected_features(self, scope: str = 'default') -> list[str]:
        path = self._feature_pkl_path(scope)
        if not path.exists():
            return []
        try:
            with path.open('rb') as fh:
                obj = pickle.load(fh)
            if isinstance(obj, (list, tuple)):
                return list(dict.fromkeys([str(x) for x in obj if str(x).strip()]))
        except Exception:
            return []
        return []

    def preferred_selected_features_path(self, use_approved: bool = True, scope: str = 'default') -> Path:
        approved = self._feature_pkl_path(scope)
        if use_approved and approved.exists():
            return approved
        return PATHS.model_dir / 'selected_features.pkl'

    def approved_params_summary(self, scope: str = 'default') -> dict[str, Any]:
        row = load_approved_params(scope)
        return {
            'scope': scope,
            'present': bool(row),
            'param_count': len(row.get('params', {})) if isinstance(row, dict) else 0,
            'approved_at': row.get('approved_at') if isinstance(row, dict) else None,
            'approved_by': row.get('approved_by') if isinstance(row, dict) else None,
        }

    def build_mount_status(self, scope: str = 'default', live_enabled: bool = False, training_enabled: bool = True) -> tuple[Path, dict[str, Any]]:
        feature_meta = {}
        meta_path = self._feature_meta_path(scope)
        if meta_path.exists():
            try:
                feature_meta = json.loads(meta_path.read_text(encoding='utf-8'))
            except Exception:
                feature_meta = {}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'scope': scope,
            'training_mount_enabled': bool(training_enabled),
            'live_mount_enabled': bool(live_enabled),
            'approved_feature_snapshot_present': self._feature_pkl_path(scope).exists(),
            'approved_feature_snapshot_count': len(self.load_approved_selected_features(scope)),
            'approved_feature_snapshot_meta': feature_meta,
            'approved_params_summary': self.approved_params_summary(scope),
            'param_storage_summary': param_storage_summary(),
            'status': 'approved_mount_status_ready',
        }
        self.mount_status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.mount_status_path, payload
