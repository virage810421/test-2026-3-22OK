# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, write_json


class ModelVersionRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'model_registry_runtime.json'

    def build(self) -> tuple[Path, dict[str, Any]]:
        models_dir = PATHS.model_dir
        governance_registry = load_json(models_dir / 'model_registry.json', {}) or {}
        candidates = []
        if models_dir.exists():
            for p in sorted(models_dir.glob('*')):
                candidates.append({
                    'name': p.name,
                    'path': str(p),
                    'is_file': p.is_file(),
                    'is_dir': p.is_dir(),
                    'size_bytes': p.stat().st_size if p.exists() and p.is_file() else 0,
                })
        suspicious_small_artifacts = [x for x in candidates if x.get('is_file') and str(x.get('name', '')).endswith('.pkl') and int(x.get('size_bytes', 0)) < 128]
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'models_dir': str(models_dir),
            'candidates': candidates,
            'candidate_count': len(candidates),
            'suspicious_small_artifacts': suspicious_small_artifacts,
            'governance_registry': governance_registry,
        }
        write_json(self.path, payload)
        log(f'🧬 已輸出 model registry：{self.path}')
        return self.path, payload


class ModelSelectionGate:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'model_selection_gate.json'

    def evaluate(self, ai_status: dict[str, Any], readiness: dict[str, Any], governance: dict[str, Any] | None = None) -> tuple[Path, dict[str, Any]]:
        governance = governance or {}
        failures = []
        warnings = []
        if not ai_status.get('all_core_scripts_present', True):
            failures.append({'type': 'ai_core_scripts_missing', 'message': 'AI 訓練核心腳本未齊備'})
        if not ai_status.get('training_assets_present', True):
            warnings.append({'type': 'training_assets_incomplete', 'message': '訓練資料或 models 資產尚未齊備'})
        if readiness.get('total_signals', 0) == 0:
            warnings.append({'type': 'zero_signal', 'message': '目前 decision 輸出為 0 筆有效訊號'})
        if governance.get('go_for_promote') is False:
            failures.append({'type': 'governance_blocked', 'message': '模型治理閘門未放行'})
        suspicious = list((ai_status or {}).get('suspicious_small_artifacts', []) or [])
        if suspicious:
            warnings.append({'type': 'suspicious_small_model_artifacts', 'count': len(suspicious), 'items': suspicious[:10], 'message': '偵測到過小模型檔，請確認不是占位檔或損毀檔'})
        gate = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'go_for_model_linkage': len(failures) == 0,
            'governance': governance,
            'failures': failures,
            'warnings': warnings,
            'summary': {
                'failure_count': len(failures),
                'warning_count': len(warnings),
            },
        }
        write_json(self.path, gate)
        log(f"🧪 Model Gate | go_for_model_linkage={gate['go_for_model_linkage']} | failures={len(failures)} | warnings={len(warnings)}")
        return self.path, gate
