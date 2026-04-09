# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log
from model_governance import ModelGovernanceManager


class TrainingStressAudit:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'training_stress_audit.json'
        self.backend_path = PATHS.runtime_dir / 'trainer_backend_report.json'

    def build(self) -> tuple[Path, dict[str, Any]]:
        if self.backend_path.exists():
            report = json.loads(self.backend_path.read_text(encoding='utf-8'))
        else:
            report = {}
        integrity = ModelGovernanceManager().evaluate_training_integrity(report or {'leakage_guards': {}, 'out_of_time': {}, 'overfit_gap': 1.0, 'feature_to_sample_ratio': 1.0})
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'trainer_backend_report_exists': self.backend_path.exists(),
            'integrity': integrity,
            'key_findings': {
                'overfit_gap': float(report.get('overfit_gap', 0.0) or 0.0),
                'oot_hit_rate': float(report.get('out_of_time', {}).get('hit_rate', 0.0) or 0.0),
                'oot_profit_factor': float(report.get('out_of_time', {}).get('profit_factor', 0.0) or 0.0),
                'feature_to_sample_ratio': float(report.get('feature_to_sample_ratio', 0.0) or 0.0),
            },
            'status': integrity.get('status', 'training_integrity_blocked'),
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧪 已輸出 training stress audit：{self.path}')
        return self.path, payload
