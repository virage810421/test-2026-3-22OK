# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_config import PATHS
from fts_utils import now_str, log


class ScreeningDetachmentAudit:
    MODULE_VERSION = 'v83_screening_detachment_audit'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'screening_detachment_audit.json'

    def run(self) -> tuple[Path, dict[str, Any]]:
        direct_imports = []
        wrapper_imports = []
        for path in PATHS.base_dir.glob('*.py'):
            try:
                text = path.read_text(encoding='utf-8')
            except Exception:
                continue
            for mod in ['screening', 'strategies', 'advanced_chart', 'master_pipeline', 'ml_data_generator', 'ml_trainer', 'yahoo_csv_to_sql']:
                if f'import {mod}' in text or f'from {mod} import' in text:
                    if path.name == f'{mod}.py':
                        continue
                    wrapper_imports.append(f'{path.name}:{mod}')
            if path.name != 'fts_screening_detachment_audit.py' and '_legacy_screening' in text:
                direct_imports.append(path.name)
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'direct_legacy_fallback_modules': sorted(direct_imports),
            'legacy_facade_import_callers': sorted(wrapper_imports),
            'status': 'legacy_detachment_audited',
            'note': '目標是讓核心主線不再反向 import 任一 legacy facade；legacy 檔只保留 CLI / 外部相容入口。',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧪 screening detachment audited: {self.runtime_path}')
        return self.runtime_path, payload
