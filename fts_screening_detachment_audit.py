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
            if 'import screening' in text or 'from screening import' in text:
                if path.name == 'screening.py':
                    continue
                wrapper_imports.append(path.name)
            if path.name != 'fts_screening_detachment_audit.py' and '_legacy_screening' in text:
                direct_imports.append(path.name)
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'direct_legacy_fallback_modules': sorted(direct_imports),
            'legacy_screening_import_callers': sorted(wrapper_imports),
            'status': 'screening_detachment_audited',
            'note': '目標是讓 service 不再反向 import 舊 screening.py；舊 screening.py 只保留相容 wrapper。',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧪 screening detachment audited: {self.runtime_path}')
        return self.runtime_path, payload
