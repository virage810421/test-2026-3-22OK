# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_config import PATHS
from fts_utils import log, now_str

LEGACY_FACADE_MODULES = [
    'advanced_chart',
    'screening',
    'strategies',
    'master_pipeline',
    'ml_data_generator',
    'ml_trainer',
    'yahoo_csv_to_sql',
]

CORE_MODULES = [
    'live_paper_trading.py',
    'event_backtester.py',
    'advanced_optimizer.py',
    'optimizer.py',
    'fts_model_layer.py',
    'fts_etl_daily_chip_service.py',
    'fts_legacy_master_pipeline_impl.py',
]


class LegacyDetachGuard:
    MODULE_VERSION = 'v20260413_legacy_detach_guard'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'legacy_detach_guard.json'

    def run(self) -> tuple[Path, dict[str, Any]]:
        violations: dict[str, list[str]] = {}
        for name in CORE_MODULES:
            path = PATHS.base_dir / name
            if not path.exists():
                continue
            try:
                text = path.read_text(encoding='utf-8')
            except Exception:
                continue
            bad = []
            for mod in LEGACY_FACADE_MODULES:
                if f'from {mod} import' in text or f'import {mod}' in text:
                    bad.append(mod)
            if bad:
                violations[name] = sorted(set(bad))
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'legacy_facade_modules': LEGACY_FACADE_MODULES,
            'core_modules': CORE_MODULES,
            'violations': violations,
            'status': 'detached' if not violations else 'violations_found',
            'note': '核心主線應只走 fts_service_api / 正式 service layer，不應反向 import legacy facade。',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧱 legacy detach guard：{self.runtime_path}')
        return self.runtime_path, payload


if __name__ == '__main__':
    LegacyDetachGuard().run()
