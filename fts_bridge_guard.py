# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from fts_config import PATHS, CONFIG
from fts_utils import now_str

LEGACY_FACADES = ('screening', 'strategies', 'advanced_chart')
DEFAULT_CORE_MODULES = [
    'live_paper_trading.py',
    'event_backtester.py',
    'advanced_optimizer.py',
    'optimizer.py',
    'fts_model_layer.py',
    'fts_legacy_master_pipeline_impl.py',
]


def audit_bridge_usage(core_modules: Iterable[str] | None = None) -> dict:
    modules = list(core_modules or DEFAULT_CORE_MODULES)
    callers: dict[str, list[str]] = {name: [] for name in LEGACY_FACADES}
    base_dir = Path(PATHS.base_dir)
    for mod in modules:
        path = base_dir / mod
        if not path.exists():
            continue
        text = path.read_text(encoding='utf-8', errors='ignore')
        for facade in LEGACY_FACADES:
            if f'from {facade} import' in text or f'import {facade}' in text:
                callers[facade].append(mod)
    offenders = sorted({m for vals in callers.values() for m in vals})
    payload = {
        'generated_at': now_str(),
        'mode': str(getattr(CONFIG, 'mode', 'PAPER')).upper(),
        'force_service_api_only': bool(getattr(CONFIG, 'force_service_api_only', True)),
        'legacy_facade_callers': callers,
        'offender_count': len(offenders),
        'offenders': offenders,
        'ok': len(offenders) == 0,
        'status': 'service_api_only' if len(offenders) == 0 else 'legacy_facade_imports_detected',
    }
    out = Path(PATHS.runtime_dir) / 'bridge_guard.json'
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload
