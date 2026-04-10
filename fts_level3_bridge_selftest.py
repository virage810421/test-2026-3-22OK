# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import py_compile
from pathlib import Path

TARGETS = [
    'execution_engine.py',
    'paper_broker.py',
    'risk_gateway.py',
    'live_paper_trading.py',
    'fts_level3_runtime_loader.py',
]

results = {}
for name in TARGETS:
    try:
        py_compile.compile(name, doraise=True)
        results[name] = {'compile_ok': True}
    except Exception as e:
        results[name] = {'compile_ok': False, 'error': repr(e)}

out = Path('runtime') / 'level3_bridge_selftest.json'
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print(out)
