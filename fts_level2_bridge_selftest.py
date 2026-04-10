# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import py_compile
from pathlib import Path
from fts_level2_variant_loader import available_variants

results = {}
for name in available_variants():
    path = Path('advanced_chart1_runtime_variants') / f'{name}.py'
    try:
        py_compile.compile(str(path), doraise=True)
        results[name] = {'compile_ok': True}
    except Exception as e:
        results[name] = {'compile_ok': False, 'error': repr(e)}

out = Path('runtime') / 'level2_bridge_selftest.json'
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print(out)
