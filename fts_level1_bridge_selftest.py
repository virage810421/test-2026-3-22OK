from __future__ import annotations

import importlib
import json
from pathlib import Path

MODULES = ['advanced_chart', 'screening', 'ml_data_generator', 'ml_trainer']
results = {}
for name in MODULES:
    try:
        mod = importlib.import_module(name)
        results[name] = {
            'import_ok': True,
            'facade_mode': getattr(mod, 'FACADE_MODE', getattr(mod, 'API_MODE', 'unknown')),
            'facade_target': getattr(mod, 'BRIDGE_TARGET', ''),
        }
    except Exception as e:
        results[name] = {'import_ok': False, 'error': repr(e)}

out = Path('runtime') / 'facade_selftest.json'
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print(out)
