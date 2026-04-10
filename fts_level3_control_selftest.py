# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import py_compile
from pathlib import Path

TARGETS = [
    'fts_control_tower.py',
    'formal_trading_system_v83_official_main.py',
    'master_pipeline.py',
    'fts_pipeline.py',
    'launcher.py',
    'yahoo_csv_to_sql.py',
]

results = {}
for name in TARGETS:
    try:
        py_compile.compile(name, doraise=True)
        results[name] = {'compile_ok': True}
    except Exception as e:
        results[name] = {'compile_ok': False, 'error': repr(e)}

try:
    from pathlib import Path as _Path
    from fts_control_tower import _call_builder_result
    from fts_gatekeeper import LaunchGatekeeper
    from fts_live_release_gate import LiveReleaseGate

    launch_path, launch_payload = _call_builder_result(
        LaunchGatekeeper(),
        'evaluate',
        {'ready': [], 'missing': []},
        {'failed': []},
        {'items': []},
        {'row_count': 0, 'rows_with_price': 0, 'rows_with_ticker': 0, 'rows_with_action': 0},
        {'total_signals': 0},
        fallback_path=_Path('runtime/launch_gate.json'),
    )
    live_release_path, live_release_payload = _call_builder_result(
        LiveReleaseGate(),
        'evaluate',
        governance={},
        safety={},
        recon={},
        recovery={},
        approval={},
        broker_contract={'defined': True},
        fallback_path=_Path('runtime/live_release_gate.json'),
    )
    results['control_tower_builder_smoke'] = {
        'ok': True,
        'launch_gate_path': str(launch_path),
        'launch_gate_has_payload': isinstance(launch_payload, dict),
        'live_release_path': str(live_release_path),
        'live_release_has_payload': isinstance(live_release_payload, dict),
    }
except Exception as e:
    results['control_tower_builder_smoke'] = {'ok': False, 'error': repr(e)}

out = Path('runtime') / 'level3_control_selftest.json'
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
print(out)
