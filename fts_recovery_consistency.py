# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, normalize_key, write_json


class RecoveryConsistencySuite:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'recovery_consistency_report.json'

    def build(self, retry_queue_summary: dict[str, Any], broker_snapshot: dict[str, Any] | None = None) -> tuple[Any, dict[str, Any]]:
        state = load_json(PATHS.state_dir / 'engine_state.json', {}) or {}
        broker_snapshot = broker_snapshot or {}
        failures = []
        warnings = []
        if not state:
            failures.append({'type': 'missing_state_file', 'message': '尚未找到 state/engine_state.json'})
        if int(retry_queue_summary.get('total', 0) or 0) > 0:
            warnings.append({'type': 'pending_retry_queue', 'message': f"retry queue 目前仍有 {retry_queue_summary.get('total', 0)} 筆待處理/已記錄項目"})
        state_tickers = {normalize_key(x.get('ticker')) for x in state.get('positions', [])}
        broker_tickers = {normalize_key(x.get('ticker')) for x in broker_snapshot.get('positions', [])}
        missing_on_broker = sorted(x for x in state_tickers - broker_tickers if x)
        orphan_on_broker = sorted(x for x in broker_tickers - state_tickers if x)
        if missing_on_broker:
            failures.append({'type': 'state_position_missing_on_broker', 'tickers': missing_on_broker[:50]})
        if orphan_on_broker:
            warnings.append({'type': 'broker_position_missing_in_state', 'tickers': orphan_on_broker[:50]})
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'checks': {
                'state_file_exists': bool(state),
                'retry_queue_total': int(retry_queue_summary.get('total', 0) or 0),
                'broker_snapshot_found': bool(broker_snapshot),
                'missing_on_broker_count': len(missing_on_broker),
                'orphan_on_broker_count': len(orphan_on_broker),
            },
            'failures': failures,
            'warnings': warnings,
            'all_passed': len(failures) == 0,
            'status': 'consistency_green' if len(failures) == 0 else 'consistency_break',
        }
        write_json(self.path, payload)
        log(f'🧩 已輸出 recovery consistency report：{self.path}')
        return self.path, payload
