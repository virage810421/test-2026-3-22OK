# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json


class EODCloseBookBuilder:
    def build(self) -> tuple[str, dict[str, Any]]:
        recon = load_json(PATHS.runtime_dir / 'reconciliation_engine.json', {}) or {}
        attrib = load_json(PATHS.runtime_dir / 'performance_attribution.json', {}) or {}
        daily_ops = load_json(PATHS.runtime_dir / 'daily_ops_summary.json', {}) or {}
        payload = {
            'generated_at': now_str(),
            'status': 'closebook_ready',
            'reconciliation_status': recon.get('status', 'missing'),
            'all_green': recon.get('all_green', False),
            'pnl_summary': attrib.get('headline', {}),
            'close_notes': daily_ops.get('close_notes', []),
            'next_day_resume_ready': recon.get('all_green', False),
        }
        path = PATHS.runtime_dir / 'eod_closebook.json'
        write_json(path, payload)
        return str(path), payload
