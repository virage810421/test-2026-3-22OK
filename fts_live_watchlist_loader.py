# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config import PARAMS, WATCH_LIST
from fts_config import PATHS
from fts_utils import now_str, log
from fts_live_watchlist_registry import load_approved, summary as registry_summary


class ApprovedLiveWatchlistLoader:
    MODULE_VERSION = 'v86_approved_live_watchlist_loader'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'approved_live_watchlist_loader.json'

    def _fallback(self) -> list[str]:
        return list(dict.fromkeys([str(x).strip() for x in WATCH_LIST if str(x).strip()]))

    def load_live_watchlist(self) -> list[str]:
        if not bool(PARAMS.get('APPROVED_LIVE_WATCHLIST_ENABLED', True)):
            return self._fallback()
        approved = load_approved()
        rows = approved.get('rows', []) if isinstance(approved, dict) else []
        if not rows:
            return self._fallback()
        max_total = int(PARAMS.get('LIVE_WATCHLIST_MAX_NAMES', 12))
        max_per_sector = int(PARAMS.get('LIVE_WATCHLIST_MAX_PER_SECTOR', 3))
        sector_counts: dict[str, int] = {}
        chosen: list[str] = []
        ordered = sorted(rows, key=lambda r: (float(r.get('promotion_score', 0.0) or 0.0), -int(r.get('sector_rank', 9999) or 9999), -int(r.get('global_rank', 9999) or 9999)), reverse=True)
        for row in ordered:
            ticker = str(row.get('ticker', '')).strip()
            if not ticker or ticker in chosen:
                continue
            sector = str(row.get('sector', 'OTHERS') or 'OTHERS')
            if sector_counts.get(sector, 0) >= max_per_sector:
                continue
            if bool(row.get('feature_integrity_ok', True)) is False:
                continue
            if bool(row.get('liquidity_ok', True)) is False:
                continue
            chosen.append(ticker)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            if len(chosen) >= max_total:
                break
        return chosen or self._fallback()

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        approved = load_approved()
        selected = self.load_live_watchlist()
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'approved_present': bool(approved),
            'selected_ticker_count': len(selected),
            'selected_tickers': selected,
            'registry_summary': registry_summary(),
            'status': 'approved_live_watchlist_loader_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🛰️ approved live watchlist loader ready: {self.runtime_path}')
        return self.runtime_path, payload


if __name__ == '__main__':
    path, payload = ApprovedLiveWatchlistLoader().build_summary()
    print(path)
    print(payload.get('status'))
