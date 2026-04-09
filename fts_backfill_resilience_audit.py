# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class BackfillResilienceAudit:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'backfill_resilience_audit.json'
        self.backfill_path = PATHS.runtime_dir / 'fundamentals_true_backfill.json'
        self.recon_path = PATHS.runtime_dir / 'reconciliation_engine.json'

    def build(self) -> tuple[Path, dict[str, Any]]:
        backfill = json.loads(self.backfill_path.read_text(encoding='utf-8')) if self.backfill_path.exists() else {}
        recon = json.loads(self.recon_path.read_text(encoding='utf-8')) if self.recon_path.exists() else {}
        stale_flags = []
        for name, info in (backfill.get('staleness') or {}).items():
            if info.get('stale'):
                stale_flags.append(name)
        corporate_actions = int(recon.get('summary', {}).get('corporate_action_suspect_count', 0) or 0)
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'backfill_report_exists': self.backfill_path.exists(),
            'reconciliation_report_exists': self.recon_path.exists(),
            'stale_streams': stale_flags,
            'corporate_action_suspects': corporate_actions,
            'repair_actions': recon.get('repair_actions', []),
            'status': 'resilience_attention_needed' if stale_flags or corporate_actions else 'resilience_ok',
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧪 已輸出 backfill resilience audit：{self.path}')
        return self.path, payload
