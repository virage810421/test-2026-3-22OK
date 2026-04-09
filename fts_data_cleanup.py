# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from datetime import datetime


def now_str() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class DataCleanupManager:
    TEMPLATE_NAMES = {
        'last_price_snapshot_template.csv',
        'manual_price_snapshot_template.csv',
        'ml_training_data_template.csv',
    }

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / 'data'
        self.runtime_dir = self.base_dir / 'runtime'
        self.archive_root = self.base_dir / 'archive' / 'root_exports'
        self.templates_dir = self.data_dir / 'templates'
        self.audit_dir = self.data_dir / 'audit'
        self.report_path = self.runtime_dir / 'data_cleanup.json'

    def _file_info(self, p: Path) -> dict:
        return {
            'path': str(p),
            'exists': p.exists(),
            'size': p.stat().st_size if p.exists() else None,
            'mtime': p.stat().st_mtime if p.exists() else None,
        }

    def _copy_if_needed(self, src: Path, dst: Path):
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.exists():
            shutil.copy2(src, dst)

    def run(self) -> tuple[Path, dict]:
        self.templates_dir.mkdir(parents=True, exist_ok=True)
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.archive_root.mkdir(parents=True, exist_ok=True)

        actions = []
        root_decision = self.base_dir / 'daily_decision_desk.csv'
        data_decision = self.data_dir / 'daily_decision_desk.csv'
        chosen = None
        if root_decision.exists() and data_decision.exists():
            chosen = data_decision if data_decision.stat().st_size >= root_decision.stat().st_size else root_decision
            backup_target = self.archive_root / 'daily_decision_desk.root.backup.csv'
            self._copy_if_needed(root_decision, backup_target)
            if chosen == data_decision:
                shutil.copy2(data_decision, root_decision)
                actions.append({'action': 'sync_root_from_data', 'source': str(data_decision), 'target': str(root_decision)})
            else:
                data_decision.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(root_decision, data_decision)
                actions.append({'action': 'sync_data_from_root', 'source': str(root_decision), 'target': str(data_decision)})
        elif root_decision.exists() and not data_decision.exists():
            shutil.copy2(root_decision, data_decision)
            chosen = root_decision
            actions.append({'action': 'create_data_decision_from_root', 'source': str(root_decision), 'target': str(data_decision)})
        elif data_decision.exists() and not root_decision.exists():
            shutil.copy2(data_decision, root_decision)
            chosen = data_decision
            actions.append({'action': 'create_root_decision_from_data', 'source': str(data_decision), 'target': str(root_decision)})

        for name in self.TEMPLATE_NAMES:
            src = self.data_dir / name
            if src.exists():
                dst = self.templates_dir / name
                shutil.copy2(src, dst)
                actions.append({'action': 'template_snapshot', 'source': str(src), 'target': str(dst)})

        suspicious = []
        for src in sorted(self.data_dir.glob('*')):
            if src.is_file() and src.stat().st_size < 300 and src.name not in self.TEMPLATE_NAMES:
                suspicious.append(self._file_info(src))

        datakline = self.data_dir / 'datakline_cache.csv'
        if datakline.exists():
            self._copy_if_needed(datakline, self.audit_dir / datakline.name)
            actions.append({'action': 'audit_copy_suspicious_name', 'source': str(datakline), 'target': str(self.audit_dir / datakline.name)})

        payload = {
            'generated_at': now_str(),
            'status': 'data_cleanup_applied',
            'canonical_daily_decision_desk': str((self.data_dir / 'daily_decision_desk.csv').resolve()),
            'root_daily_decision_desk_kept_for_compat': str(root_decision.resolve()) if root_decision.exists() else None,
            'actions': actions,
            'suspicious_small_files': suspicious,
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.report_path, payload
