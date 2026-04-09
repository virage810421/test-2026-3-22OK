# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import shutil
from pathlib import Path
from datetime import datetime


def now_str() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class RuntimeCleanupManager:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.runtime_dir = self.base_dir / 'runtime'
        self.history_dir = self.runtime_dir / 'history'
        self.errors_dir = self.runtime_dir / 'errors'
        self.report_path = self.runtime_dir / 'runtime_cleanup.json'

    def _move(self, src: Path, dst_dir: Path, moved: list[dict]):
        if not src.exists():
            return
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        if dst.exists():
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            dst = dst.with_name(f'{dst.stem}_{stamp}{dst.suffix}')
        shutil.move(str(src), str(dst))
        moved.append({'from': str(src), 'to': str(dst)})

    def run(self) -> tuple[Path, dict]:
        moved_history, moved_errors = [], []
        history_patterns = [
            'config_snapshot_*.json',
            'run_manifest_v*.json',
            'upgrade_status_report_v*.md',
            'formal_trading_system_v80_*',
            'formal_trading_system_v81_*',
            'formal_trading_system_v82_*',
            'wave1_*.json',
            'legacy_*.json',
            'target95_*.json',
        ]
        for pattern in history_patterns:
            for src in sorted(self.runtime_dir.glob(pattern)):
                self._move(src, self.history_dir, moved_history)

        for src in sorted(self.runtime_dir.glob('*error*.json')):
            self._move(src, self.errors_dir, moved_errors)

        payload = {
            'generated_at': now_str(),
            'status': 'runtime_cleanup_applied',
            'moved_to_history': moved_history,
            'moved_to_errors': moved_errors,
            'current_runtime_root_preserved': True,
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.report_path, payload
