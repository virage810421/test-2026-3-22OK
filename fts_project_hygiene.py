# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List
from datetime import datetime


def now_str() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


@dataclass
class ProjectHygieneReport:
    moved_versions: List[Dict[str, str]] = field(default_factory=list)
    moved_docs: List[Dict[str, str]] = field(default_factory=list)
    created_paths: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    status: str = 'pending'


class ProjectHygieneManager:
    OLD_MAINS = [
        'formal_trading_system_v79.py',
        'formal_trading_system_v80_prebroker_sealed.py',
        'formal_trading_system_v81_mainline_merged.py',
        'formal_trading_system_v82_three_stage_upgrade.py',
    ]

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.runtime_dir = self.base_dir / 'runtime'
        self.archive_dir = self.base_dir / 'archive'
        self.report_path = self.runtime_dir / 'project_hygiene.json'

    def _safe_move(self, src: Path, dst: Path) -> bool:
        if not src.exists():
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            dst = dst.with_name(f'{dst.stem}_{stamp}{dst.suffix}')
        shutil.move(str(src), str(dst))
        return True

    def run(self) -> tuple[Path, dict]:
        r = ProjectHygieneReport()
        for p in [
            self.archive_dir / 'versions',
            self.archive_dir / 'docs' / 'upgrades',
            self.archive_dir / 'root_exports',
            self.runtime_dir / 'history',
            self.runtime_dir / 'errors',
            self.base_dir / 'docs' / 'upgrades',
            self.base_dir / 'data' / 'templates',
            self.base_dir / 'data' / 'audit',
            self.base_dir / 'data' / 'fundamentals',
        ]:
            p.mkdir(parents=True, exist_ok=True)
            r.created_paths.append(str(p))

        for name in self.OLD_MAINS:
            src = self.base_dir / name
            dst = self.archive_dir / 'versions' / name
            if self._safe_move(src, dst):
                r.moved_versions.append({'from': str(src), 'to': str(dst)})

        for src in sorted(self.base_dir.glob('UPDATED_FILES_v*.md')) + sorted(self.base_dir.glob('UPGRADE_SUMMARY_v*.md')):
            dst = self.base_dir / 'docs' / 'upgrades' / src.name
            if self._safe_move(src, dst):
                r.moved_docs.append({'from': str(src), 'to': str(dst)})

        wrapper = self.base_dir / 'formal_trading_system.py'
        if not wrapper.exists():
            wrapper.write_text(
                "from formal_trading_system_v83_official_main import main\n"
                "if __name__ == '__main__':\n"
                "    raise SystemExit(main())\n",
                encoding='utf-8'
            )

        r.status = 'project_hygiene_applied'
        payload = {
            'generated_at': now_str(),
            'status': r.status,
            'moved_versions': r.moved_versions,
            'moved_docs': r.moved_docs,
            'created_paths': r.created_paths,
            'warnings': r.warnings,
            'recommended_entry': str(self.base_dir / 'formal_trading_system.py'),
            'official_entry': str(self.base_dir / 'formal_trading_system_v83_official_main.py'),
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.report_path, payload
