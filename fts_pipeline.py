# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from fts_config import PATHS
from fts_utils import log, now_str


@dataclass
class StageResult:
    stage: str
    status: str
    script: str | None = None
    args: list[str] | None = None
    returncode: int | None = None
    seconds: float | None = None
    critical: bool = False
    stdout_tail: str = ''
    stderr_tail: str = ''
    note: str = ''


class Level2MainlinePipeline:
    MODULE_VERSION = 'level2_full_mainline_integrated'

    def __init__(self):
        self.report_path = PATHS.runtime_dir / 'level2_mainline_pipeline.json'
        self.results: list[StageResult] = []

    def _run_script(self, script: str, *args: str, critical: bool = False, timeout: int = 7200) -> StageResult:
        target = PATHS.base_dir / script
        if not target.exists():
            result = StageResult(stage=script, status='missing', script=script, args=list(args), critical=critical, note='script not found')
            self.results.append(result)
            if critical:
                raise FileNotFoundError(script)
            return result

        cmd = [sys.executable, str(target), *args]
        start = time.time()
        log(f'🚀 Level-2 主線執行：{script} {" ".join(args)}'.rstrip())
        proc = subprocess.run(cmd, cwd=str(PATHS.base_dir), capture_output=True, text=True, encoding='utf-8', timeout=timeout, env={**os.environ, 'PYTHONPATH': str(PATHS.base_dir)})
        result = StageResult(
            stage=script,
            status='ok' if proc.returncode == 0 else 'failed',
            script=script,
            args=list(args),
            returncode=proc.returncode,
            seconds=round(time.time() - start, 3),
            critical=critical,
            stdout_tail=(proc.stdout or '')[-4000:],
            stderr_tail=(proc.stderr or '')[-4000:],
        )
        self.results.append(result)
        if proc.stdout:
            log((proc.stdout or '').strip()[:1200])
        if proc.stderr:
            log((proc.stderr or '').strip()[:1200])
        if critical and proc.returncode != 0:
            raise RuntimeError(f'{script} failed with returncode={proc.returncode}')
        return result

    def _write_report(self, status: str) -> Path:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': status,
            'results': [asdict(r) for r in self.results],
            'summary': {
                'total': len(self.results),
                'ok': sum(1 for r in self.results if r.status == 'ok'),
                'failed': sum(1 for r in self.results if r.status == 'failed'),
                'missing': sum(1 for r in self.results if r.status == 'missing'),
            },
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.report_path

    def run(self) -> tuple[Path, dict[str, Any]]:
        weekday = datetime.now().weekday()
        is_weekend = weekday >= 5
        log('=' * 72)
        log('🚦 Level-2 全主線整合啟動')
        log('=' * 72)

        # ETL mainlines
        if is_weekend:
            self.results.append(StageResult(stage='daily_chip_etl.py', status='skipped', script='daily_chip_etl.py', note='weekend skip'))
        else:
            self._run_script('daily_chip_etl.py', critical=True, timeout=7200)
        self._run_script('monthly_revenue_simple.py', critical=False, timeout=7200)
        self._run_script('yahoo_csv_to_sql.py', critical=False, timeout=7200)

        # governance / readiness sidecar, does not replace decision mainline yet
        self._run_script('formal_trading_system_v83_official_main.py', '--daily', critical=False, timeout=7200)

        # keep legacy decision / report / live-paper logic, but skip duplicate ETL stage
        legacy_env = {**os.environ, 'PYTHONPATH': str(PATHS.base_dir), 'FTS_SKIP_MAINLINE_ETL': '1'}
        target = PATHS.base_dir / 'fts_legacy_master_pipeline_impl.py'
        log('🧠 Level-2 交棒給 legacy 決策主體（已略過重複 ETL）')
        start = time.time()
        proc = subprocess.run([sys.executable, str(target)], cwd=str(PATHS.base_dir), capture_output=True, text=True, encoding='utf-8', timeout=14400, env=legacy_env)
        result = StageResult(
            stage='fts_legacy_master_pipeline_impl.py',
            status='ok' if proc.returncode == 0 else 'failed',
            script='fts_legacy_master_pipeline_impl.py',
            args=[],
            returncode=proc.returncode,
            seconds=round(time.time() - start, 3),
            critical=True,
            stdout_tail=(proc.stdout or '')[-4000:],
            stderr_tail=(proc.stderr or '')[-4000:],
            note='legacy decision/research/live-paper body executed under level-2 mainline shell',
        )
        self.results.append(result)
        if proc.stdout:
            log((proc.stdout or '').strip()[:1200])
        if proc.stderr:
            log((proc.stderr or '').strip()[:1200])
        if proc.returncode != 0:
            path = self._write_report('failed')
            raise RuntimeError(f'legacy decision body failed | report={path}')

        path = self._write_report('ok')
        payload = json.loads(path.read_text(encoding='utf-8'))
        log(f'✅ Level-2 全主線整合完成：{path}')
        return path, payload


def main() -> int:
    Level2MainlinePipeline().run()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
