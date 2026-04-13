# -*- coding: utf-8 -*-
from __future__ import annotations

"""Level-2 mainline orchestrator.

收尾目標：
1. 預設改成 service-first，不再默默執行 legacy decision engine。
2. 只有在明確開啟 execute_legacy_pipeline 時，才進入 legacy compatibility。
3. 將 heuristics / model / execution 的分層狀態寫入 runtime，避免新舊主線混在一起。
"""

import contextlib
import importlib
import io
import json
import os
import subprocess
import sys
import traceback
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import log, now_str

RUNTIME_PATH = PATHS.runtime_dir / 'level2_mainline_runtime.json'


@dataclass
class StageResult:
    stage: str
    target: str
    ok: bool
    mode: str
    returncode: int | None = None
    error: str = ''
    stdout_tail: str = ''
    stderr_tail: str = ''
    seconds: float = 0.0
    skipped: bool = False


class ExternalScriptRunner:
    def run_script(self, script_name: str, timeout: int = 1800, critical: bool = False) -> StageResult:
        target = PATHS.base_dir / script_name
        if not target.exists():
            return StageResult(stage='script', target=script_name, ok=not critical, mode='subprocess', error='missing', skipped=not critical)
        cmd = [sys.executable, str(target)]
        env = os.environ.copy()
        env['PYTHONUTF8'] = '1'
        env['PYTHONIOENCODING'] = 'utf-8'
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(PATHS.base_dir),
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=timeout,
                env=env,
            )
            ok = proc.returncode == 0
            return StageResult(
                stage='script',
                target=script_name,
                ok=ok,
                mode='subprocess',
                returncode=proc.returncode,
                stdout_tail='\n'.join((proc.stdout or '').splitlines()[-20:]),
                stderr_tail='\n'.join((proc.stderr or '').splitlines()[-20:]),
            )
        except Exception as exc:
            return StageResult(stage='script', target=script_name, ok=False, mode='subprocess', error=repr(exc))


class StageManager:
    def __init__(self, runner: ExternalScriptRunner | None = None):
        self.runner = runner or ExternalScriptRunner()

    def _call_module_main(self, module_name: str, attr_name: str = 'main') -> StageResult:
        buf_out = io.StringIO()
        buf_err = io.StringIO()
        try:
            mod = importlib.import_module(module_name)
            fn = getattr(mod, attr_name)
            with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
                result = fn()
            ok = False if isinstance(result, int) and result != 0 else True
            return StageResult(
                stage='python',
                target=f'{module_name}.{attr_name}',
                ok=ok,
                mode='inprocess',
                stdout_tail='\n'.join(buf_out.getvalue().splitlines()[-20:]),
                stderr_tail='\n'.join(buf_err.getvalue().splitlines()[-20:]),
            )
        except Exception:
            return StageResult(
                stage='python',
                target=f'{module_name}.{attr_name}',
                ok=False,
                mode='inprocess',
                error=traceback.format_exc()[-4000:],
                stdout_tail='\n'.join(buf_out.getvalue().splitlines()[-20:]),
                stderr_tail='\n'.join(buf_err.getvalue().splitlines()[-20:]),
            )

    def run(self, execute_legacy: bool = False) -> dict[str, Any]:
        execute_legacy = bool(execute_legacy and getattr(CONFIG, 'execute_legacy_pipeline', False))
        etl_results = []
        for script_name in ['daily_chip_etl.py', 'monthly_revenue_simple.py', 'yahoo_csv_to_sql.py']:
            result = self.runner.run_script(script_name, timeout=getattr(CONFIG, 'upstream_timeout_seconds', 3600), critical=False)
            etl_results.append(asdict(result))
            log(f"🔁 mainline stage | {script_name} | ok={result.ok} | skipped={result.skipped}")

        legacy_result = asdict(StageResult(stage='python', target='fts_legacy_master_pipeline_impl.main', ok=True, mode='inprocess', skipped=not execute_legacy))
        if execute_legacy:
            result = self._call_module_main('fts_legacy_master_pipeline_impl', 'main')
            legacy_result = asdict(result)
            log(f"🧠 legacy decision engine | ok={result.ok}")
        else:
            log('🧭 level-2 mainline | legacy decision engine skipped (service-first mode)')

        etl_ok = all(r['ok'] or r['skipped'] for r in etl_results)
        payload = {
            'generated_at': now_str(),
            'module_version': 'v86_level2_service_first_hardened',
            'status': 'mainline_ready' if (etl_ok and (legacy_result['ok'] or legacy_result['skipped'])) else 'mainline_degraded',
            'legacy_mode': 'compatibility_enabled' if execute_legacy else 'service_first',
            'legacy_pipeline_enabled': bool(execute_legacy),
            'etl': etl_results,
            'legacy_pipeline': legacy_result,
            'decision_csv': str(PATHS.base_dir / 'daily_decision_desk.csv'),
            'model_dir': str(PATHS.model_dir),
            'boundary_status': {
                'heuristic_scope': 'setup_diagnostics_only',
                'model_scope': 'edge_probability_and_expectancy',
                'execution_scope': 'sizing_and_order_constraints',
            },
        }
        RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return payload


def run_level2_mainline(execute_legacy: bool = False) -> tuple[str, dict[str, Any]]:
    payload = StageManager().run(execute_legacy=execute_legacy)
    return str(RUNTIME_PATH), payload


def main() -> int:
    path, payload = run_level2_mainline(execute_legacy=False)
    log(f'✅ level-2 mainline 完成：{path}')
    return 0 if payload.get('status') in {'mainline_ready', 'mainline_degraded'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
