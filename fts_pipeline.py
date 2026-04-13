# -*- coding: utf-8 -*-
from __future__ import annotations

"""Level-2 mainline orchestrator.

收尾目標：
1. Level-2 只跑正式 service module，不再經過 legacy wrapper 檔名。
2. legacy decision engine 已自 Level-2 主線拆離，不再由 control tower 直接觸發。
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




class StageManager:
    def __init__(self):
        self.stage_modules: list[tuple[str, str, str]] = [
            ('etl', 'fts_etl_daily_chip_service', 'main_scheduler'),
            ('etl', 'fts_etl_monthly_revenue_service', 'main'),
            ('etl', 'fts_fundamentals_etl_mainline', 'main'),
        ]

    def _call_module_main(self, module_name: str, attr_name: str = 'main', stage: str = 'python') -> StageResult:
        buf_out = io.StringIO()
        buf_err = io.StringIO()
        try:
            mod = importlib.import_module(module_name)
            fn = getattr(mod, attr_name)
            with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
                result = fn()
            ok = False if isinstance(result, int) and result != 0 else True
            return StageResult(
                stage=stage,
                target=f'{module_name}.{attr_name}',
                ok=ok,
                mode='inprocess',
                stdout_tail='\n'.join(buf_out.getvalue().splitlines()[-20:]),
                stderr_tail='\n'.join(buf_err.getvalue().splitlines()[-20:]),
            )
        except Exception:
            return StageResult(
                stage=stage,
                target=f'{module_name}.{attr_name}',
                ok=False,
                mode='inprocess',
                error=traceback.format_exc()[-4000:],
                stdout_tail='\n'.join(buf_out.getvalue().splitlines()[-20:]),
                stderr_tail='\n'.join(buf_err.getvalue().splitlines()[-20:]),
            )

    def run(self) -> dict[str, Any]:
        etl_results = []
        for stage_name, module_name, attr_name in self.stage_modules:
            result = self._call_module_main(module_name, attr_name, stage=stage_name)
            etl_results.append(asdict(result))
            log(f"🔁 mainline stage | {module_name}.{attr_name} | ok={result.ok} | skipped={result.skipped}")

        etl_ok = all(r['ok'] or r['skipped'] for r in etl_results)
        payload = {
            'generated_at': now_str(),
            'module_version': 'v87_level2_service_detached',
            'status': 'mainline_ready' if etl_ok else 'mainline_degraded',
            'legacy_mode': 'detached_from_level2',
            'legacy_pipeline_enabled': False,
            'legacy_pipeline': {
                'stage': 'python',
                'target': 'fts_legacy_master_pipeline_impl.main',
                'ok': True,
                'mode': 'detached',
                'skipped': True,
                'note': 'legacy decision engine 已自 Level-2 主線拆離；如需單獨研究請手動執行 legacy facade。',
            },
            'etl': etl_results,
            'service_modules': [
                f'{module_name}.{attr_name}' for _, module_name, attr_name in self.stage_modules
            ],
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


def run_level2_mainline() -> tuple[str, dict[str, Any]]:
    payload = StageManager().run()
    return str(RUNTIME_PATH), payload


def main() -> int:
    path, payload = run_level2_mainline()
    log(f'✅ level-2 mainline 完成：{path}')
    return 0 if payload.get('status') in {'mainline_ready', 'mainline_degraded'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
