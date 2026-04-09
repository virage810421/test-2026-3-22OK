# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path

from fts_project_hygiene import ProjectHygieneManager
from fts_runtime_cleanup import RuntimeCleanupManager
from fts_data_cleanup import DataCleanupManager
from fts_fundamentals_true_backfill import FundamentalsTrueBackfill


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    runtime_dir = base_dir / 'runtime'
    runtime_dir.mkdir(parents=True, exist_ok=True)

    outputs = {}
    for runner_cls, key in [
        (ProjectHygieneManager, 'project_hygiene'),
        (RuntimeCleanupManager, 'runtime_cleanup'),
        (DataCleanupManager, 'data_cleanup'),
        (FundamentalsTrueBackfill, 'fundamentals_true_backfill'),
    ]:
        path, payload = runner_cls(base_dir).run()
        outputs[key] = {'path': str(path), 'status': payload.get('status')}

    report_path = runtime_dir / 'v83_housekeeping_once.json'
    report_path.write_text(json.dumps(outputs, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'✅ v83 housekeeping 完成：{report_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
