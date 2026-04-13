# -*- coding: utf-8 -*-
from __future__ import annotations

"""Archive-and-remove helper for retired legacy facade files.

預設 dry_run=True，只輸出計畫，不直接刪檔。
要真的執行請：python fts_legacy_facade_cleanup.py --apply
"""

import json
import shutil
import sys
from pathlib import Path

LEGACY_FACADES = [
    'advanced_chart.py',
    'screening.py',
    'strategies.py',
    'master_pipeline.py',
    'ml_data_generator.py',
    'ml_trainer.py',
    'yahoo_csv_to_sql.py',
]


def main(apply: bool = False) -> int:
    base_dir = Path(__file__).resolve().parent
    runtime_dir = base_dir / 'runtime'
    runtime_dir.mkdir(parents=True, exist_ok=True)
    archive_dir = base_dir / 'absorbed_references' / 'legacy_facades_retired'
    archive_dir.mkdir(parents=True, exist_ok=True)

    moved = []
    missing = []
    for name in LEGACY_FACADES:
        src = base_dir / name
        if not src.exists():
            missing.append(name)
            continue
        if apply:
            dst = archive_dir / name
            shutil.move(str(src), str(dst))
            moved.append({'from': str(src), 'to': str(dst)})
        else:
            moved.append({'planned_from': str(src), 'planned_to': str(archive_dir / name)})

    payload = {
        'apply': bool(apply),
        'facades': LEGACY_FACADES,
        'moved_or_planned': moved,
        'missing': missing,
        'status': 'legacy_facades_retired' if apply else 'dry_run_ready',
    }
    out = runtime_dir / 'legacy_facade_cleanup.json'
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'🧹 legacy facade cleanup: {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main(apply='--apply' in sys.argv))
