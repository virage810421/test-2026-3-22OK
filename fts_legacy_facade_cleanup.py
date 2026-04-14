# -*- coding: utf-8 -*-
from __future__ import annotations

"""清掉舊門牌 + 保留功能本體 套用清理器。

用途：
1. 本 ZIP 不再提供舊門牌 wrapper。
2. 若你是覆蓋到既有專案，舊專案資料夾裡可能仍殘留舊檔名。
3. 執行 python fts_legacy_facade_cleanup.py --apply 可刪掉這些殘留舊門牌。
"""

import json
import sys
from pathlib import Path

RETIRED_OLD_DOORS = [
    'advanced_chart.py',
    'daily_chip_etl.py',
    'monthly_revenue_simple.py',
    'yahoo_csv_to_sql.py',
    'ml_data_generator.py',
    'ml_trainer.py',
    'formal_trading_system.py',
    'kline_cache.py',
    'screening.py',
    'strategies.py',
    'master_pipeline.py',
]

FUNCTION_BODY_OWNERS = {
    'advanced_chart.py': 'fts_chart_service.py',
    'daily_chip_etl.py': 'fts_etl_daily_chip_service.py',
    'monthly_revenue_simple.py': 'fts_etl_monthly_revenue_service.py',
    'yahoo_csv_to_sql.py': 'fts_fundamentals_etl_mainline.py',
    'ml_data_generator.py': 'fts_training_data_builder.py',
    'ml_trainer.py': 'fts_trainer_backend.py',
    'formal_trading_system.py': 'formal_trading_system_v83_official_main.py',
    'kline_cache.py': 'fts_market_data_service.py',
    'screening.py': 'fts_screening_engine.py / fts_service_api.py',
    'strategies.py': 'fts_signal_primitives.py / fts_screening_engine.py',
    'master_pipeline.py': 'fts_pipeline.py / fts_control_tower.py',
}


def main(apply: bool = False) -> int:
    base_dir = Path(__file__).resolve().parent
    runtime_dir = base_dir / 'runtime'
    runtime_dir.mkdir(parents=True, exist_ok=True)

    removed = []
    missing = []
    blocked = []
    planned = []

    for name in RETIRED_OLD_DOORS:
        path = base_dir / name
        owner = FUNCTION_BODY_OWNERS.get(name, '')
        if not path.exists():
            missing.append({'file': name, 'function_body_owner': owner})
            continue
        if path.resolve() == Path(__file__).resolve():
            blocked.append({'file': name, 'reason': 'self_protection'})
            continue
        if apply:
            try:
                path.unlink()
                removed.append({'file': name, 'function_body_owner': owner})
            except Exception as exc:
                blocked.append({'file': name, 'reason': repr(exc), 'function_body_owner': owner})
        else:
            planned.append({'file': name, 'function_body_owner': owner})

    payload = {
        'apply': bool(apply),
        'policy': 'clean_old_doors_keep_function_body',
        'retired_old_doors': RETIRED_OLD_DOORS,
        'function_body_owners': FUNCTION_BODY_OWNERS,
        'planned': planned,
        'removed': removed,
        'missing': missing,
        'blocked': blocked,
        'status': 'applied' if apply else 'dry_run_ready',
    }
    out = runtime_dir / 'old_door_cleanup.json'
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"🧹 舊門牌清理報告：{out}")
    print(f"   apply={apply} | removed={len(removed)} | planned={len(planned)} | missing={len(missing)} | blocked={len(blocked)}")
    return 0 if not blocked else 1


if __name__ == '__main__':
    raise SystemExit(main(apply='--apply' in sys.argv))
