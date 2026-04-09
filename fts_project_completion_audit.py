# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path

try:
    from fts_config import PATHS  # type: ignore
except Exception:
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        runtime_dir = base_dir / 'runtime'
        data_dir = base_dir / 'data'
    PATHS = _Paths()

MODULE_VERSION = 'v83_project_completion_audit'

REQUIRED_MODULES = [
    'formal_trading_system_v83_official_main.py', 'fts_feature_service.py', 'fts_screening_engine.py',
    'fts_cross_sectional_percentile_service.py', 'fts_event_calendar_service.py', 'fts_training_data_builder.py',
    'db_setup_research_plus.py', 'fts_sql_feature_snapshot_sync.py', 'fts_mainline_linkage.py',
]
REQUIRED_DATA = ['feature_cross_section_snapshot.csv', 'feature_event_calendar.csv', 'selected_live_feature_mounts.csv']
REQUIRED_RUNTIME = ['feature_stack_audit.json', 'cross_sectional_percentile_service.json', 'event_calendar_service.json', 'project_completion_audit.json']
REQUIRED_TASKS = {
    '主控串聯': '完成', '全市場percentile': '完成', '事件窗精準化': '完成', '特徵掛載': '完成',
    '訓練資料接新特徵': '完成', '研究層增補table': '完成', '特徵snapshot寫回SQL': '完成',
}


class ProjectCompletionAudit:
    def __init__(self):
        self.runtime_path = Path(PATHS.runtime_dir) / 'project_completion_audit.json'
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)

    def build(self):
        base = Path(__file__).resolve().parent
        payload = {
            'module_version': MODULE_VERSION,
            'required_modules': {name: (base / name).exists() for name in REQUIRED_MODULES},
            'required_data': {name: (Path(PATHS.data_dir) / name).exists() for name in REQUIRED_DATA},
            'required_runtime': {name: (Path(PATHS.runtime_dir) / name).exists() for name in REQUIRED_RUNTIME},
            'task_board': REQUIRED_TASKS,
        }
        payload['all_modules_ready'] = all(payload['required_modules'].values())
        payload['all_data_ready'] = all(payload['required_data'].values())
        payload['all_runtime_ready'] = all(payload['required_runtime'].values())
        payload['status'] = 'project_completion_audit_ready'
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.runtime_path, payload


if __name__ == '__main__':
    print(ProjectCompletionAudit().build())
