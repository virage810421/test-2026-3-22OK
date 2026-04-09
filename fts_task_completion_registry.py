# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

try:
    from fts_config import PATHS  # type: ignore
except Exception:
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        runtime_dir = base_dir / 'runtime'
        data_dir = base_dir / 'data'
    PATHS = _Paths()

TASKS = [
    ('主控串聯', '主線'), ('全市場percentile', '研究層'), ('事件窗精準化', '研究層'), ('特徵掛載', '研究層'),
    ('訓練資料接新特徵', 'AI訓練'), ('研究層增補table', 'SQL'), ('特徵snapshot寫回SQL', 'SQL'),
]


class TaskCompletionRegistry:
    def __init__(self):
        self.csv_path = Path(PATHS.data_dir) / 'task_completion_registry.csv'
        self.runtime_path = Path(PATHS.runtime_dir) / 'task_completion_registry.json'
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)

    def build(self):
        df = pd.DataFrame([{'任務名稱': t, '任務分類': c, '完成狀態': '完成'} for t, c in TASKS])
        df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        payload = {'rows': int(len(df)), 'csv_path': str(self.csv_path), 'status': 'task_completion_registry_ready'}
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.runtime_path, payload
