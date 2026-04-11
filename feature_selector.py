# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFECV

from fts_config import PATHS, CONFIG
from fts_utils import now_str
from fts_research_lab import ResearchLab

_LAB = ResearchLab()


def auto_select_best_features(csv_file: str | None = None) -> dict[str, Any] | None:
    csv_path = Path(csv_file or (PATHS.data_dir / 'ml_training_data.csv'))
    if not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    if 'Label_Y' not in df.columns:
        return None

    drop_cols = {'Ticker', 'Ticker SYMBOL', 'Date', 'Setup', 'Regime', 'Label_Y', 'Target_Return'}
    feature_pool = [c for c in df.columns if c not in drop_cols]
    if len(feature_pool) < int(getattr(CONFIG, 'selected_features_min_count_for_training', 8)):
        return None

    numeric_df = df[feature_pool].apply(pd.to_numeric, errors='coerce').fillna(0.0)
    y = pd.to_numeric(df['Label_Y'], errors='coerce').fillna(0).astype(int)
    if len(numeric_df) < 120 or y.nunique() < 2:
        selected = feature_pool[: int(getattr(CONFIG, 'selected_features_min_count_for_training', 8))]
        mode = 'fallback_small_sample'
    else:
        estimator = RandomForestClassifier(n_estimators=120, random_state=42, n_jobs=-1, class_weight='balanced_subsample')
        selector = RFECV(estimator, step=1, cv=5, scoring='accuracy', min_features_to_select=max(4, int(getattr(CONFIG, 'selected_features_min_count_for_training', 8)) // 2))
        selector = selector.fit(numeric_df, y)
        selected = [f for f, keep in zip(feature_pool, selector.support_) if keep]
        if not selected:
            selected = feature_pool[: int(getattr(CONFIG, 'selected_features_min_count_for_training', 8))]
        mode = 'rfecv_train_only_candidate'

    candidate_id = now_str().replace(':', '').replace('-', '').replace('T', '_')
    area = 'feature_candidates'
    pkl_path = _LAB.area(area) / f'selected_features_candidate_{candidate_id}.pkl'
    json_path = _LAB.area(area) / f'selected_features_candidate_{candidate_id}.json'
    joblib.dump(selected, pkl_path)
    payload = {
        'candidate_id': candidate_id,
        'generated_at': now_str(),
        'csv_file': str(csv_path),
        'selection_mode': mode,
        'feature_pool_count': len(feature_pool),
        'selected_count': len(selected),
        'selected_features': selected,
        'writes_production_selected_features': False,
        'note': 'research candidate only; production selected_features.pkl remains unchanged',
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    _LAB.append_registry(area, {
        'candidate_id': candidate_id,
        'generated_at': payload['generated_at'],
        'selected_count': len(selected),
        'artifact_path': str(json_path),
        'status': 'candidate_only_not_live',
    })
    return payload


if __name__ == '__main__':
    print(auto_select_best_features())
