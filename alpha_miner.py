# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_classif
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

from fts_config import PATHS
from fts_utils import now_str
from fts_research_lab import ResearchLab

_LAB = ResearchLab()


def _load_latest_candidate_payload() -> dict[str, Any] | None:
    rows = _LAB.load_registry('alpha_candidates')
    if not rows:
        return None
    path = rows[-1].get('artifact_path')
    if not path:
        return None
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except Exception:
        return None


def run_alpha_forge(csv_file: str | None = None, top_n: int = 20) -> dict[str, Any] | None:
    csv_path = Path(csv_file or (PATHS.data_dir / 'ml_training_data.csv'))
    if not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    if 'Label_Y' not in df.columns:
        return None
    drop_cols = {'Ticker', 'Ticker SYMBOL', 'Date', 'Setup', 'Regime', 'Label_Y', 'Target_Return'}
    base_features = [c for c in df.columns if c not in drop_cols]
    X = df[base_features].apply(pd.to_numeric, errors='coerce').fillna(0.0)
    y = pd.to_numeric(df['Label_Y'], errors='coerce').fillna(0).astype(int)
    if len(X) < 100 or y.nunique() < 2:
        return None

    numeric_cols = [c for c in base_features if X[c].nunique() > 5][:12]
    bool_cols = [c for c in base_features if X[c].nunique() <= 2][:12]
    new_features = pd.DataFrame(index=X.index)

    for i in range(len(numeric_cols)):
        for j in range(i + 1, len(numeric_cols)):
            col1, col2 = numeric_cols[i], numeric_cols[j]
            new_features[f'Alpha_{col1}_x_{col2}'] = X[col1] * X[col2]
    for i in range(len(bool_cols)):
        for j in range(i + 1, len(bool_cols)):
            col1, col2 = bool_cols[i], bool_cols[j]
            new_features[f'Alpha_{col1}_AND_{col2}'] = ((X[col1] > 0).astype(int) & (X[col2] > 0).astype(int)).astype(int)

    if new_features.empty:
        return None

    X_new = new_features.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    mi_scores = mutual_info_classif(X_new, y, random_state=42)
    mi_series = pd.Series(mi_scores, index=X_new.columns).sort_values(ascending=False)
    top = mi_series.head(int(top_n))
    candidate_id = now_str().replace(':', '').replace('-', '').replace(' ', '_')
    area = 'alpha_candidates'
    csv_out = _LAB.area(area) / f'alpha_candidates_{candidate_id}.csv'
    json_out = _LAB.area(area) / f'alpha_candidates_{candidate_id}.json'
    top_df = top.reset_index()
    top_df.columns = ['feature_name', 'mutual_info_score']
    top_df.to_csv(csv_out, index=False, encoding='utf-8-sig')
    payload = {
        'candidate_id': candidate_id,
        'generated_at': now_str(),
        'csv_file': str(csv_path),
        'candidate_count': int(len(top_df)),
        'top_candidates': top_df.to_dict('records'),
        'writes_production_features': False,
        'note': 'research alpha candidates only; requires validation/OOT/promotion before production',
        'artifact_path': str(json_out),
    }
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    _LAB.append_registry(area, {
        'candidate_id': candidate_id,
        'generated_at': payload['generated_at'],
        'candidate_count': int(len(top_df)),
        'artifact_path': str(json_out),
        'status': 'candidate_only_not_live',
    })
    return payload


def evaluate_alpha_candidate(payload: dict[str, Any] | None = None, min_oot_acc: float = 0.50) -> dict[str, Any] | None:
    payload = payload or _load_latest_candidate_payload()
    if not payload:
        return None
    csv_path = Path(payload.get('csv_file', PATHS.data_dir / 'ml_training_data.csv'))
    if not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    if 'Label_Y' not in df.columns:
        return None
    top = payload.get('top_candidates', [])[:10]
    feature_names = [row.get('feature_name') for row in top if row.get('feature_name')]
    if not feature_names:
        return None

    # rebuild derived features from candidate names
    drop_cols = {'Ticker', 'Ticker SYMBOL', 'Date', 'Setup', 'Regime', 'Label_Y', 'Target_Return'}
    base_X = df[[c for c in df.columns if c not in drop_cols]].apply(pd.to_numeric, errors='coerce').fillna(0.0)
    X = pd.DataFrame(index=base_X.index)
    for name in feature_names:
        if '_x_' in name:
            _, c1, c2 = name.split('Alpha_', 1)[1].split('_x_')[0], None, None
        if name.startswith('Alpha_') and '_x_' in name:
            body = name[len('Alpha_'):]
            c1, c2 = body.split('_x_', 1)
            if c1 in base_X.columns and c2 in base_X.columns:
                X[name] = base_X[c1] * base_X[c2]
        elif name.startswith('Alpha_') and '_AND_' in name:
            body = name[len('Alpha_'):]
            c1, c2 = body.split('_AND_', 1)
            if c1 in base_X.columns and c2 in base_X.columns:
                X[name] = ((base_X[c1] > 0).astype(int) & (base_X[c2] > 0).astype(int)).astype(int)
    if X.empty:
        return None
    y = pd.to_numeric(df['Label_Y'], errors='coerce').fillna(0).astype(int)
    split_idx = max(int(len(df) * 0.8), len(df) - 120)
    split_idx = min(max(split_idx, 80), max(len(df) - 20, 80))
    X_train, X_oot = X.iloc[:split_idx].fillna(0.0), X.iloc[split_idx:].fillna(0.0)
    y_train, y_oot = y.iloc[:split_idx], y.iloc[split_idx:]
    if len(X_train) < 80 or len(X_oot) < 20 or y_train.nunique() < 2 or y_oot.nunique() < 1:
        return None
    model = LogisticRegression(max_iter=500, class_weight='balanced')
    model.fit(X_train, y_train)
    pred = model.predict(X_oot)
    oot_acc = float(accuracy_score(y_oot, pred)) if len(y_oot) else 0.0
    eval_payload = {
        'candidate_id': payload.get('candidate_id'),
        'generated_at': now_str(),
        'oot_accuracy': oot_acc,
        'feature_count': int(X.shape[1]),
        'min_oot_acc': float(min_oot_acc),
        'promotion_ready': oot_acc >= float(min_oot_acc),
        'status': 'alpha_candidate_ready' if oot_acc >= float(min_oot_acc) else 'alpha_candidate_blocked',
    }
    out = _LAB.area('alpha_candidates') / f'alpha_eval_{payload.get("candidate_id")}.json'
    out.write_text(json.dumps(eval_payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return eval_payload


def auto_approve_latest_alpha_candidate(approver: str = 'auto') -> dict[str, Any] | None:
    payload = _load_latest_candidate_payload()
    if not payload:
        return None
    evaluation = evaluate_alpha_candidate(payload)
    if not evaluation or not evaluation.get('promotion_ready'):
        return evaluation
    approved = {
        'approved_at': now_str(),
        'approved_by': approver,
        'candidate_id': payload.get('candidate_id'),
        'feature_names': [row.get('feature_name') for row in payload.get('top_candidates', [])[:10] if row.get('feature_name')],
        'evaluation': evaluation,
        'status': 'approved_alpha_snapshot_only',
        'live_effect': 'none_until_feature_snapshot_is_manually_promoted',
    }
    out = _LAB.area('alpha_candidates') / f'approved_alpha_{payload.get("candidate_id")}.json'
    out.write_text(json.dumps(approved, ensure_ascii=False, indent=2), encoding='utf-8')
    _LAB.append_registry('alpha_candidates', {
        'candidate_id': payload.get('candidate_id'),
        'generated_at': approved['approved_at'],
        'artifact_path': str(out),
        'status': approved['status'],
    })
    return approved


if __name__ == '__main__':
    print(run_alpha_forge())
