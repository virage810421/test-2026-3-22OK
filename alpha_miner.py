# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from sklearn.feature_selection import mutual_info_classif  # type: ignore
except Exception:
    mutual_info_classif = None

try:
    from fts_config import PATHS, CONFIG  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        base_dir = Path('.')
        data_dir = Path('data')
        runtime_dir = Path('runtime')
    class _Config:
        enable_directional_alpha_miner = True
    PATHS = _Paths()
    CONFIG = _Config()

try:
    from fts_utils import now_str, log  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')
    def log(msg: str) -> None:
        print(msg)


class AlphaMiner:
    MODULE_VERSION = 'v85_alpha_miner_directional_candidate_only'

    def __init__(self) -> None:
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)
        self.runtime_json = Path(PATHS.runtime_dir) / 'alpha_miner_directional.json'
        self.runtime_csv = Path(PATHS.runtime_dir) / 'alpha_miner_directional.csv'
        self.training_csv = Path(PATHS.data_dir) / 'ml_training_data.csv'

    def _score(self, X: pd.DataFrame, y: pd.Series) -> pd.Series:
        if X.empty or y.empty:
            return pd.Series(dtype=float)
        X2 = X.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        if mutual_info_classif is None:
            return X2.var(axis=0).sort_values(ascending=False)
        vals = mutual_info_classif(X2, y.astype(int), random_state=42)
        return pd.Series(vals, index=X2.columns).sort_values(ascending=False)

    def generate_interactions(self, df: pd.DataFrame) -> pd.DataFrame:
        drop_cols = {'Ticker', 'Ticker SYMBOL', 'Date', 'Setup', 'Regime', 'Label_Y', 'Long_Label_Y', 'Short_Label_Y', 'Range_Label_Y'}
        base_features = [c for c in df.columns if c not in drop_cols]
        numeric_cols = [c for c in base_features if pd.to_numeric(df[c], errors='coerce').notna().mean() > 0.8 and df[c].nunique(dropna=True) > 2][:12]
        bool_cols = [c for c in base_features if df[c].nunique(dropna=True) <= 2][:12]
        out = pd.DataFrame(index=df.index)
        for i in range(len(numeric_cols)):
            for j in range(i + 1, min(i + 4, len(numeric_cols))):
                c1, c2 = numeric_cols[i], numeric_cols[j]
                out[f'Alpha_{c1}_X_{c2}'] = pd.to_numeric(df[c1], errors='coerce').fillna(0.0) * pd.to_numeric(df[c2], errors='coerce').fillna(0.0)
        for i in range(len(bool_cols)):
            for j in range(i + 1, min(i + 4, len(bool_cols))):
                c1, c2 = bool_cols[i], bool_cols[j]
                out[f'Alpha_{c1}_AND_{c2}'] = (pd.to_numeric(df[c1], errors='coerce').fillna(0.0) > 0).astype(int) & (pd.to_numeric(df[c2], errors='coerce').fillna(0.0) > 0).astype(int)
        return out

    def score_candidate_features(self, df: pd.DataFrame, interactions: pd.DataFrame) -> pd.DataFrame:
        rows = []
        if interactions.empty:
            return pd.DataFrame(columns=['feature_name', 'strategy_scope', 'interaction_type', 'approval_bucket', 'oot_long_score', 'oot_short_score', 'oot_range_score'])
        long_y = df.get('Long_Label_Y', df.get('Label_Y', pd.Series(0, index=df.index))).fillna(0)
        short_y = df.get('Short_Label_Y', pd.Series(0, index=df.index)).fillna(0)
        range_y = df.get('Range_Label_Y', pd.Series(0, index=df.index)).fillna(0)
        long_scores = self._score(interactions, long_y)
        short_scores = self._score(interactions, short_y)
        range_scores = self._score(interactions, range_y)
        all_names = list(dict.fromkeys(list(long_scores.index[:20]) + list(short_scores.index[:20]) + list(range_scores.index[:20])))
        for name in all_names:
            ls = float(long_scores.get(name, 0.0))
            ss = float(short_scores.get(name, 0.0))
            rs = float(range_scores.get(name, 0.0))
            scope = 'SHARED'
            if ss > max(ls, rs):
                scope = 'SHORT_ONLY'
            elif rs > max(ls, ss):
                scope = 'RANGE_ONLY'
            elif ls > max(ss, rs):
                scope = 'LONG_ONLY'
            rows.append({
                'feature_name': name,
                'strategy_scope': scope,
                'interaction_type': 'numeric_or_bool_combo',
                'approval_bucket': 'candidate_only',
                'oot_long_score': round(ls, 6),
                'oot_short_score': round(ss, 6),
                'oot_range_score': round(rs, 6),
            })
        return pd.DataFrame(rows).sort_values(['strategy_scope', 'oot_long_score', 'oot_short_score', 'oot_range_score'], ascending=False)

    def mine_alpha_candidates(self) -> tuple[Path, dict[str, Any]]:
        if not self.training_csv.exists():
            payload = {
                'generated_at': now_str(),
                'module_version': self.MODULE_VERSION,
                'status': 'training_csv_missing',
                'path': str(self.training_csv),
            }
            self.runtime_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.runtime_json, payload
        df = pd.read_csv(self.training_csv)
        interactions = self.generate_interactions(df)
        scored = self.score_candidate_features(df, interactions)
        scored.to_csv(self.runtime_csv, index=False, encoding='utf-8-sig')
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'candidate_count': int(len(scored)),
            'top_long_candidates': scored[scored['strategy_scope'].isin(['LONG_ONLY', 'SHARED'])].head(5).to_dict(orient='records'),
            'top_short_candidates': scored[scored['strategy_scope'] == 'SHORT_ONLY'].head(5).to_dict(orient='records'),
            'top_range_candidates': scored[scored['strategy_scope'] == 'RANGE_ONLY'].head(5).to_dict(orient='records'),
            'status': 'alpha_candidates_ready',
        }
        self.runtime_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧪 alpha miner ready: {self.runtime_json}')
        return self.runtime_json, payload


def main() -> int:
    AlphaMiner().mine_alpha_candidates()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
