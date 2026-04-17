# -*- coding: utf-8 -*-
from __future__ import annotations

"""Formal feature ablation service.

This is a true leave-one-feature-out governance step.  It retrains a compact
model on the training window, evaluates on out-of-time data, then repeats after
removing one feature at a time.  Features that do not survive OOT ablation are
moved to review/watchlist instead of being promoted just because correlation or
proxy scores looked good.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        data_dir = Path('data')
        runtime_dir = Path('runtime')
        model_dir = Path('models')
        base_dir = Path('.')
    PATHS = _Paths()

try:
    from config import PARAMS  # type: ignore
except Exception:  # pragma: no cover
    PARAMS = {}

META_COLS = {
    'Ticker', 'Ticker SYMBOL', 'Date', '日期', 'Setup', 'Setup_Tag', 'Regime', 'Direction', 'Action',
    'Label', 'Label_Y', 'Target_Return', 'Target_Return_Unit', 'Future_Return_Pct', 'Entry_Date', 'Exit_Date',
    'Entry_Price', 'Exit_Price', 'Sample_Type', 'Position_Date', 'Is_Position_Day', 'Current_Close',
    'Exit_Defend_Label', 'Exit_Reduce_Label', 'Exit_Confirm_Label', 'Setup_Ready_Label', 'Trigger_Confirm_Label',
}


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == '' or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


class FeatureAblationService:
    MODULE_VERSION = 'v20260417_full_leave_one_feature_out_ablation'

    def __init__(self) -> None:
        self.runtime_path = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'feature_ablation_report.json'
        self.policy_path = Path(getattr(PATHS, 'model_dir', Path('models'))) / 'feature_ablation_approved.json'

    def _candidate_training_files(self) -> list[Path]:
        return [
            Path(getattr(PATHS, 'data_dir', Path('data'))) / 'ml_training_data.csv',
            Path(getattr(PATHS, 'data_dir', Path('data'))) / 'training_dataset.csv',
            Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'ml_training_data.csv',
            Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'training_dataset.csv',
            Path(getattr(PATHS, 'base_dir', Path('.'))) / 'ml_training_data.csv',
        ]

    def _load_training_frame(self) -> tuple[Path | None, pd.DataFrame]:
        for path in self._candidate_training_files():
            if not path.exists():
                continue
            try:
                df = pd.read_csv(path, low_memory=False)
                if not df.empty:
                    return path, df
            except Exception:
                continue
        return None, pd.DataFrame()

    def _normalize_target(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
        if 'Target_Return' not in df.columns:
            return df, {'status': 'blocked', 'reason': 'target_return_missing'}
        ret = pd.to_numeric(df['Target_Return'], errors='coerce')
        finite = np.isfinite(ret.astype(float))
        if finite.any() and float(ret[finite].abs().quantile(0.95)) > 0.8 and float(ret[finite].abs().max()) <= 100:
            ret = ret / 100.0
        out = df.loc[finite].copy()
        out['Target_Return'] = ret.loc[finite].astype(float)
        if 'Label_Y' not in out.columns:
            out['Label_Y'] = (out['Target_Return'] > 0).astype(int)
        else:
            out['Label_Y'] = pd.to_numeric(out['Label_Y'], errors='coerce').fillna((out['Target_Return'] > 0).astype(int)).astype(int)
        return out, {'status': 'ok', 'rows_after': int(len(out))}

    @staticmethod
    def _chronological_split(df: pd.DataFrame, oot_ratio: float = 0.2) -> tuple[pd.DataFrame, pd.DataFrame]:
        if 'Date' in df.columns:
            df = df.sort_values('Date')
        elif '日期' in df.columns:
            df = df.sort_values('日期')
        df = df.reset_index(drop=True)
        n = len(df)
        oot_n = max(40, int(n * oot_ratio))
        oot_n = min(max(1, n // 3), oot_n) if n > 120 else max(20, n // 4)
        return df.iloc[:-oot_n].copy(), df.iloc[-oot_n:].copy()

    def _candidate_features(self, df: pd.DataFrame, limit: int) -> list[str]:
        approved_path = Path(getattr(PATHS, 'model_dir', Path('models'))) / 'approved_features_review.json'
        features: list[str] = []
        if approved_path.exists():
            try:
                data = json.loads(approved_path.read_text(encoding='utf-8'))
                features.extend([str(x) for x in data.get('approved_features', []) if str(x) in df.columns])
            except Exception:
                pass
        selected_pkl = Path(getattr(PATHS, 'model_dir', Path('models'))) / 'selected_features.pkl'
        if selected_pkl.exists():
            try:
                features.extend([str(x) for x in joblib.load(selected_pkl) if str(x) in df.columns])
            except Exception:
                pass
        if not features:
            numeric = []
            for col in df.columns:
                if col in META_COLS:
                    continue
                s = pd.to_numeric(df[col], errors='coerce')
                if s.notna().mean() >= 0.7 and s.nunique(dropna=True) >= 3:
                    try:
                        corr = abs(float(s.corr(pd.to_numeric(df['Target_Return'], errors='coerce'))))
                    except Exception:
                        corr = 0.0
                    numeric.append((corr if np.isfinite(corr) else 0.0, col))
            numeric.sort(reverse=True)
            features = [c for _, c in numeric]
        return list(dict.fromkeys([f for f in features if f in df.columns and f not in META_COLS]))[:max(1, int(limit))]

    @staticmethod
    def _fit(X: pd.DataFrame, y: pd.Series) -> RandomForestClassifier:
        model = RandomForestClassifier(
            n_estimators=int(PARAMS.get('ABLATION_N_ESTIMATORS', min(int(PARAMS.get('MODEL_N_ESTIMATORS', 160)), 160))),
            max_depth=int(PARAMS.get('ABLATION_MAX_DEPTH', min(int(PARAMS.get('MODEL_MAX_DEPTH', 7)), 6))),
            min_samples_leaf=int(PARAMS.get('MODEL_MIN_SAMPLES_LEAF', 3)),
            random_state=123,
            class_weight='balanced',
        )
        model.fit(X, y)
        return model

    @staticmethod
    def _metrics(model: RandomForestClassifier, X: pd.DataFrame, y: pd.Series, ret: pd.Series) -> dict[str, float]:
        pred = model.predict(X)
        strategy_ret = np.where(pred == 1, pd.to_numeric(ret, errors='coerce').fillna(0.0).values, 0.0)
        gp = float(strategy_ret[strategy_ret > 0].sum()) if np.any(strategy_ret > 0) else 0.0
        gl = float(abs(strategy_ret[strategy_ret < 0].sum())) if np.any(strategy_ret < 0) else 0.0
        pf = gp / gl if gl > 1e-12 else (99.9 if gp > 0 else 0.0)
        return {
            'avg_return': float(np.mean(strategy_ret)) if len(strategy_ret) else 0.0,
            'hit_rate': float(np.mean(strategy_ret > 0)) if len(strategy_ret) else 0.0,
            'profit_factor': float(pf),
            'coverage': float(np.mean(pred == 1)) if len(pred) else 0.0,
            'pred_accuracy': float(accuracy_score(y, pred)) if len(y) else 0.0,
        }

    @staticmethod
    def _score(metrics: dict[str, float]) -> float:
        return float(metrics.get('avg_return', 0.0) * 100.0 + min(metrics.get('profit_factor', 0.0), 5.0) + metrics.get('hit_rate', 0.0) * 10.0)

    def build(self, features: list[str] | None = None) -> tuple[Path, dict[str, Any]]:
        data_path, df = self._load_training_frame()
        generated_at = _now()
        if df.empty:
            payload = {'generated_at': generated_at, 'module_version': self.MODULE_VERSION, 'status': 'blocked_training_data_missing', 'training_data_path': str(data_path) if data_path else None}
            _write_json(self.runtime_path, payload)
            _write_json(self.policy_path, {'generated_at': generated_at, 'status': 'blocked_training_data_missing', 'ablation_approved_features': [], 'ablation_rejected_features': []})
            return self.runtime_path, payload
        df, target_report = self._normalize_target(df)
        min_rows = int(PARAMS.get('ABLATION_MIN_ROWS', max(160, int(PARAMS.get('MODEL_MIN_TRAIN_ROWS', 80)) * 2)))
        if target_report.get('status') != 'ok' or len(df) < min_rows or df['Label_Y'].nunique() < 2:
            payload = {'generated_at': generated_at, 'module_version': self.MODULE_VERSION, 'status': 'blocked_insufficient_ablation_data', 'training_data_path': str(data_path), 'target_report': target_report, 'row_count': int(len(df)), 'min_rows': min_rows, 'label_unique': int(df['Label_Y'].nunique()) if 'Label_Y' in df else 0}
            _write_json(self.runtime_path, payload)
            _write_json(self.policy_path, {'generated_at': generated_at, 'status': payload['status'], 'ablation_approved_features': [], 'ablation_rejected_features': []})
            return self.runtime_path, payload

        max_features = int(PARAMS.get('ABLATION_MAX_FEATURES', 24))
        candidate_features = list(dict.fromkeys([f for f in (features or self._candidate_features(df, max_features)) if f in df.columns and f not in META_COLS]))[:max_features]
        if len(candidate_features) < int(PARAMS.get('ABLATION_MIN_FEATURES', 4)):
            payload = {'generated_at': generated_at, 'module_version': self.MODULE_VERSION, 'status': 'blocked_too_few_ablation_features', 'candidate_features': candidate_features}
            _write_json(self.runtime_path, payload)
            _write_json(self.policy_path, {'generated_at': generated_at, 'status': payload['status'], 'ablation_approved_features': [], 'ablation_rejected_features': []})
            return self.runtime_path, payload

        train_df, oot_df = self._chronological_split(df, oot_ratio=float(PARAMS.get('OOT_RATIO', 0.2)))
        X_train = train_df[candidate_features].apply(pd.to_numeric, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0.0)
        y_train = pd.to_numeric(train_df['Label_Y'], errors='coerce').fillna(0).astype(int)
        X_oot = oot_df[candidate_features].apply(pd.to_numeric, errors='coerce').replace([np.inf, -np.inf], np.nan).fillna(0.0)
        y_oot = pd.to_numeric(oot_df['Label_Y'], errors='coerce').fillna(0).astype(int)
        ret_oot = pd.to_numeric(oot_df['Target_Return'], errors='coerce').fillna(0.0)
        if len(X_train) < 80 or y_train.nunique() < 2 or len(X_oot) < 20:
            payload = {'generated_at': generated_at, 'module_version': self.MODULE_VERSION, 'status': 'blocked_train_oot_split_insufficient', 'rows_train': int(len(X_train)), 'rows_oot': int(len(X_oot)), 'label_unique_train': int(y_train.nunique())}
            _write_json(self.runtime_path, payload)
            return self.runtime_path, payload

        base_model = self._fit(X_train, y_train)
        base_metrics = self._metrics(base_model, X_oot, y_oot, ret_oot)
        base_score = self._score(base_metrics)
        rows: list[dict[str, Any]] = []
        min_positive = float(PARAMS.get('ABLATION_MIN_POSITIVE_SCORE_IMPACT', -0.10))
        hard_negative = float(PARAMS.get('ABLATION_REJECT_IF_NEGATIVE_IMPACT_BELOW', -0.75))
        for feature in candidate_features:
            reduced = [f for f in candidate_features if f != feature]
            if len(reduced) < 2:
                continue
            try:
                model = self._fit(X_train[reduced], y_train)
                metrics = self._metrics(model, X_oot[reduced], y_oot, ret_oot)
                reduced_score = self._score(metrics)
                # Positive impact means keeping the feature helps base score vs removing it.
                contribution = float(base_score - reduced_score)
                status = 'approved'
                if contribution < hard_negative:
                    status = 'rejected_negative_ablation'
                elif contribution < min_positive:
                    status = 'watchlist_weak_ablation'
                rows.append({
                    'feature': feature,
                    'status': status,
                    'base_score': round(base_score, 6),
                    'score_without_feature': round(reduced_score, 6),
                    'ablation_contribution_score': round(contribution, 6),
                    'metrics_without_feature': metrics,
                })
            except Exception as exc:
                rows.append({'feature': feature, 'status': 'ablation_error_review', 'error': repr(exc), 'base_score': round(base_score, 6)})
        rows.sort(key=lambda r: (r.get('status') != 'approved', -float(r.get('ablation_contribution_score', -999.0)), r.get('feature', '')))
        approved = [r['feature'] for r in rows if r.get('status') == 'approved']
        rejected = [r['feature'] for r in rows if str(r.get('status')).startswith('rejected')]
        watchlist = [r['feature'] for r in rows if 'watchlist' in str(r.get('status')) or 'error' in str(r.get('status'))]
        payload = {
            'generated_at': generated_at,
            'module_version': self.MODULE_VERSION,
            'status': 'feature_ablation_ready' if approved else 'blocked_no_ablation_survivors',
            'training_data_path': str(data_path),
            'rows_train': int(len(train_df)),
            'rows_oot': int(len(oot_df)),
            'candidate_feature_count': int(len(candidate_features)),
            'base_metrics': base_metrics,
            'base_score': round(base_score, 6),
            'approval_policy': {
                'method': 'leave_one_feature_out_retrain_on_train_evaluate_on_oot',
                'min_positive_score_impact': min_positive,
                'reject_if_negative_impact_below': hard_negative,
                'max_features_evaluated': max_features,
            },
            'ablation_approved_features': approved,
            'ablation_rejected_features': rejected,
            'ablation_watchlist_features': watchlist,
            'ablation_rows': rows[:1000],
        }
        _write_json(self.runtime_path, payload)
        _write_json(self.policy_path, {k: payload[k] for k in ['generated_at', 'module_version', 'status', 'ablation_approved_features', 'ablation_rejected_features', 'ablation_watchlist_features', 'approval_policy']})
        return self.runtime_path, payload


def main() -> int:
    path, payload = FeatureAblationService().build()
    print(f'🧪 特徵 ablation 完成：{path} | status={payload.get("status")} approved={len(payload.get("ablation_approved_features", []))}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
