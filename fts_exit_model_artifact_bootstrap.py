# -*- coding: utf-8 -*-
from __future__ import annotations

"""Exit AI artifact bootstrap.

This creates real exit models only when a training dataset with exit labels is
available.  It never creates fake placeholder models; if data is insufficient,
it writes a blocking readiness report.
"""

import json
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str
from fts_exception_policy import record_diagnostic

EXIT_LABELS = {
    'DEFEND': 'Exit_Defend_Label',
    'REDUCE': 'Exit_Reduce_Label',
    'CONFIRM': 'Exit_Confirm_Label',
}
EXIT_MODEL_FILES = {
    'DEFEND': lambda: getattr(CONFIG, 'exit_defend_model_filename', 'exit_model_defend.pkl'),
    'REDUCE': lambda: getattr(CONFIG, 'exit_reduce_model_filename', 'exit_model_reduce.pkl'),
    'CONFIRM': lambda: getattr(CONFIG, 'exit_confirm_model_filename', 'exit_model_confirm.pkl'),
}


def _candidate_training_files() -> list[Path]:
    return [
        PATHS.data_dir / 'ml_training_data.csv',
        PATHS.data_dir / 'training_dataset.csv',
        PATHS.data_dir / 'model_training_dataset.csv',
        PATHS.runtime_dir / 'training_dataset.csv',
    ]


def _numeric_feature_columns(df: Any, labels: set[str]) -> list[str]:
    exclude = set(labels) | {'Target', 'Target_Return', 'Ticker SYMBOL', 'ticker_symbol', 'Date', '日期', 'Entry_Date', 'Exit_Date'}
    cols: list[str] = []
    for c in df.columns:
        if c in exclude:
            continue
        try:
            if str(df[c].dtype).startswith(('float', 'int', 'bool')):
                cols.append(str(c))
        except Exception:
            continue
    preferred = [
        'Exit_Hazard_Score', 'Breakout_Risk_Next3', 'Reversal_Risk_Next3', 'Trend_Exhaustion_Score',
        'AI_Proba', 'Expected_Return', 'Entry_Readiness', 'ATR_Pct', 'RV20', 'Volume_Ratio',
    ]
    ordered = [c for c in preferred if c in cols] + [c for c in cols if c not in preferred]
    return ordered[:80]


class ExitModelArtifactBootstrap:
    def __init__(self):
        self.report_path = PATHS.runtime_dir / 'exit_model_artifact_bootstrap.json'
        self.status_path = PATHS.runtime_dir / str(getattr(CONFIG, 'exit_model_runtime_status_filename', 'exit_model_status.json')).split('/')[-1]

    def build(self) -> tuple[str, dict[str, Any]]:
        report: dict[str, Any] = {'generated_at': now_str(), 'models_dir': str(PATHS.model_dir), 'created': {}, 'missing': [], 'errors': []}
        try:
            import pandas as pd  # type: ignore
            import joblib  # type: ignore
            from sklearn.ensemble import RandomForestClassifier  # type: ignore
            from sklearn.impute import SimpleImputer  # type: ignore
            from sklearn.pipeline import Pipeline  # type: ignore
        except Exception as exc:
            record_diagnostic('exit_model_bootstrap', 'ml_dependencies_unavailable', exc, severity='error', fail_closed=True)
            report.update({'status': 'blocked_ml_dependencies_unavailable', 'error': repr(exc)})
            return self._write(report)
        training_path = next((p for p in _candidate_training_files() if p.exists()), None)
        if training_path is None:
            report.update({'status': 'blocked_missing_training_dataset', 'candidate_files': [str(p) for p in _candidate_training_files()]})
            return self._write(report)
        try:
            df = pd.read_csv(training_path)
        except Exception as exc:
            record_diagnostic('exit_model_bootstrap', 'read_training_dataset_failed', exc, severity='error', fail_closed=True)
            report.update({'status': 'blocked_training_dataset_unreadable', 'training_path': str(training_path), 'error': repr(exc)})
            return self._write(report)
        feature_cols = _numeric_feature_columns(df, set(EXIT_LABELS.values()))
        min_features = int(getattr(CONFIG, 'exit_model_min_features', 6) or 6)
        if len(feature_cols) < min_features:
            report.update({'status': 'blocked_insufficient_exit_features', 'training_path': str(training_path), 'feature_count': len(feature_cols), 'required': min_features})
            return self._write(report)
        PATHS.model_dir.mkdir(parents=True, exist_ok=True)
        selected_path = PATHS.model_dir / str(getattr(CONFIG, 'exit_selected_features_filename', 'selected_features_exit.pkl'))
        joblib.dump(feature_cols, selected_path)
        X = df[feature_cols]
        saved = 0
        for key, label in EXIT_LABELS.items():
            if label not in df.columns:
                report['missing'].append(label)
                continue
            y = pd.to_numeric(df[label], errors='coerce').fillna(0).astype(int)
            if len(set(y.tolist())) < 2 or int(y.sum()) < 3:
                report['missing'].append(f'{label}:insufficient_positive_or_single_class')
                continue
            model = Pipeline([
                ('imputer', SimpleImputer(strategy='median')),
                ('rf', RandomForestClassifier(n_estimators=80, max_depth=5, random_state=42, class_weight='balanced_subsample')),
            ])
            try:
                model.fit(X, y)
                model_path = PATHS.model_dir / str(EXIT_MODEL_FILES[key]())
                joblib.dump(model, model_path)
                report['created'][key] = {'path': str(model_path), 'label': label, 'rows': int(len(y)), 'positive_ratio': float(y.mean())}
                saved += 1
            except Exception as exc:
                record_diagnostic('exit_model_bootstrap', f'train_{key.lower()}_failed', exc, severity='error', fail_closed=True)
                report['errors'].append({'model': key, 'error': repr(exc)})
        report.update({
            'training_path': str(training_path),
            'selected_features_path': str(selected_path),
            'selected_feature_count': len(feature_cols),
            'created_count': saved,
            'status': 'exit_models_bootstrapped' if saved == 3 else 'exit_models_incomplete_block_live',
            'live_exit_ai_ready': saved == 3,
        })
        self._write_status(report)
        return self._write(report)

    def _write_status(self, report: dict[str, Any]) -> None:
        status = {
            'generated_at': now_str(),
            'exit_models_loaded': report.get('created_count') == 3,
            'exit_selected_features_ready': bool(report.get('selected_feature_count', 0) >= int(getattr(CONFIG, 'exit_model_min_features', 6) or 6)),
            'exit_model_source': 'exit_ai_model' if report.get('created_count') == 3 else 'exit_ai_model_unavailable',
            'exit_model_bootstrap_status': report.get('status'),
            'hard_block_when_unavailable': True,
            'fallback_to_hazard': False,
        }
        self.status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

    def _write(self, report: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        self.report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        return str(self.report_path), report


def main(argv: list[str] | None = None) -> int:
    path, payload = ExitModelArtifactBootstrap().build()
    print(json.dumps({'status': payload.get('status'), 'path': path, 'created_count': payload.get('created_count', 0)}, ensure_ascii=False, indent=2))
    return 0 if payload.get('status') == 'exit_models_bootstrapped' else 1


if __name__ == '__main__':
    raise SystemExit(main())
