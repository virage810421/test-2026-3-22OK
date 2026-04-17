# -*- coding: utf-8 -*-
from __future__ import annotations

"""正式特徵審核服務 v93.

升級點：
1. 缺訓練資料時輸出 fail-closed policy，不再讓 trainer 默默跳過審核。
2. 加入 train/live parity 檢查；正式 live 訓練預設只允許 live-approved 或明確 priority 的特徵。
3. 加入 stability / regime persistence / cross-period survival 指標。
4. 接上真正 leave-one-feature-out ablation retrain governance，不再只靠 proxy。
5. 產出 approved/rejected/noise/watchlist 清單供 fts_trainer_backend 強制使用。
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fts_config import PATHS
from config import PARAMS
from fts_feature_catalog import PRIORITY_NEW_FEATURES_20, is_feature_live_approved


META_COLS = {
    'Ticker', 'Ticker SYMBOL', 'Date', '日期', 'Setup', 'Setup_Tag', 'Regime', 'Direction',
    'Label', 'Label_Y', 'Target_Return', 'Target_Return_Unit', 'Future_Return_Pct',
    'Entry_Date', 'Exit_Date', 'Entry_Price', 'Exit_Price', 'Sample_Type', 'Position_Date',
    'Exit_Defend_Label', 'Exit_Reduce_Label', 'Exit_Confirm_Label', 'Setup_Ready_Label', 'Trigger_Confirm_Label',
}


class FeatureReviewService:
    MODULE_VERSION = 'v20260417_feature_review_full_ablation_governance'

    def __init__(self) -> None:
        self.runtime_path = PATHS.runtime_dir / 'feature_review_report.json'
        self.approved_path = PATHS.model_dir / 'approved_features_review.json'
        self.runtime_approved_path = PATHS.runtime_dir / 'approved_features_review.json'

    def _candidate_training_files(self) -> list[Path]:
        return [
            PATHS.data_dir / 'ml_training_data.csv',
            PATHS.data_dir / 'training_dataset.csv',
            PATHS.runtime_dir / 'ml_training_data.csv',
            PATHS.runtime_dir / 'training_dataset.csv',
            PATHS.base_dir / 'ml_training_data.csv',
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

    @staticmethod
    def _target_series(df: pd.DataFrame) -> pd.Series | None:
        for name in ['Target_Return', 'Label_Y', 'Label']:
            if name in df.columns:
                s = pd.to_numeric(df[name], errors='coerce')
                if s.notna().sum() > 0:
                    if name == 'Target_Return' and float(s.abs().quantile(0.95)) > 0.8 and float(s.abs().max()) <= 100:
                        s = s / 100.0
                    return s
        return None

    @staticmethod
    def _time_folds(df: pd.DataFrame, n_folds: int = 4) -> pd.Series:
        if 'Date' in df.columns:
            dt = pd.to_datetime(df['Date'], errors='coerce')
        elif '日期' in df.columns:
            dt = pd.to_datetime(df['日期'], errors='coerce')
        else:
            dt = pd.Series(pd.RangeIndex(len(df)), index=df.index)
        rank = dt.rank(method='first') if hasattr(dt, 'rank') else pd.Series(range(len(df)), index=df.index)
        try:
            return pd.qcut(rank, q=min(n_folds, max(1, rank.notna().sum())), labels=False, duplicates='drop')
        except Exception:
            return pd.Series(np.arange(len(df)) % max(1, n_folds), index=df.index)

    @staticmethod
    def _regime_series(df: pd.DataFrame) -> pd.Series:
        for key in ('Regime', 'Regime_Label', '市場狀態'):
            if key in df.columns:
                return df[key].astype(str).fillna('UNKNOWN')
        return pd.Series(['ALL'] * len(df), index=df.index)

    def _fold_metrics(self, series: pd.Series, target: pd.Series | None, fold_id: pd.Series, regime: pd.Series, min_unique: int) -> dict[str, Any]:
        fold_corrs: list[float] = []
        if target is not None and len(target) == len(series):
            for fid in sorted(pd.Series(fold_id).dropna().unique().tolist()):
                mask = fold_id == fid
                pair = pd.DataFrame({'x': pd.to_numeric(series[mask], errors='coerce'), 'y': pd.to_numeric(target[mask], errors='coerce')}).replace([np.inf, -np.inf], np.nan).dropna()
                if len(pair) >= 20 and pair['x'].nunique() >= min_unique and pair['y'].nunique() >= 2:
                    corr = float(pair['x'].corr(pair['y']))
                    if np.isfinite(corr):
                        fold_corrs.append(corr)
        abs_corrs = [abs(x) for x in fold_corrs]
        stability_score = 0.0
        survival_score = 0.0
        sign_consistency = 0.0
        if abs_corrs:
            mean_abs = float(np.mean(abs_corrs))
            std_abs = float(np.std(abs_corrs))
            stability_score = max(0.0, min(1.0, mean_abs / max(mean_abs + std_abs, 1e-9)))
            survival_score = float(np.mean([1.0 if x >= float(PARAMS.get('FEATURE_REVIEW_SURVIVAL_MIN_ABS_CORR', 0.01)) else 0.0 for x in abs_corrs]))
            signs = [np.sign(x) for x in fold_corrs if abs(x) > 1e-12]
            if signs:
                sign_consistency = float(max(sum(1 for s in signs if s > 0), sum(1 for s in signs if s < 0)) / len(signs))
        regime_persistence = 0.0
        regime_scores: list[float] = []
        if target is not None and len(target) == len(series):
            for rg in sorted(pd.Series(regime).dropna().astype(str).unique().tolist()):
                mask = regime.astype(str) == str(rg)
                pair = pd.DataFrame({'x': pd.to_numeric(series[mask], errors='coerce'), 'y': pd.to_numeric(target[mask], errors='coerce')}).replace([np.inf, -np.inf], np.nan).dropna()
                if len(pair) >= 20 and pair['x'].nunique() >= min_unique and pair['y'].nunique() >= 2:
                    corr = float(pair['x'].corr(pair['y']))
                    if np.isfinite(corr):
                        regime_scores.append(abs(corr))
            if regime_scores:
                regime_persistence = float(np.mean([1.0 if x >= float(PARAMS.get('FEATURE_REVIEW_REGIME_MIN_ABS_CORR', 0.01)) else 0.0 for x in regime_scores]))
        return {
            'fold_corrs': [round(float(x), 6) for x in fold_corrs[:8]],
            'stability_score': round(float(stability_score), 6),
            'cross_period_survival': round(float(survival_score), 6),
            'regime_persistence': round(float(regime_persistence), 6),
            'sign_consistency': round(float(sign_consistency), 6),
        }

    def _score_features(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        target = self._target_series(df)
        rows: list[dict[str, Any]] = []
        min_coverage = float(PARAMS.get('FEATURE_REVIEW_MIN_COVERAGE', 0.70))
        min_unique = int(PARAMS.get('FEATURE_REVIEW_MIN_UNIQUE', 3))
        min_abs_corr = float(PARAMS.get('FEATURE_REVIEW_MIN_ABS_CORR', 0.01))
        min_stability = float(PARAMS.get('FEATURE_REVIEW_MIN_STABILITY', 0.35))
        min_survival = float(PARAMS.get('FEATURE_REVIEW_MIN_SURVIVAL', 0.50))
        min_regime_persistence = float(PARAMS.get('FEATURE_REVIEW_MIN_REGIME_PERSISTENCE', 0.34))
        enforce_live_parity = bool(PARAMS.get('MODEL_ENFORCE_TRAIN_LIVE_FEATURE_PARITY', True))
        allow_priority_live_bypass = bool(PARAMS.get('FEATURE_REVIEW_ALLOW_PRIORITY_BYPASS_LIVE_APPROVAL', True))
        priority_set = set(PRIORITY_NEW_FEATURES_20)
        fold_id = self._time_folds(df)
        regime = self._regime_series(df)

        for col in df.columns:
            if col in META_COLS:
                continue
            s = pd.to_numeric(df[col], errors='coerce')
            finite_mask = np.isfinite(s.astype(float)) if len(s) else pd.Series(dtype=bool)
            coverage = float(finite_mask.mean()) if len(s) else 0.0
            unique_count = int(s[finite_mask].nunique()) if len(s) and getattr(finite_mask, 'any', lambda: False)() else 0
            zero_ratio = float((s[finite_mask].abs() <= 1e-12).mean()) if len(s) and getattr(finite_mask, 'any', lambda: False)() else 1.0
            corr = None
            if target is not None and len(target) == len(s) and getattr(finite_mask, 'any', lambda: False)():
                try:
                    pair = pd.DataFrame({'x': s, 'y': target}).replace([np.inf, -np.inf], np.nan).dropna()
                    if len(pair) >= 30 and pair['x'].nunique() >= min_unique and pair['y'].nunique() >= 2:
                        corr = float(pair['x'].corr(pair['y']))
                except Exception:
                    corr = None

            fold_metrics = self._fold_metrics(s, target, fold_id, regime, min_unique)
            stability_score = float(fold_metrics['stability_score'])
            survival_score = float(fold_metrics['cross_period_survival'])
            regime_persistence = float(fold_metrics['regime_persistence'])
            ablation_proxy_score = float(abs(corr or 0.0) * max(stability_score, 0.0) * max(survival_score, 0.0))

            priority = col in priority_set
            live_approved = bool(is_feature_live_approved(col))
            reject_reasons: list[str] = []
            if coverage < min_coverage:
                reject_reasons.append('low_coverage')
            if unique_count < min_unique:
                reject_reasons.append('low_unique_count')
            if zero_ratio >= 0.995:
                reject_reasons.append('almost_all_zero')
            if stability_score < min_stability and not priority:
                reject_reasons.append('stability_too_low')
            if survival_score < min_survival and not priority:
                reject_reasons.append('cross_period_survival_too_low')
            if regime_persistence < min_regime_persistence and not priority:
                reject_reasons.append('regime_persistence_too_low')
            if enforce_live_parity and not live_approved and not (priority and allow_priority_live_bypass):
                reject_reasons.append('not_live_approved_train_live_parity')

            evidence_ok = (corr is not None and abs(corr) >= min_abs_corr) or priority or live_approved
            status = 'approved'
            if reject_reasons:
                if coverage >= min_coverage and unique_count >= min_unique and evidence_ok:
                    status = 'watchlist_noise_candidate'
                else:
                    status = 'reject'
            elif not evidence_ok:
                status = 'watchlist_noise_candidate'

            rows.append({
                'feature': col,
                'status': status,
                'coverage': round(coverage, 6),
                'unique_count': unique_count,
                'zero_ratio': round(zero_ratio, 6),
                'corr_to_target': None if corr is None else round(float(corr), 6),
                'abs_corr_to_target': None if corr is None else round(abs(float(corr)), 6),
                'stability_score': round(stability_score, 6),
                'cross_period_survival': round(survival_score, 6),
                'regime_persistence': round(regime_persistence, 6),
                'ablation_proxy_score': round(ablation_proxy_score, 6),
                'fold_corrs': fold_metrics['fold_corrs'],
                'sign_consistency': fold_metrics['sign_consistency'],
                'priority_feature': priority,
                'live_approved': live_approved,
                'reject_reasons': reject_reasons,
            })
        rows.sort(key=lambda r: (r['status'] != 'approved', -(r.get('ablation_proxy_score') or 0), -(r.get('abs_corr_to_target') or 0), r['feature']))
        return rows

    def _write_policy(self, payload: dict[str, Any], approved: list[str], rejected: list[str], watchlist: list[str]) -> None:
        self.approved_path.parent.mkdir(parents=True, exist_ok=True)
        self.runtime_approved_path.parent.mkdir(parents=True, exist_ok=True)
        payload_text = json.dumps({
            'generated_at': payload.get('generated_at'),
            'module_version': payload.get('module_version'),
            'status': payload.get('status'),
            'approved_features': approved,
            'rejected_features': rejected,
            'noise_watchlist': watchlist,
            'hard_blocks': payload.get('hard_blocks', []),
            'fail_closed': payload.get('status') not in {'feature_review_ready'},
        }, ensure_ascii=False, indent=2)
        self.approved_path.write_text(payload_text, encoding='utf-8')
        self.runtime_approved_path.write_text(payload_text, encoding='utf-8')

    def build(self) -> tuple[Path, dict[str, Any]]:
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)
        self.approved_path.parent.mkdir(parents=True, exist_ok=True)
        generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_path, df = self._load_training_frame()
        if df.empty:
            payload = {
                'generated_at': generated_at,
                'module_version': self.MODULE_VERSION,
                'status': 'blocked_training_data_missing',
                'training_data_path': str(data_path) if data_path else None,
                'approved_feature_count': 0,
                'reject_feature_count': 0,
                'noise_watchlist_count': 0,
                'feature_rows': [],
                'hard_blocks': ['training_data_missing_for_feature_review'],
            }
            self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            self._write_policy(payload, [], [], [])
            return self.runtime_path, payload

        rows = self._score_features(df)
        approved = [r['feature'] for r in rows if r['status'] == 'approved']
        rejected = [r['feature'] for r in rows if r['status'] == 'reject']
        watchlist = [r['feature'] for r in rows if r['status'] == 'watchlist_noise_candidate']

        ablation_gate: dict[str, Any] = {
            'enabled': bool(PARAMS.get('FEATURE_REVIEW_ENABLE_FULL_ABLATION', True)),
            'required': bool(PARAMS.get('FEATURE_REVIEW_REQUIRE_FULL_ABLATION', True)),
            'enforced': False,
            'status': 'not_run',
            'policy_path': '',
        }
        if ablation_gate['enabled'] and approved:
            try:
                from fts_feature_ablation_service import FeatureAblationService
                service = FeatureAblationService()
                ab_path, ab_payload = service.build(features=approved[:int(PARAMS.get('ABLATION_MAX_FEATURES', 24))])
                ablation_gate.update({
                    'status': str(ab_payload.get('status')),
                    'runtime_path': str(ab_path),
                    'policy_path': str(service.policy_path),
                    'base_score': ab_payload.get('base_score'),
                    'candidate_feature_count': ab_payload.get('candidate_feature_count'),
                    'approved_count': len(ab_payload.get('ablation_approved_features', []) or []),
                    'rejected_count': len(ab_payload.get('ablation_rejected_features', []) or []),
                    'watchlist_count': len(ab_payload.get('ablation_watchlist_features', []) or []),
                })
                if ab_payload.get('status') == 'feature_ablation_ready':
                    ablation_gate['enforced'] = True
                    ab_approved = {str(x) for x in ab_payload.get('ablation_approved_features', [])}
                    ab_rejected = {str(x) for x in ab_payload.get('ablation_rejected_features', [])}
                    ab_watch = {str(x) for x in ab_payload.get('ablation_watchlist_features', [])}
                    approved_before = list(approved)
                    approved = [f for f in approved if f in ab_approved]
                    rejected = list(dict.fromkeys(rejected + [f for f in approved_before if f in ab_rejected]))
                    watchlist = list(dict.fromkeys(watchlist + [f for f in approved_before if f in ab_watch or f not in ab_approved]))
                    for r in rows:
                        f = r.get('feature')
                        if f in ab_rejected:
                            r['status'] = 'reject'
                            r.setdefault('reject_reasons', []).append('failed_full_ablation')
                        elif f in ab_watch or (f in approved_before and f not in ab_approved):
                            r['status'] = 'watchlist_noise_candidate'
                            r.setdefault('reject_reasons', []).append('weak_or_missing_full_ablation_survival')
                elif ablation_gate['required']:
                    approved = []
            except Exception as exc:
                ablation_gate.update({'status': 'error', 'error': repr(exc)})
                if ablation_gate['required']:
                    approved = []

        payload = {
            'generated_at': generated_at,
            'module_version': self.MODULE_VERSION,
            'status': 'feature_review_ready',
            'training_data_path': str(data_path),
            'row_count': int(len(df)),
            'feature_count_scored': int(len(rows)),
            'approved_feature_count': int(len(approved)),
            'reject_feature_count': int(len(rejected)),
            'noise_watchlist_count': int(len(watchlist)),
            'approval_policy': {
                'min_coverage': float(PARAMS.get('FEATURE_REVIEW_MIN_COVERAGE', 0.70)),
                'min_unique_count': int(PARAMS.get('FEATURE_REVIEW_MIN_UNIQUE', 3)),
                'min_abs_corr_or_priority': float(PARAMS.get('FEATURE_REVIEW_MIN_ABS_CORR', 0.01)),
                'min_stability': float(PARAMS.get('FEATURE_REVIEW_MIN_STABILITY', 0.35)),
                'min_cross_period_survival': float(PARAMS.get('FEATURE_REVIEW_MIN_SURVIVAL', 0.50)),
                'min_regime_persistence': float(PARAMS.get('FEATURE_REVIEW_MIN_REGIME_PERSISTENCE', 0.34)),
                'train_live_parity_enforced': bool(PARAMS.get('MODEL_ENFORCE_TRAIN_LIVE_FEATURE_PARITY', True)),
                'priority_or_live_approved_can_pass_evidence_floor': True,
                'ablation_policy': 'full_leave_one_feature_out_retrain_enforced_when_ready',
                'full_ablation_required': bool(PARAMS.get('FEATURE_REVIEW_REQUIRE_FULL_ABLATION', True)),
            },
            'ablation_gate': ablation_gate,
            'approved_features': approved[:500],
            'noise_watchlist': watchlist[:500],
            'rejected_features': rejected[:500],
            'feature_rows': rows[:1000],
            'hard_blocks': [] if approved else ['no_approved_features_after_review'],
        }
        if not approved:
            if ablation_gate.get('enabled') and ablation_gate.get('required') and ablation_gate.get('status') != 'feature_ablation_ready':
                payload['status'] = 'blocked_full_ablation_not_ready'
                payload['blocked_reason_category'] = 'full_ablation_not_ready'
                payload['hard_blocks'] = list(dict.fromkeys(payload.get('hard_blocks', []) + ['full_ablation_not_ready']))
            else:
                payload['status'] = 'blocked_no_approved_features'
                payload['blocked_reason_category'] = 'no_approved_features_after_review'
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        self._write_policy(payload, approved, rejected, watchlist)
        return self.runtime_path, payload


def main() -> int:
    path, payload = FeatureReviewService().build()
    print(f'🧪 特徵審核完成：{path} | status={payload.get("status")} approved={payload.get("approved_feature_count")} reject={payload.get("reject_feature_count")}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
