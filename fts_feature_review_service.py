# -*- coding: utf-8 -*-
from __future__ import annotations

"""正式特徵審核服務 v92.

升級點：
1. 缺訓練資料時輸出 fail-closed policy，不再讓 trainer 默默跳過審核。
2. 加入 train/live parity 檢查；正式 live 訓練預設只允許 live-approved 或明確 priority 的特徵。
3. 產出 approved/rejected/noise/watchlist 清單供 fts_trainer_backend 強制使用。
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fts_config import PATHS
from config import PARAMS
from fts_feature_catalog import FEATURE_SPECS, PRIORITY_NEW_FEATURES_20, is_feature_live_approved


META_COLS = {
    'Ticker', 'Ticker SYMBOL', 'Date', '日期', 'Setup', 'Setup_Tag', 'Regime', 'Direction',
    'Label', 'Label_Y', 'Target_Return', 'Target_Return_Unit', 'Future_Return_Pct',
    'Entry_Date', 'Exit_Date', 'Entry_Price', 'Exit_Price', 'Sample_Type', 'Position_Date',
    'Exit_Defend_Label', 'Exit_Reduce_Label', 'Exit_Confirm_Label', 'Setup_Ready_Label', 'Trigger_Confirm_Label',
}


class FeatureReviewService:
    MODULE_VERSION = 'v92_feature_review_train_live_parity_fail_closed'

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

    def _score_features(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        target = self._target_series(df)
        rows: list[dict[str, Any]] = []
        min_coverage = float(PARAMS.get('FEATURE_REVIEW_MIN_COVERAGE', 0.70))
        min_unique = int(PARAMS.get('FEATURE_REVIEW_MIN_UNIQUE', 3))
        min_abs_corr = float(PARAMS.get('FEATURE_REVIEW_MIN_ABS_CORR', 0.01))
        enforce_live_parity = bool(PARAMS.get('MODEL_ENFORCE_TRAIN_LIVE_FEATURE_PARITY', True))
        allow_priority_live_bypass = bool(PARAMS.get('FEATURE_REVIEW_ALLOW_PRIORITY_BYPASS_LIVE_APPROVAL', True))
        priority_set = set(PRIORITY_NEW_FEATURES_20)

        for col in df.columns:
            if col in META_COLS:
                continue
            s = pd.to_numeric(df[col], errors='coerce')
            finite = np.isfinite(s.astype(float)) if len(s) else pd.Series(dtype=bool)
            coverage = float(finite.mean()) if len(s) else 0.0
            unique_count = int(s[finite].nunique()) if len(s) and finite.any() else 0
            zero_ratio = float((s[finite].abs() <= 1e-12).mean()) if len(s) and finite.any() else 1.0
            corr = None
            if target is not None and len(target) == len(s) and finite.any():
                try:
                    pair = pd.DataFrame({'x': s, 'y': target}).replace([np.inf, -np.inf], np.nan).dropna()
                    if len(pair) >= 30 and pair['x'].nunique() >= min_unique and pair['y'].nunique() >= 2:
                        corr = float(pair['x'].corr(pair['y']))
                except Exception:
                    corr = None

            cataloged = col in FEATURE_SPECS
            live_approved = bool(is_feature_live_approved(col)) if cataloged else False
            priority = col in priority_set
            reject_reasons: list[str] = []
            warn_reasons: list[str] = []
            if coverage < min_coverage:
                reject_reasons.append('coverage_below_floor')
            if unique_count < min_unique:
                reject_reasons.append('low_unique_count')
            if zero_ratio > 0.98:
                reject_reasons.append('almost_all_zero')
            if enforce_live_parity and not live_approved and not (priority and allow_priority_live_bypass):
                reject_reasons.append('not_live_approved_train_live_parity')
            if not cataloged:
                warn_reasons.append('not_in_feature_catalog')

            evidence_ok = (corr is not None and abs(corr) >= min_abs_corr) or priority or live_approved
            status = 'approved'
            if reject_reasons:
                status = 'reject'
            elif not evidence_ok:
                status = 'watchlist_noise_candidate'

            rows.append({
                'feature': col,
                'status': status,
                'coverage': round(coverage, 4),
                'unique_count': unique_count,
                'zero_ratio': round(zero_ratio, 4),
                'corr_to_target': None if corr is None else round(corr, 6),
                'abs_corr_to_target': None if corr is None else round(abs(corr), 6),
                'cataloged': cataloged,
                'live_approved': live_approved,
                'priority_feature': priority,
                'reject_reasons': reject_reasons,
                'warn_reasons': warn_reasons,
            })
        rows.sort(key=lambda r: (r['status'] != 'approved', -(r.get('abs_corr_to_target') or 0), r['feature']))
        return rows

    def _write_policy(self, payload: dict[str, Any], approved: list[str], rejected: list[str], watchlist: list[str]) -> None:
        self.approved_path.parent.mkdir(parents=True, exist_ok=True)
        self.runtime_approved_path.parent.mkdir(parents=True, exist_ok=True)
        policy = {
            'generated_at': payload.get('generated_at'),
            'module_version': self.MODULE_VERSION,
            'source_report': str(self.runtime_path),
            'status': payload.get('status'),
            'fail_closed': payload.get('status') not in {'feature_review_ready'},
            'approved_features': approved,
            'noise_watchlist': watchlist,
            'rejected_features': rejected,
            'train_live_parity_enforced': bool(PARAMS.get('MODEL_ENFORCE_TRAIN_LIVE_FEATURE_PARITY', True)),
        }
        payload_text = json.dumps(policy, ensure_ascii=False, indent=2)
        self.approved_path.write_text(payload_text, encoding='utf-8')
        self.runtime_approved_path.write_text(payload_text, encoding='utf-8')

    def build(self) -> tuple[Path, dict[str, Any]]:
        data_path, df = self._load_training_frame()
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)
        self.approved_path.parent.mkdir(parents=True, exist_ok=True)
        generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if df.empty:
            payload = {
                'generated_at': generated_at,
                'module_version': self.MODULE_VERSION,
                'status': 'blocked_waiting_for_training_data',
                'message': '找不到 ml_training_data.csv；正式特徵審核 fail-closed，trainer 不可繞過噪音淘汰流程。',
                'candidate_files': [str(p) for p in self._candidate_training_files()],
                'blocked_reason_category': 'training_data_missing',
                'approved_feature_count': 0,
                'reject_feature_count': 0,
                'hard_blocks': ['training_data_missing_for_feature_review'],
            }
            self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            self._write_policy(payload, [], [], [])
            return self.runtime_path, payload

        rows = self._score_features(df)
        approved = [r['feature'] for r in rows if r['status'] == 'approved']
        rejected = [r['feature'] for r in rows if r['status'] == 'reject']
        watchlist = [r['feature'] for r in rows if r['status'] == 'watchlist_noise_candidate']
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
                'train_live_parity_enforced': bool(PARAMS.get('MODEL_ENFORCE_TRAIN_LIVE_FEATURE_PARITY', True)),
                'priority_or_live_approved_can_pass_evidence_floor': True,
            },
            'approved_features': approved[:500],
            'noise_watchlist': watchlist[:500],
            'rejected_features': rejected[:500],
            'feature_rows': rows[:1000],
            'hard_blocks': [] if approved else ['no_approved_features_after_review'],
        }
        if not approved:
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
