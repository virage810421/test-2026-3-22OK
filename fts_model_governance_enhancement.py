# -*- coding: utf-8 -*-
from __future__ import annotations

"""模型治理補強 v92：walk-forward / OOS / promotion / retention / drift."""

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fts_config import PATHS
from config import PARAMS


class ModelGovernanceEnhancement:
    MODULE_VERSION = 'v92_walkforward_oos_drift_retention_governance'

    def __init__(self) -> None:
        self.path = PATHS.runtime_dir / 'model_governance_enhancement.json'
        self.retired_dir = PATHS.model_dir / 'retired_models'

    def _load_json(self, p: Path) -> dict[str, Any]:
        if not p.exists():
            return {}
        try:
            data = json.loads(p.read_text(encoding='utf-8'))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _load_training_data(self) -> tuple[Path | None, pd.DataFrame]:
        for p in [PATHS.data_dir / 'ml_training_data.csv', PATHS.data_dir / 'training_dataset.csv', PATHS.runtime_dir / 'ml_training_data.csv']:
            if p.exists():
                try:
                    df = pd.read_csv(p)
                    if not df.empty:
                        return p, df
                except Exception:
                    continue
        return None, pd.DataFrame()

    @staticmethod
    def _simple_feature_drift(df: pd.DataFrame) -> dict[str, Any]:
        if df.empty or len(df) < 100:
            return {'status': 'insufficient_rows_for_drift', 'row_count': int(len(df))}
        if 'Date' in df.columns:
            try:
                df = df.sort_values('Date')
            except Exception:
                pass
        split = max(30, int(len(df) * 0.7))
        old = df.iloc[:split]
        recent = df.iloc[split:]
        rows = []
        meta = {'Target_Return', 'Label', 'Label_Y', 'Date', 'Ticker', 'Ticker SYMBOL'}
        for col in df.columns:
            if col in meta:
                continue
            x_old = pd.to_numeric(old[col], errors='coerce').replace([np.inf, -np.inf], np.nan).dropna()
            x_new = pd.to_numeric(recent[col], errors='coerce').replace([np.inf, -np.inf], np.nan).dropna()
            if len(x_old) < 30 or len(x_new) < 10:
                continue
            std = float(x_old.std() or 0.0)
            if std <= 1e-12:
                continue
            z = float(abs((x_new.mean() - x_old.mean()) / std))
            if z >= 1.0:
                rows.append({'feature': col, 'mean_shift_z': round(z, 4), 'old_mean': round(float(x_old.mean()), 6), 'recent_mean': round(float(x_new.mean()), 6)})
        rows.sort(key=lambda r: -r['mean_shift_z'])
        return {'status': 'drift_scored', 'high_drift_count': sum(1 for r in rows if r['mean_shift_z'] >= 2.0), 'top_drift_features': rows[:50]}

    def _stale_model_report(self, promoted_version: str | None = None) -> dict[str, Any]:
        now = datetime.now()
        rows = []
        protected = {'selected_features.pkl', 'model_趨勢多頭.pkl', 'model_區間盤整.pkl', 'model_趨勢空頭.pkl'}
        for p in PATHS.model_dir.glob('*.pkl'):
            try:
                age_days = (now - datetime.fromtimestamp(p.stat().st_mtime)).days
            except Exception:
                age_days = 9999
            is_active_name = p.name in protected or p.name.startswith(('model_long_', 'model_short_', 'model_range_', 'selected_features_long', 'selected_features_short', 'selected_features_range'))
            rows.append({'file': p.name, 'age_days': age_days, 'size': p.stat().st_size if p.exists() else 0, 'active_name': is_active_name})
        stale = [r for r in rows if r['age_days'] > int(PARAMS.get('MODEL_RETIRE_STALE_AFTER_DAYS', 45)) and not r['active_name']]
        moved = []
        if bool(PARAMS.get('MODEL_AUTO_RETIRE_STALE_ARTIFACTS', False)) and stale:
            self.retired_dir.mkdir(parents=True, exist_ok=True)
            for r in stale:
                src = PATHS.model_dir / r['file']
                dst = self.retired_dir / f"{now.strftime('%Y%m%d_%H%M%S')}_{r['file']}"
                try:
                    shutil.move(str(src), str(dst))
                    moved.append({'from': str(src), 'to': str(dst)})
                except Exception as exc:
                    moved.append({'from': str(src), 'error': repr(exc)})
        return {'model_file_count': len(rows), 'stale_model_count': len(stale), 'stale_models': stale[:100], 'auto_retire_enabled': bool(PARAMS.get('MODEL_AUTO_RETIRE_STALE_ARTIFACTS', False)), 'retired_moves': moved}

    def build(self) -> tuple[Path, dict[str, Any]]:
        backend = self._load_json(PATHS.runtime_dir / 'trainer_backend_report.json')
        live_gate = self._load_json(PATHS.runtime_dir / 'model_live_signal_gate.json')
        data_path, df = self._load_training_data()
        wf = backend.get('walk_forward_summary', {}) if backend else {}
        oot = backend.get('out_of_time', {}) if backend else {}
        promotion = backend.get('promotion', {}) if backend else {}
        hard_blocks = []
        warnings = []
        if not backend:
            hard_blocks.append('trainer_backend_report_missing')
        if backend and int(wf.get('effective_splits', 0) or 0) < int(PARAMS.get('MODEL_MIN_WF_EFFECTIVE_SPLITS', 3)):
            hard_blocks.append('walk_forward_effective_splits_below_floor')
        if backend and not bool(oot):
            hard_blocks.append('out_of_time_report_missing')
        if backend and not bool(live_gate.get('allow_live_signal', False)):
            warnings.append('live_signal_gate_not_allowed')
        if backend and promotion.get('status') not in {'promoted_best'}:
            warnings.append('current_candidate_not_promoted')
        drift = self._simple_feature_drift(df)
        if drift.get('high_drift_count', 0) > 0:
            warnings.append('high_feature_drift_detected_review_before_live')
        stale = self._stale_model_report(str(promotion.get('version') or ''))
        if stale.get('stale_model_count', 0) > 0:
            warnings.append('stale_model_files_present_review_or_enable_retirement')
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'module_version': self.MODULE_VERSION,
            'status': 'blocked' if hard_blocks else 'model_governance_enhanced_ready',
            'training_data_path': str(data_path) if data_path else None,
            'walk_forward': wf,
            'out_of_time': oot,
            'promotion': promotion,
            'live_signal_gate': live_gate,
            'drift_monitor': drift,
            'model_retention': stale,
            'hard_blocks': hard_blocks,
            'warnings': warnings,
            'required_before_live': [
                'trainer_backend_report exists',
                'walk_forward_effective_splits >= floor',
                'out_of_time report present',
                'model_live_signal_gate allow_live_signal true for live',
                'stale model files reviewed or retired',
                'drift monitor reviewed when high_drift_count > 0',
            ],
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.path, payload


def main() -> int:
    path, payload = ModelGovernanceEnhancement().build()
    print(f'🧠 模型治理補強完成：{path} | status={payload.get("status")}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
