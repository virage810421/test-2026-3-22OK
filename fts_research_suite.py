# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 4 files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_research_suite.py
# ==============================================================================
"""Consolidated module generated from 4 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_research_suite.py
# ==============================================================================
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchQualityStatsBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_quality_stats.json"

    def build(self, compat_info: dict):
        row_count = compat_info.get("row_count", 0)
        rows_with_ticker = compat_info.get("rows_with_ticker", 0)
        rows_with_action = compat_info.get("rows_with_action", 0)
        rows_with_price = compat_info.get("rows_with_price", 0)

        def ratio(x):
            return round(x / row_count, 4) if row_count else 0

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "row_count": row_count,
            "stats": {
                "ticker_coverage": ratio(rows_with_ticker),
                "action_coverage": ratio(rows_with_action),
                "price_coverage": ratio(rows_with_price),
            },
            "status": "quality_stats_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🔬 已輸出 research quality stats：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_research_suite.py
# ==============================================================================
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class ResearchSelectionRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_selection_registry.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "research_selection_layer": {
                "role": "研究層/選股層候選輸出登錄，不直接覆蓋真倉",
                "current_status": "registered_and_isolated_from_live",
                "isolation_rules": [
                    "candidate params 與 approved params 分離",
                    "candidate features 不覆蓋 models/selected_features.pkl",
                    "alpha 候選需經 validation/OOT/promotion",
                    "研究模組不得直接寫 production config 或正式模型檔"
                ],
                "merged_old_modules": {
                    "research_only": [
                        "advanced_optimizer.py", "optimizer.py", "auto_optimizer.py",
                        "feature_selector.py", "alpha_miner.py"
                    ],
                    "serviceized_into_mainline": [
                        "market_language.py", "kline_cache.py", "param_storage.py"
                    ]
                },
                "artifact_root": str(PATHS.runtime_dir / 'research_lab'),
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔎 已輸出 research selection registry：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_research_suite.py
# ==============================================================================
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchDecisionReportBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_decision_report.json"

    def build(self, compat_info: dict, readiness: dict, research_gate: dict):
        report = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "research_rows": compat_info.get("row_count", 0),
                "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                "rows_with_action": compat_info.get("rows_with_action", 0),
                "rows_with_price": compat_info.get("rows_with_price", 0),
                "signal_count": readiness.get("total_signals", 0),
                "go_for_decision_linkage": research_gate.get("go_for_decision_linkage", False),
                "failure_count": len(research_gate.get("failures", [])),
                "warning_count": len(research_gate.get("warnings", [])),
            },
            "research_gate": research_gate,
            "compat_info": compat_info,
            "readiness": readiness,
            "interpretation": {
                "what_this_means": [
                    "研究/選股輸出是否足以接到 decision / execution",
                    "資料是否至少具備 ticker/action/price",
                    "是否發生 research 有輸出但 signal 轉換為 0 的情況"
                ],
                "next_focus": [
                    "若 failure_count > 0，先修 research 輸出欄位",
                    "若 warning_count > 0，優先檢查 scoring / action mapping / price 欄位"
                ]
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        log(f"📘 已輸出 research decision report：{self.path}")
        return self.path, report


# ==============================================================================
# Merged from: fts_research_suite.py
# ==============================================================================
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchVersioningBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_versioning.json"

    def build(self, compat_info: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "versioning": {
                "research_output_version": getattr(CONFIG, "package_version", "v53"),
                "decision_input_rows": compat_info.get("row_count", 0),
                "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                "rows_with_action": compat_info.get("rows_with_action", 0),
                "rows_with_price": compat_info.get("rows_with_price", 0),
            },
            "required_metadata": [
                "research_output_version",
                "generated_at",
                "decision_input_rows",
            ],
            "status": "versioning_defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🏷️ 已輸出 research versioning：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_research_gate.py
# ==============================================================================
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchQualityGate:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_quality_gate.json"

    def evaluate(self, compat_info: dict, readiness: dict):
        failures = []
        warnings = []

        row_count = compat_info.get("row_count", 0)
        rows_with_ticker = compat_info.get("rows_with_ticker", 0)
        rows_with_action = compat_info.get("rows_with_action", 0)
        rows_with_price = compat_info.get("rows_with_price", 0)
        total_signals = readiness.get("total_signals", 0)

        if row_count == 0:
            failures.append({
                "type": "empty_research_output",
                "message": "研究/選股輸出在 normalize 後為空"
            })

        if rows_with_ticker == 0:
            failures.append({
                "type": "ticker_missing",
                "message": "研究/選股輸出缺少可用 ticker"
            })

        if rows_with_action == 0:
            failures.append({
                "type": "action_missing",
                "message": "研究/選股輸出缺少可用 action"
            })

        if rows_with_price == 0:
            failures.append({
                "type": "price_missing",
                "message": "研究/選股輸出缺少可用 reference price"
            })

        if row_count > 0 and total_signals == 0:
            warnings.append({
                "type": "zero_signal_after_research",
                "message": "研究/選股有輸出，但轉成有效訊號後為 0"
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "go_for_decision_linkage": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
            "summary": {
                "row_count": row_count,
                "rows_with_ticker": rows_with_ticker,
                "rows_with_action": rows_with_action,
                "rows_with_price": rows_with_price,
                "total_signals": total_signals,
                "failure_count": len(failures),
                "warning_count": len(warnings),
            }
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(
            f"🔎 Research Quality Gate | go_for_decision_linkage={payload['go_for_decision_linkage']} | "
            f"failures={len(failures)} | warnings={len(warnings)}"
        )
        return self.path, payload


# ==============================================================================
# Merged from: feature_selector.py
# ==============================================================================
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


# ==============================================================================
# Merged from: alpha_miner.py
# ==============================================================================
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


# ==============================================================================
# Merged support function: alpha candidate auto-approval facade
# ==============================================================================
def auto_approve_latest_alpha_candidate(approver: str = 'auto', note: str = '') -> dict[str, Any]:
    """Build the latest AlphaMiner candidate report and record it as an approved snapshot.

    This keeps alpha features candidate-only by default; it does not mutate production
    config or live mounts. It returns a compact status payload for ApprovedPipeline.
    """
    path, payload = AlphaMiner().mine_alpha_candidates()
    status = str(payload.get('status', 'unknown'))
    approved = {
        'approved_at': now_str(),
        'approved_by': str(approver),
        'approval_note': str(note or 'alpha miner auto approval snapshot'),
        'source_path': str(path),
        'candidate_count': int(payload.get('candidate_count', 0) or 0),
        'status': 'approved_snapshot_only' if status == 'alpha_candidates_ready' else status,
        'live_effect': 'none_until_explicitly_promoted_to_feature_registry',
        'top_long_candidates': payload.get('top_long_candidates', []),
        'top_short_candidates': payload.get('top_short_candidates', []),
        'top_range_candidates': payload.get('top_range_candidates', []),
    }
    try:
        approval_path = Path(PATHS.runtime_dir) / 'alpha_miner_auto_approval.json'
        approval_path.write_text(json.dumps(approved, ensure_ascii=False, indent=2), encoding='utf-8')
        approved['approval_path'] = str(approval_path)
    except Exception as exc:
        approved['approval_write_error'] = str(exc)
    return approved
