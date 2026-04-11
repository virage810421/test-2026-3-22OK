# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fts_config import PATHS
from fts_utils import now_str, log
from fts_sector_service import SectorService


class TrainingTickerScoreboard:
    MODULE_VERSION = 'v86_training_ticker_scoreboard'

    def __init__(self):
        self.csv_path = PATHS.runtime_dir / 'training_ticker_scoreboard.csv'
        self.summary_path = PATHS.runtime_dir / 'training_ticker_scoreboard.json'
        self.sector_service = SectorService()

    @staticmethod
    def _ticker_col(df: pd.DataFrame) -> str | None:
        for col in ['Ticker SYMBOL', 'Ticker', 'ticker', 'symbol']:
            if col in df.columns:
                return col
        return None

    @staticmethod
    def _industry_col(df: pd.DataFrame) -> str | None:
        for col in ['產業類別名稱', '產業類別', 'industry_name', 'industry']:
            if col in df.columns:
                return col
        return None

    @staticmethod
    def _safe_numeric(series: pd.Series | Any, default: float = 0.0) -> pd.Series:
        if isinstance(series, pd.Series):
            return pd.to_numeric(series, errors='coerce').fillna(default)
        return pd.Series(dtype=float)

    @staticmethod
    def _max_drawdown(returns: pd.Series) -> float:
        if returns.empty:
            return 0.0
        curve = (1.0 + returns.fillna(0.0)).cumprod()
        peak = curve.cummax()
        dd = (curve / peak) - 1.0
        return abs(float(dd.min())) if len(dd) else 0.0

    @staticmethod
    def _profit_factor(returns: pd.Series) -> float:
        pos = float(returns[returns > 0].sum()) if len(returns) else 0.0
        neg = float(abs(returns[returns < 0].sum())) if len(returns) else 0.0
        return pos / neg if neg > 1e-12 else (99.9 if pos > 0 else 0.0)

    @staticmethod
    def _recent_trend(returns: pd.Series, window: int = 20) -> float:
        if len(returns) < window + 5:
            return float(returns.tail(min(len(returns), window)).mean()) if len(returns) else 0.0
        recent = float(returns.tail(window).mean())
        prev = float(returns.iloc[-window*2:-window].mean()) if len(returns) >= window * 2 else float(returns.iloc[:-window].tail(window).mean())
        return recent - prev

    def _feature_coverage(self, df: pd.DataFrame, selected_features: list[str]) -> float:
        safe = [f for f in selected_features if f in df.columns]
        if not safe or df.empty:
            return 0.0
        return float(df[safe].notna().mean().mean())

    def build_from_parts(
        self,
        train_df: pd.DataFrame,
        oot_df: pd.DataFrame,
        pred_oot: list[int] | np.ndarray | None,
        selected_features: list[str],
    ) -> tuple[Path, dict[str, Any]]:
        train_df = train_df.copy()
        oot_df = oot_df.copy()
        ticker_col = self._ticker_col(oot_df)
        if ticker_col is None:
            payload = {
                'generated_at': now_str(),
                'module_version': self.MODULE_VERSION,
                'status': 'missing_ticker_column',
            }
            self.summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.summary_path, payload

        if pred_oot is None or len(pred_oot) != len(oot_df):
            oot_df['__pred'] = 1
            prediction_source = 'fallback_all_ones'
        else:
            oot_df['__pred'] = pd.Series(pred_oot, index=oot_df.index).astype(int)
            prediction_source = 'model_prediction'

        if 'Target_Return' not in oot_df.columns:
            oot_df['Target_Return'] = np.where(pd.to_numeric(oot_df.get('Label_Y', 0), errors='coerce').fillna(0).astype(int) == 1, 0.05, -0.05)
        oot_df['Target_Return'] = self._safe_numeric(oot_df['Target_Return'])
        if 'Date' in oot_df.columns:
            oot_df = oot_df.sort_values('Date').reset_index(drop=True)

        ticker_train_counts = train_df[ticker_col].astype(str).value_counts().to_dict() if ticker_col in train_df.columns else {}
        industry_col = self._industry_col(oot_df)

        rows: list[dict[str, Any]] = []
        for ticker, grp in oot_df.groupby(oot_df[ticker_col].astype(str)):
            grp = grp.copy().reset_index(drop=True)
            strategy_ret = np.where(grp['__pred'].astype(int).values == 1, grp['Target_Return'].values, 0.0)
            strat_series = pd.Series(strategy_ret)
            oot_samples = int(len(grp))
            selected_samples = int((grp['__pred'].astype(int) == 1).sum())
            hit_rate = float((strat_series > 0).mean()) if len(strat_series) else 0.0
            avg_ret = float(strat_series.mean()) if len(strat_series) else 0.0
            pf = self._profit_factor(strat_series)
            mdd = self._max_drawdown(strat_series)
            stability = float(1.0 / (1.0 + max(0.0, float(strat_series.std(ddof=0) if len(strat_series) > 1 else 0.0)) * 10.0))
            feature_cov = self._feature_coverage(grp, selected_features)
            regime = str(grp['Regime'].mode().iloc[0]) if 'Regime' in grp.columns and not grp['Regime'].mode().empty else '未知'
            industry = str(grp[industry_col].mode().iloc[0]) if industry_col and not grp[industry_col].mode().empty else '未知'
            sector = self.sector_service.get_stock_sector(str(ticker))
            recent_trend = self._recent_trend(strat_series, window=20)
            liquidity = 1.0
            for vol_col in ['DollarVol20_Proxy', 'ADV20_Proxy', '成交金額', 'Volume']:
                if vol_col in grp.columns:
                    vv = pd.to_numeric(grp[vol_col], errors='coerce').fillna(0.0)
                    liquidity = float(np.clip(vv.mean() / max(vv.quantile(0.75), 1.0), 0.0, 1.0))
                    break
            walk_forward_ev = float(avg_ret * stability)
            rows.append({
                'ticker': str(ticker),
                'industry': industry,
                'sector': sector,
                'regime': regime,
                'train_samples': int(ticker_train_counts.get(str(ticker), 0)),
                'oot_samples': oot_samples,
                'selected_trade_samples': selected_samples,
                'oot_hit_rate': hit_rate,
                'oot_ev': avg_ret,
                'walk_forward_ev': walk_forward_ev,
                'avg_return': avg_ret,
                'max_drawdown': mdd,
                'profit_factor': float(pf),
                'stability_score': stability,
                'selected_feature_coverage': feature_cov,
                'recent_20d_score_trend': recent_trend,
                'liquidity_score': liquidity,
                'prediction_source': prediction_source,
            })

        scoreboard = pd.DataFrame(rows)
        if scoreboard.empty:
            payload = {
                'generated_at': now_str(),
                'module_version': self.MODULE_VERSION,
                'status': 'empty_scoreboard',
                'prediction_source': prediction_source,
            }
            self.summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.summary_path, payload

        # sector-aware base score
        scoreboard['promotion_score_base'] = (
            30.0 * scoreboard['oot_ev'].clip(lower=0.0).div(0.05).clip(upper=1.0)
            + 20.0 * scoreboard['oot_hit_rate'].clip(0.0, 1.0)
            + 20.0 * scoreboard['walk_forward_ev'].clip(lower=0.0).div(0.05).clip(upper=1.0)
            + 10.0 * (scoreboard['oot_samples'] + scoreboard['train_samples']).div(60.0).clip(upper=1.0)
            + 10.0 * scoreboard['stability_score'].clip(0.0, 1.0)
            + 10.0 * scoreboard['liquidity_score'].clip(0.0, 1.0)
        )
        scoreboard = scoreboard.sort_values(['promotion_score_base', 'oot_ev', 'oot_hit_rate'], ascending=[False, False, False]).reset_index(drop=True)
        scoreboard['global_rank'] = np.arange(1, len(scoreboard) + 1)
        scoreboard['sector_rank'] = scoreboard.groupby('sector')['promotion_score_base'].rank(method='dense', ascending=False).astype(int)
        scoreboard['regime_rank'] = scoreboard.groupby('regime')['promotion_score_base'].rank(method='dense', ascending=False).astype(int)

        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        scoreboard.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': 'scoreboard_ready',
            'csv_path': str(self.csv_path),
            'row_count': int(len(scoreboard)),
            'top_tickers': scoreboard['ticker'].head(10).tolist(),
            'prediction_source': prediction_source,
            'sector_counts': {str(k): int(v) for k, v in scoreboard['sector'].value_counts().to_dict().items()},
        }
        self.summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'📘 training ticker scoreboard ready: {self.csv_path}')
        return self.csv_path, payload

    def build_from_dataset(self, dataset_path: str | Path = 'data/ml_training_data.csv') -> tuple[Path, dict[str, Any]]:
        dataset = Path(dataset_path)
        if not dataset.exists():
            payload = {
                'generated_at': now_str(),
                'module_version': self.MODULE_VERSION,
                'status': 'dataset_missing',
                'dataset_path': str(dataset),
            }
            self.summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.summary_path, payload
        df = pd.read_csv(dataset)
        if 'Date' in df.columns:
            df = df.sort_values('Date').reset_index(drop=True)
        if 'Target_Return' not in df.columns:
            df['Target_Return'] = np.where(pd.to_numeric(df.get('Label_Y', 0), errors='coerce').fillna(0).astype(int) == 1, 0.05, -0.05)
        split = max(int(len(df) * 0.8), 1)
        train_df = df.iloc[:split].copy()
        oot_df = df.iloc[split:].copy()
        selected_features = []
        models_selected = PATHS.model_dir / 'selected_features.pkl'
        if models_selected.exists():
            try:
                import joblib
                selected_features = list(joblib.load(models_selected))
            except Exception:
                selected_features = []
        return self.build_from_parts(train_df=train_df, oot_df=oot_df, pred_oot=None, selected_features=selected_features)

    def load_scoreboard(self) -> pd.DataFrame:
        if self.csv_path.exists():
            try:
                return pd.read_csv(self.csv_path, encoding='utf-8-sig')
            except Exception:
                try:
                    return pd.read_csv(self.csv_path)
                except Exception:
                    return pd.DataFrame()
        return pd.DataFrame()

    def load_summary(self) -> dict[str, Any]:
        if self.summary_path.exists():
            try:
                return json.loads(self.summary_path.read_text(encoding='utf-8'))
            except Exception:
                return {}
        return {}


if __name__ == '__main__':
    path, payload = TrainingTickerScoreboard().build_from_dataset()
    print(f'📘 {path}')
    print(payload.get('status'))
