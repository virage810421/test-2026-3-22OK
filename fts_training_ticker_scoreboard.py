# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
        data_dir = Path('data')
    PATHS = _Paths()


class TrainingTickerScoreboard:
    MODULE_VERSION = 'v84_training_ticker_scoreboard_long_short_range_safe'

    def __init__(self):
        self.runtime_dir = Path(getattr(PATHS, 'runtime_dir', Path('runtime')))
        self.data_dir = Path(getattr(PATHS, 'data_dir', Path('data')))
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.runtime_dir / 'training_ticker_scoreboard.csv'
        self.json_path = self.runtime_dir / 'training_ticker_scoreboard.json'
        self.long_csv_path = self.runtime_dir / 'training_ticker_scoreboard_long.csv'
        self.short_csv_path = self.runtime_dir / 'training_ticker_scoreboard_short.csv'
        self.range_csv_path = self.runtime_dir / 'training_ticker_scoreboard_range.csv'
        self.dataset_path = self.data_dir / 'ml_training_data.csv'

    @staticmethod
    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            if pd.isna(v):
                return default
            return float(v)
        except Exception:
            return default

    def _load(self) -> pd.DataFrame:
        if not self.dataset_path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(self.dataset_path)
        except Exception:
            return pd.DataFrame()

    def _bucket_metrics(self, df: pd.DataFrame, bucket: str) -> dict[str, float]:
        if df.empty:
            return {'trade_count': 0.0, 'hit_rate': 0.0, 'oot_ev': 0.0, 'pf': 0.0, 'maxdd': 0.0, 'stability': 0.0}
        if bucket == 'LONG':
            active = df[df.get('Long_Label_Y', 0).fillna(0).astype(int) >= 0]
            series = pd.to_numeric(active.get('Long_Target_Return', 0.0), errors='coerce').fillna(0.0)
        elif bucket == 'SHORT':
            active = df[df.get('Short_Label_Y', 0).fillna(0).astype(int) >= 0]
            series = pd.to_numeric(active.get('Short_Target_Return', 0.0), errors='coerce').fillna(0.0)
        else:
            active = df[df.get('Range_Label_Y', 0).fillna(0).astype(int) >= 0]
            series = pd.to_numeric(active.get('Range_Target_Return', 0.0), errors='coerce').fillna(0.0)
        if active.empty:
            return {'trade_count': 0.0, 'hit_rate': 0.0, 'oot_ev': 0.0, 'pf': 0.0, 'maxdd': 0.0, 'stability': 0.0}
        wins = series[series > 0]
        losses = series[series <= 0]
        pf = float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 1e-12 else (99.9 if len(wins) else 0.0)
        cum = series.cumsum()
        maxdd = float((cum.cummax() - cum).max()) if not cum.empty else 0.0
        stability = float((series.rolling(5, min_periods=3).mean().fillna(0.0) > 0).mean()) if len(series) >= 3 else float((series > 0).mean())
        return {
            'trade_count': float(len(series)),
            'hit_rate': float((series > 0).mean()) if len(series) else 0.0,
            'oot_ev': float(series.mean()) if len(series) else 0.0,
            'pf': pf,
            'maxdd': maxdd,
            'stability': stability,
        }

    def build_scoreboard(self) -> tuple[Path, dict[str, Any]]:
        df = self._load()
        if df.empty or 'Ticker SYMBOL' not in df.columns:
            payload = {'module_version': self.MODULE_VERSION, 'status': 'scoreboard_unavailable', 'rows': 0}
            self.json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.json_path, payload
        records: list[dict[str, Any]] = []
        for ticker, g in df.groupby('Ticker SYMBOL'):
            sector = str(g.get('Industry_Name', g.get('產業名稱', pd.Series(['未知']))).iloc[0]) if len(g) else '未知'
            regime_mode = str(g.get('Regime', pd.Series(['區間盤整'])).mode().iloc[0]) if 'Regime' in g.columns and not g['Regime'].dropna().empty else '區間盤整'
            long_m = self._bucket_metrics(g, 'LONG')
            short_m = self._bucket_metrics(g, 'SHORT')
            range_m = self._bucket_metrics(g, 'RANGE')
            feat_cov = float(pd.to_numeric(g.get('Mounted_Feature_Count', 0), errors='coerce').fillna(0).mean())
            recent = pd.to_numeric(g.get('Target_Return', 0.0), errors='coerce').fillna(0.0).tail(20)
            rec = {
                'Ticker SYMBOL': ticker,
                'Sector': sector,
                'Regime': regime_mode,
                'Long_Trade_Count': long_m['trade_count'],
                'Short_Trade_Count': short_m['trade_count'],
                'Range_Trade_Count': range_m['trade_count'],
                'Long_HitRate': long_m['hit_rate'],
                'Short_HitRate': short_m['hit_rate'],
                'Range_HitRate': range_m['hit_rate'],
                'Long_OOT_EV': long_m['oot_ev'],
                'Short_OOT_EV': short_m['oot_ev'],
                'Range_OOT_EV': range_m['oot_ev'],
                'Long_PF': long_m['pf'],
                'Short_PF': short_m['pf'],
                'Range_PF': range_m['pf'],
                'Long_MaxDD': long_m['maxdd'],
                'Short_MaxDD': short_m['maxdd'],
                'Range_MaxDD': range_m['maxdd'],
                'Long_Stability_Score': long_m['stability'],
                'Short_Stability_Score': short_m['stability'],
                'Range_Stability_Score': range_m['stability'],
                'Selected_Feature_Coverage': feat_cov,
                'Recent_20d_Score_Trend': float(recent.mean()) if len(recent) else 0.0,
            }
            rec['Ticker_Promotion_Score_Long'] = round(rec['Long_OOT_EV'] * 0.30 + rec['Long_HitRate'] * 0.20 + min(rec['Long_PF'], 5.0) * 0.10 + rec['Long_Stability_Score'] * 0.20 + min(rec['Selected_Feature_Coverage'] / 20.0, 1.0) * 0.20, 6)
            rec['Ticker_Promotion_Score_Short'] = round(rec['Short_OOT_EV'] * 0.30 + rec['Short_HitRate'] * 0.20 + min(rec['Short_PF'], 5.0) * 0.10 + rec['Short_Stability_Score'] * 0.20 + min(rec['Selected_Feature_Coverage'] / 20.0, 1.0) * 0.20, 6)
            rec['Ticker_Promotion_Score_Range'] = round(rec['Range_OOT_EV'] * 0.30 + rec['Range_HitRate'] * 0.20 + min(rec['Range_PF'], 5.0) * 0.10 + rec['Range_Stability_Score'] * 0.20 + min(rec['Selected_Feature_Coverage'] / 20.0, 1.0) * 0.20, 6)
            records.append(rec)
        board = pd.DataFrame(records)
        if board.empty:
            payload = {'module_version': self.MODULE_VERSION, 'status': 'scoreboard_empty', 'rows': 0}
            self.json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.json_path, payload
        board['Global_Rank_Long'] = board['Ticker_Promotion_Score_Long'].rank(method='dense', ascending=False)
        board['Global_Rank_Short'] = board['Ticker_Promotion_Score_Short'].rank(method='dense', ascending=False)
        board['Global_Rank_Range'] = board['Ticker_Promotion_Score_Range'].rank(method='dense', ascending=False)
        board['Sector_Rank_Long'] = board.groupby('Sector')['Ticker_Promotion_Score_Long'].rank(method='dense', ascending=False)
        board['Sector_Rank_Short'] = board.groupby('Sector')['Ticker_Promotion_Score_Short'].rank(method='dense', ascending=False)
        board['Sector_Rank_Range'] = board.groupby('Sector')['Ticker_Promotion_Score_Range'].rank(method='dense', ascending=False)
        board.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        board.sort_values('Ticker_Promotion_Score_Long', ascending=False).to_csv(self.long_csv_path, index=False, encoding='utf-8-sig')
        board.sort_values('Ticker_Promotion_Score_Short', ascending=False).to_csv(self.short_csv_path, index=False, encoding='utf-8-sig')
        board.sort_values('Ticker_Promotion_Score_Range', ascending=False).to_csv(self.range_csv_path, index=False, encoding='utf-8-sig')
        payload = {
            'module_version': self.MODULE_VERSION,
            'status': 'training_ticker_scoreboard_ready',
            'rows': int(len(board)),
            'csv_path': str(self.csv_path),
            'top_long': board.sort_values('Ticker_Promotion_Score_Long', ascending=False).head(5)['Ticker SYMBOL'].tolist(),
            'top_short': board.sort_values('Ticker_Promotion_Score_Short', ascending=False).head(5)['Ticker SYMBOL'].tolist(),
            'top_range': board.sort_values('Ticker_Promotion_Score_Range', ascending=False).head(5)['Ticker SYMBOL'].tolist(),
        }
        self.json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.json_path, payload


def main() -> int:
    TrainingTickerScoreboard().build_scoreboard()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
