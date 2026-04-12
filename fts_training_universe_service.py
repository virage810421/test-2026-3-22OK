# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd

from config import PARAMS, LOSERS_LIST
from fts_config import PATHS
from fts_sector_service import SectorService
from fts_utils import log, now_str
from fts_training_universe_common import full_refresh_table, normalize_ticker, score_linear, write_json
from fts_stock_master_service import StockMasterService
from fts_company_quality_snapshot_service import CompanyQualitySnapshotService
from fts_revenue_momentum_snapshot_service import RevenueMomentumSnapshotService
from fts_price_liquidity_snapshot_service import PriceLiquiditySnapshotService
from fts_chip_factor_snapshot_service import ChipFactorSnapshotService


class TrainingUniverseService:
    MODULE_VERSION = 'v88_training_universe_service_zh_columns'

    COL_TICKER = '股票代號'
    COL_DATE = '資料日期'
    COL_INDUSTRY_CODE = '產業類別'
    COL_INDUSTRY_NAME = '產業類別名稱'
    COL_LIQUIDITY = '流動性分數'
    COL_CHIP = '籌碼分數'
    COL_FUNDAMENTAL = '基本面分數'
    COL_REVENUE_MOMENTUM = '營收動能分數'
    COL_RISK = '風險扣分'
    COL_TRADABLE = '可交易旗標'
    COL_ELIGIBLE = '可訓練旗標'
    COL_TIER = '訓練分層'
    COL_EXCLUDE = '排除原因'
    COL_SCORE = '訓練母池總分'
    COL_COMPLETENESS = '資料完整率'
    COL_ADV20 = '二十日平均成交額'
    COL_ATR = 'ATR百分比'
    COL_ROE = '股東權益報酬率(%)'
    COL_DEBT = '負債比率(%)'
    COL_REVENUE_YOY = '單月營收年增率(%)'
    COL_SECTOR_BUCKET = '產業分桶'

    def __init__(self):
        self.csv_path = PATHS.runtime_dir / 'training_universe_daily.csv'
        self.summary_path = PATHS.runtime_dir / 'training_universe_service.json'
        self.sector_service = SectorService()

    @staticmethod
    def _series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
        if col in df.columns:
            return pd.to_numeric(df[col], errors='coerce').fillna(default)
        return pd.Series([default] * len(df), index=df.index, dtype=float)

    @classmethod
    def _legacy_to_zh_map(cls) -> dict[str, str]:
        return {
            'Ticker SYMBOL': cls.COL_TICKER,
            'Liquidity_Score': cls.COL_LIQUIDITY,
            'Chip_Score': cls.COL_CHIP,
            'Fundamental_Score': cls.COL_FUNDAMENTAL,
            'Revenue_Momentum_Score': cls.COL_REVENUE_MOMENTUM,
            'Risk_Penalty': cls.COL_RISK,
            'Tradability_Flag': cls.COL_TRADABLE,
            'Training_Eligible_Flag': cls.COL_ELIGIBLE,
            'Training_Tier': cls.COL_TIER,
            'Exclude_Reason': cls.COL_EXCLUDE,
            'Universe_Score': cls.COL_SCORE,
            'ADV20': cls.COL_ADV20,
            'ATR_Pct': cls.COL_ATR,
            'ROE(%)': cls.COL_ROE,
            'SectorBucket': cls.COL_SECTOR_BUCKET,
        }

    @classmethod
    def _normalize_output_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        out = df.copy()
        for old_col, new_col in cls._legacy_to_zh_map().items():
            if old_col in out.columns and new_col not in out.columns:
                out = out.rename(columns={old_col: new_col})
        return out

    def build(self, sync_sql: bool = True, refresh_dependencies: bool = True):
        stock_master_service = StockMasterService()
        quality_service = CompanyQualitySnapshotService()
        revenue_service = RevenueMomentumSnapshotService()
        liquidity_service = PriceLiquiditySnapshotService()
        chip_service = ChipFactorSnapshotService()

        if refresh_dependencies:
            stock_master_service.build(sync_sql=sync_sql)
            quality_service.build(sync_sql=sync_sql)
            revenue_service.build(sync_sql=sync_sql)
            liquidity_service.build(sync_sql=sync_sql)
            chip_service.build(sync_sql=sync_sql)

        master = stock_master_service.load()
        quality = quality_service.load()
        revenue = revenue_service.load()
        liquidity = liquidity_service.load()
        chip = chip_service.load()

        if master.empty:
            payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'status': 'stock_master_missing', 'row_count': 0}
            write_json(self.summary_path, payload)
            return self.summary_path, payload

        df = master.copy()
        if 'SectorBucket' not in df.columns:
            df['SectorBucket'] = df['Ticker SYMBOL'].map(lambda t: self.sector_service.get_stock_sector(t))
        if not quality.empty:
            keep = ['Ticker SYMBOL', 'Quality_Total_Score', 'ROE(%)', '負債比率(%)', '單月營收年增率(%)']
            quality_keep = [c for c in keep if c in quality.columns]
            df = df.merge(quality[quality_keep], on='Ticker SYMBOL', how='left')
        if not revenue.empty:
            keep = ['Ticker SYMBOL', '營收動能分數', '單月營收年增率(%)', '三月平均年增(%)', '營收加速度']
            revenue_keep = [c for c in keep if c in revenue.columns]
            renamed = revenue[revenue_keep].copy()
            if '單月營收年增率(%)' in renamed.columns and '單月營收年增率(%)' in df.columns:
                renamed = renamed.rename(columns={'單月營收年增率(%)': '單月營收年增率_營收表(%)'})
            df = df.merge(renamed, on='Ticker SYMBOL', how='left')
        if not liquidity.empty:
            keep = ['Ticker SYMBOL', 'ADV20', 'ATR_Pct', 'Liquidity_Score', '近20日缺資料天數', '是否異常波動', '是否連續無量']
            liquidity_keep = [c for c in keep if c in liquidity.columns]
            df = df.merge(liquidity[liquidity_keep], on='Ticker SYMBOL', how='left')
        if not chip.empty:
            keep = ['Ticker SYMBOL', 'Chip_Score', '三大法人合計', '籌碼集中度', '大戶散戶差']
            chip_keep = [c for c in keep if c in chip.columns]
            df = df.merge(chip[chip_keep], on='Ticker SYMBOL', how='left')

        df[self.COL_DATE] = pd.Timestamp.now().normalize()
        df[self.COL_LIQUIDITY] = self._series(df, 'Liquidity_Score', 0.0)
        df[self.COL_CHIP] = self._series(df, 'Chip_Score', 0.0)
        df[self.COL_FUNDAMENTAL] = self._series(df, 'Quality_Total_Score', 0.0)
        df[self.COL_REVENUE_MOMENTUM] = self._series(df, '營收動能分數', 0.0)
        df[self.COL_ADV20] = self._series(df, 'ADV20', 0.0)
        df[self.COL_ATR] = self._series(df, 'ATR_Pct', 0.0)
        df[self.COL_ROE] = self._series(df, 'ROE(%)', 0.0)
        df[self.COL_DEBT] = self._series(df, '負債比率(%)', 999.0)
        if '單月營收年增率_營收表(%)' in df.columns:
            df['Revenue_YoY_Filter'] = self._series(df, '單月營收年增率_營收表(%)', 0.0).fillna(self._series(df, '單月營收年增率(%)', 0.0))
        else:
            df['Revenue_YoY_Filter'] = self._series(df, '單月營收年增率(%)', 0.0)

        components = [
            pd.to_numeric(df.get(self.COL_LIQUIDITY, 0.0), errors='coerce').notna().astype(int),
            pd.to_numeric(df.get(self.COL_CHIP, 0.0), errors='coerce').notna().astype(int),
            pd.to_numeric(df.get(self.COL_FUNDAMENTAL, 0.0), errors='coerce').notna().astype(int),
            pd.to_numeric(df.get(self.COL_REVENUE_MOMENTUM, 0.0), errors='coerce').notna().astype(int),
        ]
        df[self.COL_COMPLETENESS] = sum(components) / float(len(components))

        adv_gate = df[self.COL_ADV20] >= float(PARAMS.get('TRAINING_UNIVERSE_MIN_ADV20', 50_000_000))
        completeness_gate = df[self.COL_COMPLETENESS] >= float(PARAMS.get('TRAINING_UNIVERSE_MIN_COMPLETENESS', 0.75))
        roe_gate = df[self.COL_ROE] >= float(PARAMS.get('TRAINING_UNIVERSE_MIN_ROE', 5.0))
        debt_gate = df[self.COL_DEBT] <= float(PARAMS.get('TRAINING_UNIVERSE_MAX_DEBT_RATIO', 70.0))
        yoy_gate = df['Revenue_YoY_Filter'] >= float(PARAMS.get('TRAINING_UNIVERSE_MIN_REVENUE_YOY', -10.0))
        chip_gate = df[self.COL_CHIP] >= float(PARAMS.get('TRAINING_UNIVERSE_MIN_CHIP_SCORE', 0.10))
        tradable_gate = (
            (self._series(df, '是否ETF', 0).astype(int) == 0)
            & (self._series(df, '是否下市', 0).astype(int) == 0)
            & (self._series(df, '是否停牌', 0).astype(int) == 0)
            & (df[self.COL_LIQUIDITY] >= float(PARAMS.get('TRAINING_UNIVERSE_MIN_LIQUIDITY_SCORE', 0.10)))
        )
        risk_penalty = (
            0.35 * df[self.COL_ATR].apply(lambda v: score_linear(v, 0.03, 0.12, default=0.0))
            + 0.35 * self._series(df, '是否異常波動', 0.0).clip(0, 1)
            + 0.30 * self._series(df, '是否連續無量', 0.0).clip(0, 1)
        ).clip(lower=0.0, upper=1.0)
        df[self.COL_RISK] = risk_penalty
        df[self.COL_TRADABLE] = tradable_gate.astype(int)
        df[self.COL_ELIGIBLE] = (adv_gate & completeness_gate & roe_gate & debt_gate & yoy_gate & chip_gate & tradable_gate).astype(int)
        loser_set = {normalize_ticker(x) for x in LOSERS_LIST}
        df[self.COL_SCORE] = (
            0.30 * df[self.COL_LIQUIDITY]
            + 0.20 * df[self.COL_CHIP]
            + 0.30 * df[self.COL_FUNDAMENTAL]
            + 0.20 * df[self.COL_REVENUE_MOMENTUM]
            - 0.20 * df[self.COL_RISK]
        ).clip(lower=0.0, upper=1.0)
        reasons = []
        tiers = []
        for _, row in df.iterrows():
            ticker = normalize_ticker(row.get('Ticker SYMBOL'))
            reason_parts = []
            if int(row.get(self.COL_ELIGIBLE, 0)) == 1:
                if ticker in loser_set:
                    tiers.append('NEGATIVE')
                    reason_parts.append('loser_control_group')
                elif float(row.get(self.COL_SCORE, 0.0)) >= float(PARAMS.get('TRAINING_UNIVERSE_CORE_SCORE', 0.65)):
                    tiers.append('CORE')
                else:
                    tiers.append('OBSERVE')
            else:
                if ticker in loser_set:
                    tiers.append('NEGATIVE')
                    reason_parts.append('loser_control_group')
                else:
                    tiers.append('EXCLUDE')
                if not bool(row.get(self.COL_TRADABLE, 0)):
                    reason_parts.append('tradability_fail')
                if float(row.get(self.COL_ADV20, 0.0)) < float(PARAMS.get('TRAINING_UNIVERSE_MIN_ADV20', 50_000_000)):
                    reason_parts.append('adv20_too_low')
                if float(row.get(self.COL_COMPLETENESS, 0.0)) < float(PARAMS.get('TRAINING_UNIVERSE_MIN_COMPLETENESS', 0.75)):
                    reason_parts.append('completeness_low')
                if float(row.get(self.COL_ROE, 0.0)) < float(PARAMS.get('TRAINING_UNIVERSE_MIN_ROE', 5.0)):
                    reason_parts.append('roe_low')
                if float(row.get(self.COL_DEBT, 999.0)) > float(PARAMS.get('TRAINING_UNIVERSE_MAX_DEBT_RATIO', 70.0)):
                    reason_parts.append('debt_high')
                if float(row.get('Revenue_YoY_Filter', -999.0)) < float(PARAMS.get('TRAINING_UNIVERSE_MIN_REVENUE_YOY', -10.0)):
                    reason_parts.append('revenue_yoy_low')
                if float(row.get(self.COL_CHIP, 0.0)) < float(PARAMS.get('TRAINING_UNIVERSE_MIN_CHIP_SCORE', 0.10)):
                    reason_parts.append('chip_weak')
            reasons.append('|'.join(reason_parts) if reason_parts else '')
        df[self.COL_TIER] = tiers
        df[self.COL_EXCLUDE] = reasons
        df[self.COL_INDUSTRY_NAME] = df['產業類別名稱'].fillna('') if '產業類別名稱' in df.columns else ''
        df[self.COL_INDUSTRY_CODE] = df['SectorBucket'].fillna('') if 'SectorBucket' in df.columns else ''
        df[self.COL_TICKER] = df['Ticker SYMBOL']
        df[self.COL_REVENUE_YOY] = df['Revenue_YoY_Filter']
        df[self.COL_SECTOR_BUCKET] = df['SectorBucket']
        keep_cols = [
            self.COL_TICKER, self.COL_DATE, self.COL_INDUSTRY_CODE, self.COL_INDUSTRY_NAME, self.COL_LIQUIDITY,
            self.COL_CHIP, self.COL_FUNDAMENTAL, self.COL_REVENUE_MOMENTUM, self.COL_RISK, self.COL_TRADABLE,
            self.COL_ELIGIBLE, self.COL_TIER, self.COL_EXCLUDE, self.COL_SCORE, self.COL_COMPLETENESS,
            self.COL_ADV20, self.COL_ATR, self.COL_ROE, self.COL_DEBT, self.COL_REVENUE_YOY, self.COL_SECTOR_BUCKET,
        ]
        out = df[keep_cols].copy()
        if not out.empty:
            out = out.sort_values([self.COL_ELIGIBLE, self.COL_SCORE, self.COL_TICKER], ascending=[False, False, True]).reset_index(drop=True)
        out.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        sql_sync = full_refresh_table('training_universe_daily', out) if sync_sql else {'status': 'skip'}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': 'training_universe_ready',
            'csv_path': str(self.csv_path),
            'row_count': int(len(out)),
            'eligible_count': int(pd.to_numeric(out[self.COL_ELIGIBLE], errors='coerce').fillna(0).sum()),
            'tier_counts': {str(k): int(v) for k, v in out[self.COL_TIER].value_counts().to_dict().items()},
            'sql_sync': sql_sync,
        }
        write_json(self.summary_path, payload)
        log(f'🧠 training universe ready: {self.csv_path}')
        return self.csv_path, payload

    def load(self) -> pd.DataFrame:
        if self.csv_path.exists():
            try:
                df = pd.read_csv(self.csv_path, encoding='utf-8-sig')
            except Exception:
                df = pd.read_csv(self.csv_path)
            return self._normalize_output_columns(df)
        self.build(sync_sql=False, refresh_dependencies=False)
        if self.csv_path.exists():
            df = pd.read_csv(self.csv_path, encoding='utf-8-sig')
            return self._normalize_output_columns(df)
        return pd.DataFrame()

    def eligible_tickers(self) -> list[str]:
        df = self.load()
        if df.empty or self.COL_ELIGIBLE not in df.columns:
            return []
        return [str(t) for t in df.loc[pd.to_numeric(df[self.COL_ELIGIBLE], errors='coerce').fillna(0).astype(int) == 1, self.COL_TICKER].dropna().astype(str).tolist()]
