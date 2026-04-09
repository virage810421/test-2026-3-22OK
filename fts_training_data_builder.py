# -*- coding: utf-8 -*-
"""v83 主線收編服務：ml_data_generator.py 的訓練資料生產能力。"""
from __future__ import annotations

import os
import pandas as pd

from fts_screening_engine import ScreeningEngine
from fts_market_data_service import MarketDataService
from fts_chip_enrichment_service import ChipEnrichmentService

try:
    from config import PARAMS  # type: ignore
except Exception:
    PARAMS = {
        'TRIGGER_SCORE': 2,
        'ML_LABEL_HOLD_DAYS': 5,
        'SL_MIN_PCT': 0.03,
        'FEE_RATE': 0.001425,
        'FEE_DISCOUNT': 1.0,
        'TAX_RATE': 0.003,
    }


_market = MarketDataService()
_chip = ChipEnrichmentService()
_screen = ScreeningEngine()


def get_dynamic_watchlist():
    print('📡 啟動動態索敵雷達：正在連接 config 名單樞紐...')
    try:
        from config import get_dynamic_watch_list  # type: ignore
        dynamic_list = get_dynamic_watch_list()
        print(f'✅ 成功鎖定 {len(dynamic_list)} 檔目標，準備印製歷史課本！')
        return dynamic_list
    except Exception as e:
        print(f'⚠️ 無法獲取名單: {e}')
        try:
            from config import WATCH_LIST  # type: ignore
            return WATCH_LIST
        except Exception:
            return []


def _signal_flags(setup_tag: str):
    tag = str(setup_tag).strip()
    is_short = ('空' in tag) or ('SHORT' in tag.upper())
    is_long = ('多' in tag) or ('LONG' in tag.upper())
    return is_long, is_short


def generate_ml_dataset(tickers):
    print('🏭 [兵工廠] 啟動 AI 雙向訓練資料生成器...')
    os.makedirs('data', exist_ok=True)
    dataset_path = 'data/ml_training_data.csv'
    if os.path.exists(dataset_path):
        os.remove(dataset_path)
        print('🗑️ 已銷毀昨日舊有訓練資料，確保數據絕對純淨。')

    ml_dataset = []
    hold_days = int(PARAMS.get('ML_LABEL_HOLD_DAYS', 5))

    for ticker in tickers:
        print(f'📡 正在萃取 {ticker} 的歷史特徵與勝負標籤...')
        try:
            df = _market.smart_download(ticker, period='3y')
            if df.empty:
                continue
            df = _chip.add_chip_data(df, ticker)
            result = _screen.inspect_stock(ticker, preloaded_df=df, p=PARAMS)
            if not result or '計算後資料' not in result:
                continue
            computed_df = result['計算後資料'].copy()
            if len(computed_df) <= hold_days + 2:
                continue
            for i in range(len(computed_df) - hold_days - 1):
                row = computed_df.iloc[i]
                setup_tag = str(row.get('Golden_Type', '無')).strip()
                regime = str(row.get('Regime', '區間盤整')).strip()
                if setup_tag == '無':
                    continue
                is_long, is_short = _signal_flags(setup_tag)
                if not (is_long or is_short):
                    continue
                entry_px = float(row.get('Close', 0))
                exit_row = computed_df.iloc[min(i + hold_days, len(computed_df) - 1)]
                exit_px = float(exit_row.get('Close', entry_px))
                future_ret = ((exit_px / entry_px) - 1.0) * 100 if entry_px > 0 else 0.0
                if is_short:
                    future_ret *= -1
                label = 1 if future_ret > 0 else 0
                features = result.get('ai_features_latest') or {}
                sample = {
                    'Ticker SYMBOL': ticker,
                    'Label': label,
                    'Future_Return_Pct': round(future_ret, 4),
                    'Setup_Tag': setup_tag,
                    'Regime': regime,
                }
                sample.update(features)
                ml_dataset.append(sample)
        except Exception as exc:
            print(f'⚠️ {ticker} 訓練資料生成失敗: {exc}')
            continue

    df_out = pd.DataFrame(ml_dataset)
    df_out.to_csv(dataset_path, index=False, encoding='utf-8-sig')
    print(f'✅ 訓練資料生成完成：{dataset_path} | 筆數：{len(df_out)}')
    return df_out
