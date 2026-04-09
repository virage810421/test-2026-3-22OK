# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

try:
    from config import PARAMS  # type: ignore
except Exception:
    PARAMS = {'ML_LABEL_HOLD_DAYS': 5}

from fts_screening_engine import ScreeningEngine
from fts_market_data_service import MarketDataService
from fts_chip_enrichment_service import ChipEnrichmentService
from fts_feature_service import FeatureService

_market = MarketDataService()
_chip = ChipEnrichmentService()
_screen = ScreeningEngine()
_features = FeatureService()


def get_dynamic_watchlist():
    try:
        from config import get_dynamic_watch_list  # type: ignore
        return get_dynamic_watch_list()
    except Exception:
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


def generate_ml_dataset(tickers=None):
    tickers = tickers or get_dynamic_watchlist() or ['2330.TW', '2317.TW', '2454.TW']
    os.makedirs('data', exist_ok=True)
    dataset_path = Path('data/ml_training_data.csv')
    rows = []
    hold_days = int(PARAMS.get('ML_LABEL_HOLD_DAYS', 5))

    for ticker in tickers:
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
                feats = _features.extract_ai_features(row.to_dict(), history_df=computed_df.iloc[:i+1].copy(), ticker=ticker, as_of_date=row.name if hasattr(row, 'name') else None)
                mounted = _features.select_live_features(feats)
                sample = {'Ticker SYMBOL': ticker, 'Label': label, 'Future_Return_Pct': round(future_ret, 4), 'Setup_Tag': setup_tag, 'Regime': regime}
                sample.update(feats)
                for k, v in mounted.items():
                    sample[f'MOUNT__{k}'] = v
                rows.append(sample)
        except Exception:
            continue

    df_out = pd.DataFrame(rows)
    df_out.to_csv(dataset_path, index=False, encoding='utf-8-sig')
    if not df_out.empty:
        _features.write_training_feature_registry(df_out.iloc[0].to_dict())
    return df_out
