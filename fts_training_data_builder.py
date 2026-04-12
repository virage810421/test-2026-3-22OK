# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from config import PARAMS  # type: ignore
except Exception:
    PARAMS = {'ML_LABEL_HOLD_DAYS': 5}

from fts_screening_engine import ScreeningEngine
from fts_market_data_service import MarketDataService
from fts_chip_enrichment_service import ChipEnrichmentService
from fts_feature_service import FeatureService
from fts_data_quality_guard import sanitize_generated_training_df

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


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _signal_flags(setup_tag: str):
    tag = str(setup_tag).strip()
    is_short = ('空' in tag) or ('SHORT' in tag.upper())
    is_long = ('多' in tag) or ('LONG' in tag.upper())
    return is_long, is_short


def _round_trip_cost() -> float:
    fee_rate = float(PARAMS.get('FEE_RATE', 0.001425)) * float(PARAMS.get('FEE_DISCOUNT', 1.0))
    tax_rate = float(PARAMS.get('TAX_RATE', 0.003))
    return float((2 * fee_rate) + tax_rate)


def _build_execution_aware_label(computed_df: pd.DataFrame, i: int, hold_days: int, setup_tag: str) -> dict[str, Any] | None:
    is_long, is_short = _signal_flags(setup_tag)
    if not (is_long or is_short):
        return None

    entry_idx = i + 1
    if entry_idx >= len(computed_df):
        return None

    entry_row = computed_df.iloc[entry_idx]
    entry_price = _safe_float(entry_row.get('Open', None), 0.0) if bool(PARAMS.get('LABEL_USE_NEXT_OPEN', True)) else _safe_float(computed_df.iloc[i].get('Close', None), 0.0)
    if entry_price <= 0:
        return None

    future_window = computed_df.iloc[entry_idx: entry_idx + hold_days].copy()
    if future_window.empty:
        return None

    sl_pct = float(PARAMS.get('SL_MIN_PCT', 0.03))
    round_trip_cost = _round_trip_cost()

    if is_short:
        max_adverse_excursion = max((_safe_float(future_window['High'].max(), entry_price) - entry_price) / entry_price, 0.0)
        max_favorable_excursion = max((entry_price - _safe_float(future_window['Low'].min(), entry_price)) / entry_price, 0.0)
        stop_hit = max_adverse_excursion > sl_pct
        touched_sl = int(stop_hit)
        touched_tp = int(max_favorable_excursion > sl_pct)
        exit_price = _safe_float(future_window.iloc[-1].get('Close', entry_price), entry_price)
        realized_return = ((entry_price - exit_price) / entry_price) - round_trip_cost
        favorable_move = max_favorable_excursion
        adverse_move = max_adverse_excursion
    else:
        max_adverse_excursion = max((entry_price - _safe_float(future_window['Low'].min(), entry_price)) / entry_price, 0.0)
        max_favorable_excursion = max((_safe_float(future_window['High'].max(), entry_price) - entry_price) / entry_price, 0.0)
        stop_hit = max_adverse_excursion > sl_pct
        touched_sl = int(stop_hit)
        touched_tp = int(max_favorable_excursion > sl_pct)
        exit_price = _safe_float(future_window.iloc[-1].get('Close', entry_price), entry_price)
        realized_return = ((exit_price - entry_price) / entry_price) - round_trip_cost
        favorable_move = max_favorable_excursion
        adverse_move = max_adverse_excursion

    label_y = 1 if ((not stop_hit) and (realized_return > 0 or favorable_move > sl_pct)) else 0
    if stop_hit:
        label_reason = 'stop_hit_before_target'
        label_exit_type = 'STOP'
    elif realized_return > 0:
        label_reason = 'positive_realized_return_after_cost'
        label_exit_type = 'TIME_EXIT_PROFIT'
    elif favorable_move > sl_pct:
        label_reason = 'favorable_move_exceeded_stop_threshold'
        label_exit_type = 'TP_TOUCH'
    else:
        label_reason = 'insufficient_edge'
        label_exit_type = 'TIME_EXIT_LOSS'

    entry_dt = pd.to_datetime(entry_row.name, errors='coerce')
    exit_dt = pd.to_datetime(future_window.index[-1], errors='coerce')

    return {
        'Label': int(label_y),
        'Label_Y': int(label_y),
        'Target_Return': round(float(realized_return * 100.0), 4),
        'Future_Return_Pct': round(float(realized_return * 100.0), 4),
        'Entry_Price': round(float(entry_price), 4),
        'Entry_Price_Basis': 'next_open' if bool(PARAMS.get('LABEL_USE_NEXT_OPEN', True)) else 'signal_close',
        'Exit_Price': round(float(exit_price), 4),
        'Entry_Date': entry_dt.strftime('%Y-%m-%d') if pd.notna(entry_dt) else None,
        'Exit_Date': exit_dt.strftime('%Y-%m-%d') if pd.notna(exit_dt) else None,
        'Stop_Hit': int(stop_hit),
        'Hold_Days': int(hold_days),
        'Touched_TP': int(touched_tp),
        'Touched_SL': int(touched_sl),
        'Label_Reason': label_reason,
        'Label_Exit_Type': label_exit_type,
        'Favorable_Move_Pct': round(float(favorable_move * 100.0), 4),
        'Adverse_Move_Pct': round(float(adverse_move * 100.0), 4),
        'Max_Favorable_Excursion': round(float(max_favorable_excursion * 100.0), 4),
        'Max_Adverse_Excursion': round(float(max_adverse_excursion * 100.0), 4),
        'Realized_Return_After_Cost': round(float(realized_return * 100.0), 4),
        'Direction': 'SHORT' if is_short else 'LONG',
    }



def _derive_range_confidence(signal_row: pd.Series) -> float:
    adx = _safe_float(signal_row.get('ADX14', signal_row.get('ADX', 0.0)), 0.0)
    bb_width = _safe_float(signal_row.get('BB_Width', signal_row.get('BB_std', 0.0)), 0.0)
    width_score = max(0.0, min(1.0, 1.0 - min(bb_width, 0.20) / 0.20))
    adx_score = max(0.0, min(1.0, 1.0 - min(adx, 40.0) / 40.0))
    explicit = signal_row.get('Range_Confidence', None)
    if explicit is not None:
        try:
            return max(0.0, min(1.0, float(explicit)))
        except Exception:
            pass
    return round((width_score * 0.55) + (adx_score * 0.45), 4)


def _build_directional_label_block(base_block: dict[str, Any], signal_row: pd.Series, regime: str) -> dict[str, Any]:
    direction = str(base_block.get('Direction', 'LONG')).upper()
    realized = _safe_float(base_block.get('Realized_Return_After_Cost', 0.0), 0.0)
    favorable = _safe_float(base_block.get('Favorable_Move_Pct', 0.0), 0.0)
    adverse = _safe_float(base_block.get('Adverse_Move_Pct', 0.0), 0.0)
    stop_hit = int(base_block.get('Stop_Hit', 0) or 0)
    range_conf = _derive_range_confidence(signal_row)
    is_range_regime = '盤整' in str(regime)
    long_y = int(direction == 'LONG' and int(base_block.get('Label_Y', 0) or 0) == 1)
    short_y = int(direction == 'SHORT' and int(base_block.get('Label_Y', 0) or 0) == 1)
    # range label uses range regime + decent confidence + no stop and non-negative realized outcome
    range_y = int(is_range_regime and range_conf >= float(PARAMS.get('RANGE_MIN_CONFIDENCE', 0.55)) and stop_hit == 0 and (realized >= 0.0 or favorable >= abs(adverse)))
    if range_y:
        strategy_bucket = 'RANGE'
    elif short_y:
        strategy_bucket = 'SHORT'
    else:
        strategy_bucket = 'LONG'
    return {
        'Long_Label_Y': long_y,
        'Short_Label_Y': short_y,
        'Range_Label_Y': range_y,
        'Long_Target_Return': round(realized if direction == 'LONG' else 0.0, 4),
        'Short_Target_Return': round(realized if direction == 'SHORT' else 0.0, 4),
        'Range_Target_Return': round(realized if is_range_regime else 0.0, 4),
        'Long_MAE': round(adverse if direction == 'LONG' else 0.0, 4),
        'Short_MAE': round(adverse if direction == 'SHORT' else 0.0, 4),
        'Range_MAE': round(adverse if is_range_regime else 0.0, 4),
        'Long_MFE': round(favorable if direction == 'LONG' else 0.0, 4),
        'Short_MFE': round(favorable if direction == 'SHORT' else 0.0, 4),
        'Range_MFE': round(favorable if is_range_regime else 0.0, 4),
        'Long_Exit_Type': str(base_block.get('Label_Exit_Type')) if direction == 'LONG' else None,
        'Short_Exit_Type': str(base_block.get('Label_Exit_Type')) if direction == 'SHORT' else None,
        'Range_Exit_Type': str(base_block.get('Label_Exit_Type')) if is_range_regime else None,
        'Range_Confidence_At_Label': range_conf,
        'Strategy_Bucket': strategy_bucket,
    }

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

                label_block = _build_execution_aware_label(computed_df, i, hold_days, setup_tag)
                if not label_block:
                    continue

                feats = _features.extract_ai_features(
                    row.to_dict(),
                    history_df=computed_df.iloc[:i + 1].copy(),
                    ticker=ticker,
                    as_of_date=row.name if hasattr(row, 'name') else None,
                )
                mounted = _features.select_live_features(feats)
                sample = {
                    'Ticker SYMBOL': ticker,
                    'Ticker': ticker,
                    'Date': pd.to_datetime(row.name, errors='coerce').strftime('%Y-%m-%d') if hasattr(row, 'name') and pd.notna(row.name) else None,
                    'Setup_Tag': setup_tag,
                    'Setup': setup_tag,
                    'Regime': regime,
                }
                sample.update(label_block)
                sample.update(_build_directional_label_block(label_block, row, regime))
                sample.update(feats)
                for k, v in mounted.items():
                    sample[f'MOUNT__{k}'] = v
                rows.append(sample)
        except Exception:
            continue

    df_out = pd.DataFrame(rows)
    df_out, quality_report = sanitize_generated_training_df(df_out)
    df_out.to_csv(dataset_path, index=False, encoding='utf-8-sig')
    if not df_out.empty:
        _features.write_training_feature_registry(
            sample_row=df_out.iloc[0].to_dict(),
            dataset_columns=df_out.columns.tolist(),
        )
    return df_out


if __name__ == '__main__':
    tickers = get_dynamic_watchlist()
    generate_ml_dataset(tickers)
