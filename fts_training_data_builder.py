# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime
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
RUNTIME_PATH = Path('runtime/training_data_builder.json')


def _write_runtime(payload: dict[str, Any]) -> None:
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')



def get_dynamic_watchlist():
    try:
        from config import get_dynamic_training_universe  # type: ignore
        return get_dynamic_training_universe()
    except Exception:
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
    tp_pct = float(PARAMS.get('LABEL_TP_PCT', PARAMS.get('TP_BASE_PCT', max(sl_pct * 2.0, 0.06))))
    min_positive_return = float(PARAMS.get('LABEL_MIN_POSITIVE_RETURN', 0.0))
    round_trip_cost = _round_trip_cost()

    if is_short:
        max_adverse_excursion = max((_safe_float(future_window['High'].max(), entry_price) - entry_price) / entry_price, 0.0)
        max_favorable_excursion = max((entry_price - _safe_float(future_window['Low'].min(), entry_price)) / entry_price, 0.0)
        stop_hit = max_adverse_excursion > sl_pct
        touched_sl = int(stop_hit)
        touched_tp = int(max_favorable_excursion >= tp_pct)
        exit_price = _safe_float(future_window.iloc[-1].get('Close', entry_price), entry_price)
        realized_return = ((entry_price - exit_price) / entry_price) - round_trip_cost
        favorable_move = max_favorable_excursion
        adverse_move = max_adverse_excursion
    else:
        max_adverse_excursion = max((entry_price - _safe_float(future_window['Low'].min(), entry_price)) / entry_price, 0.0)
        max_favorable_excursion = max((_safe_float(future_window['High'].max(), entry_price) - entry_price) / entry_price, 0.0)
        stop_hit = max_adverse_excursion > sl_pct
        touched_sl = int(stop_hit)
        touched_tp = int(max_favorable_excursion >= tp_pct)
        exit_price = _safe_float(future_window.iloc[-1].get('Close', entry_price), entry_price)
        realized_return = ((exit_price - entry_price) / entry_price) - round_trip_cost
        favorable_move = max_favorable_excursion
        adverse_move = max_adverse_excursion

    label_y = 0
    if stop_hit:
        label_reason = 'stop_hit_before_target'
        label_exit_type = 'STOP'
    elif touched_tp:
        label_y = 1
        label_reason = 'tp_target_touched'
        label_exit_type = 'TP_TOUCH'
    elif realized_return > min_positive_return:
        label_y = 1
        label_reason = 'positive_realized_return_after_cost'
        label_exit_type = 'TIME_EXIT_PROFIT'
    else:
        label_reason = 'insufficient_edge'
        label_exit_type = 'TIME_EXIT_LOSS'

    entry_dt = pd.to_datetime(entry_row.name, errors='coerce')
    exit_dt = pd.to_datetime(future_window.index[-1], errors='coerce')

    setup_ready_label = int(_safe_float(computed_df.iloc[i].get('PreEntry_Score', 0.0), 0.0) >= float(PARAMS.get('PREENTRY_PILOT_THRESHOLD', 0.58)) and favorable_move * 100.0 >= float(PARAMS.get('SETUP_READY_MIN_FAVORABLE_PCT', 1.50)))
    trigger_confirm_label = int(_safe_float(computed_df.iloc[i].get('Confirm_Entry_Score', 0.0), 0.0) >= float(PARAMS.get('CONFIRM_FULL_THRESHOLD', 0.66)) and favorable_move * 100.0 >= float(PARAMS.get('TRIGGER_CONFIRM_MIN_FAVORABLE_PCT', 3.00)) and int(label_y) == 1)
    current_exit_hazard = _safe_float(computed_df.iloc[i].get('Exit_Hazard_Score', 0.0), 0.0)
    exit_state_at_label = str(computed_df.iloc[i].get('Exit_State', 'HOLD'))
    exit_defend_label = int(current_exit_hazard >= float(PARAMS.get('EXIT_LABEL_DEFEND_HAZARD', 0.55)) or adverse_move * 100.0 >= float(PARAMS.get('EXIT_LABEL_DEFEND_ADVERSE_PCT', 1.20)) or exit_state_at_label in {'DEFEND','REDUCE','EXIT'})
    exit_reduce_label = int(current_exit_hazard >= float(PARAMS.get('EXIT_LABEL_REDUCE_HAZARD', 0.68)) or adverse_move * 100.0 >= float(PARAMS.get('EXIT_LABEL_REDUCE_ADVERSE_PCT', 2.00)) or exit_state_at_label in {'REDUCE','EXIT'} or bool(stop_hit))
    exit_confirm_label = int(current_exit_hazard >= float(PARAMS.get('EXIT_LABEL_CONFIRM_HAZARD', 0.82)) or bool(stop_hit) or realized_return <= -abs(float(PARAMS.get('SL_MIN_PCT', 0.03))) * 0.75)
    return {
        'Label': int(label_y),
        'Label_Y': int(label_y),
        'Target_Return': round(float(realized_return), 6),
        'Target_Return_Unit': 'decimal_return',
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
        'Realized_Return_After_Cost': round(float(realized_return), 6),
        'Realized_Return_After_Cost_Pct': round(float(realized_return * 100.0), 4),
        'Direction': 'SHORT' if is_short else 'LONG',
        'Setup_Ready_Label': setup_ready_label,
        'Trigger_Confirm_Label': trigger_confirm_label,
        'Entry_State_At_Label': str(computed_df.iloc[i].get('Entry_State', 'NO_ENTRY')),
        'Early_Path_State_At_Label': str(computed_df.iloc[i].get('Early_Path_State', 'NO_ENTRY')),
        'Confirm_Path_State_At_Label': str(computed_df.iloc[i].get('Confirm_Path_State', 'WAIT_CONFIRM')),
        'Exit_Defend_Label': exit_defend_label,
        'Exit_Reduce_Label': exit_reduce_label,
        'Exit_Confirm_Label': exit_confirm_label,
        'Exit_State_At_Label': exit_state_at_label,
        'Target_Position_At_Label': _safe_float(computed_df.iloc[i].get('Target_Position', 0.0), 0.0),
        'Sample_Type': 'ENTRY_SIGNAL',
        'Is_Position_Day': 0,
        'Position_Day': 0,
        'Position_Age_Days': 0,
    }


def _build_position_day_label_records(
    computed_df: pd.DataFrame,
    i: int,
    hold_days: int,
    setup_tag: str,
    entry_label: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build one exit-training sample per simulated holding day."""
    if not bool(PARAMS.get('EXIT_MODEL_REQUIRE_POSITION_DAY_SAMPLES', True)):
        return []
    is_long, is_short = _signal_flags(setup_tag)
    if not (is_long or is_short):
        return []
    entry_idx = i + 1
    if entry_idx >= len(computed_df):
        return []
    entry_price = _safe_float(entry_label.get('Entry_Price'), 0.0)
    if entry_price <= 0:
        return []
    final_exit_price = _safe_float(entry_label.get('Exit_Price'), entry_price)
    sl_pct = float(PARAMS.get('SL_MIN_PCT', 0.03))
    round_trip_cost = _round_trip_cost()
    out: list[dict[str, Any]] = []
    max_day = min(int(hold_days), len(computed_df) - entry_idx)
    for age in range(1, max_day + 1):
        pos_idx = entry_idx + age - 1
        current_row = computed_df.iloc[pos_idx]
        window_to_now = computed_df.iloc[entry_idx:pos_idx + 1]
        current_close = _safe_float(current_row.get('Close', final_exit_price), final_exit_price)
        current_high = _safe_float(window_to_now['High'].max() if 'High' in window_to_now else current_close, current_close)
        current_low = _safe_float(window_to_now['Low'].min() if 'Low' in window_to_now else current_close, current_close)
        if is_short:
            unrealized_return = ((entry_price - current_close) / entry_price) - round_trip_cost
            remaining_return = ((current_close - final_exit_price) / current_close) - round_trip_cost if current_close > 0 else 0.0
            adverse_to_date = max((current_high - entry_price) / entry_price, 0.0)
            favorable_to_date = max((entry_price - current_low) / entry_price, 0.0)
        else:
            unrealized_return = ((current_close - entry_price) / entry_price) - round_trip_cost
            remaining_return = ((final_exit_price - current_close) / current_close) - round_trip_cost if current_close > 0 else 0.0
            adverse_to_date = max((entry_price - current_low) / entry_price, 0.0)
            favorable_to_date = max((current_high - entry_price) / entry_price, 0.0)
        hazard = _safe_float(current_row.get('Exit_Hazard_Score', 0.0), 0.0)
        exit_state = str(current_row.get('Exit_State', 'HOLD')).upper()
        stop_hit_to_date = adverse_to_date >= sl_pct
        defend_label = int(
            hazard >= float(PARAMS.get('EXIT_LABEL_DEFEND_HAZARD', 0.55))
            or adverse_to_date * 100.0 >= float(PARAMS.get('EXIT_LABEL_DEFEND_ADVERSE_PCT', 1.20))
            or exit_state in {'DEFEND', 'REDUCE', 'EXIT'}
            or unrealized_return <= -abs(sl_pct) * 0.35
        )
        reduce_label = int(
            hazard >= float(PARAMS.get('EXIT_LABEL_REDUCE_HAZARD', 0.68))
            or adverse_to_date * 100.0 >= float(PARAMS.get('EXIT_LABEL_REDUCE_ADVERSE_PCT', 2.00))
            or exit_state in {'REDUCE', 'EXIT'}
            or unrealized_return <= -abs(sl_pct) * 0.55
            or stop_hit_to_date
        )
        confirm_label = int(
            hazard >= float(PARAMS.get('EXIT_LABEL_CONFIRM_HAZARD', 0.82))
            or exit_state == 'EXIT'
            or stop_hit_to_date
            or unrealized_return <= -abs(sl_pct) * 0.75
            or remaining_return <= -abs(sl_pct) * 0.50
        )
        dt = pd.to_datetime(current_row.name, errors='coerce')
        out.append({
            **entry_label,
            'Sample_Type': 'POSITION_DAY',
            'Is_Position_Day': 1,
            'Position_Day': int(age),
            'Position_Age_Days': int(age),
            'Feature_Row_Index': int(pos_idx),
            'Date': dt.strftime('%Y-%m-%d') if pd.notna(dt) else None,
            'Position_Date': dt.strftime('%Y-%m-%d') if pd.notna(dt) else None,
            'Current_Close': round(float(current_close), 4),
            'Unrealized_Return': round(float(unrealized_return), 6),
            'Unrealized_Return_Pct': round(float(unrealized_return * 100.0), 4),
            'Remaining_Return_To_Planned_Exit': round(float(remaining_return), 6),
            'Remaining_Return_To_Planned_Exit_Pct': round(float(remaining_return * 100.0), 4),
            'Adverse_To_Date_Pct': round(float(adverse_to_date * 100.0), 4),
            'Favorable_To_Date_Pct': round(float(favorable_to_date * 100.0), 4),
            'Target_Return': round(float(remaining_return), 6),
            'Target_Return_Unit': 'decimal_return',
            'Future_Return_Pct': round(float(remaining_return * 100.0), 4),
            'Exit_Defend_Label': defend_label,
            'Exit_Reduce_Label': reduce_label,
            'Exit_Confirm_Label': confirm_label,
            'Exit_State_At_Label': exit_state,
            'Label_Reason': f'position_day_{age}_exit_training',
        })
    return out


def generate_ml_dataset(tickers=None):
    tickers = tickers or get_dynamic_watchlist() or ['2330.TW', '2317.TW', '2454.TW']
    os.makedirs('data', exist_ok=True)
    dataset_path = Path('data/ml_training_data.csv')
    rows = []
    ticker_reports: list[dict[str, Any]] = []
    hold_days = int(PARAMS.get('ML_LABEL_HOLD_DAYS', 5))
    base_columns = [
        'Ticker SYMBOL', 'Ticker', 'Date', 'Setup_Tag', 'Setup', 'Regime',
        'Sample_Type', 'Is_Position_Day', 'Position_Day', 'Position_Age_Days',
        'Label', 'Label_Y', 'Target_Return', 'Target_Return_Unit', 'Future_Return_Pct', 'Entry_Price', 'Exit_Price',
        'Entry_Date', 'Exit_Date', 'Hold_Days', 'Direction', 'Setup_Ready_Label', 'Trigger_Confirm_Label',
        'Entry_State_At_Label', 'Early_Path_State_At_Label', 'Confirm_Path_State_At_Label',
        'Exit_Defend_Label', 'Exit_Reduce_Label', 'Exit_Confirm_Label', 'Exit_State_At_Label', 'Target_Position_At_Label',
        'Current_Close', 'Unrealized_Return', 'Remaining_Return_To_Planned_Exit', 'Adverse_To_Date_Pct', 'Favorable_To_Date_Pct',
    ]

    for ticker in tickers:
        ticker_report: dict[str, Any] = {'ticker': ticker, 'rows_added': 0, 'status': 'init'}
        try:
            df = _market.smart_download(ticker, period='3y')
            ticker_report['download_rows'] = int(len(df)) if df is not None else 0
            if df is None or df.empty:
                ticker_report['status'] = 'skip_market_data_empty'
                ticker_reports.append(ticker_report)
                continue
            df = _chip.add_chip_data(df, ticker)
            result = _screen.inspect_stock(ticker, preloaded_df=df, p=PARAMS)
            if not result or '計算後資料' not in result:
                ticker_report['status'] = 'skip_screening_no_result'
                ticker_reports.append(ticker_report)
                continue
            computed_df = result['計算後資料'].copy()
            ticker_report['computed_rows'] = int(len(computed_df))
            if len(computed_df) <= hold_days + 2:
                ticker_report['status'] = 'skip_insufficient_history'
                ticker_reports.append(ticker_report)
                continue

            added = 0
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
                sample.update(feats)
                for k, v in mounted.items():
                    sample[f'MOUNT__{k}'] = v
                rows.append(sample)
                added += 1

                # v89: formal exit training uses position-day samples.
                for pos_label in _build_position_day_label_records(computed_df, i, hold_days, setup_tag, label_block):
                    pos_idx = int(pos_label.get('Feature_Row_Index', i))
                    if pos_idx < 0 or pos_idx >= len(computed_df):
                        continue
                    pos_row = computed_df.iloc[pos_idx]
                    pos_feats = _features.extract_ai_features(
                        pos_row.to_dict(),
                        history_df=computed_df.iloc[:pos_idx + 1].copy(),
                        ticker=ticker,
                        as_of_date=pos_row.name if hasattr(pos_row, 'name') else None,
                    )
                    pos_mounted = _features.select_live_features(pos_feats)
                    pos_sample = {
                        'Ticker SYMBOL': ticker,
                        'Ticker': ticker,
                        'Date': pos_label.get('Date'),
                        'Setup_Tag': setup_tag,
                        'Setup': setup_tag,
                        'Regime': str(pos_row.get('Regime', regime)).strip(),
                    }
                    pos_sample.update(pos_label)
                    pos_sample.update(pos_feats)
                    for k, v in pos_mounted.items():
                        pos_sample[f'MOUNT__{k}'] = v
                    rows.append(pos_sample)
                    added += 1
            ticker_report['rows_added'] = added
            ticker_report['status'] = 'ok' if added > 0 else 'skip_no_labelled_rows'
            ticker_reports.append(ticker_report)
        except Exception as e:
            ticker_report['status'] = 'error'
            ticker_report['error'] = f'{type(e).__name__}: {e}'
            ticker_reports.append(ticker_report)
            continue

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        df_out = pd.DataFrame(columns=base_columns)
    df_out, quality_report = sanitize_generated_training_df(df_out)
    if df_out.empty and not list(df_out.columns):
        df_out = pd.DataFrame(columns=base_columns)
    df_out.to_csv(dataset_path, index=False, encoding='utf-8-sig')
    if not df_out.empty:
        _features.write_training_feature_registry(
            sample_row=df_out.iloc[0].to_dict(),
            dataset_columns=df_out.columns.tolist(),
        )
    selected_features = _features.load_selected_features()
    if not df_out.empty:
        _features.write_feature_manifest(
            sample_row=df_out.iloc[0].to_dict(),
            dataset_columns=df_out.columns.tolist(),
            selected_features=selected_features,
        )
    advanced_cols = [
        'Score_Gap_Slope_3d','ADX_Delta_3d','MACD_Hist_Delta_3d','RSI_Reclaim_Speed','BB_Squeeze_Release',
        'ATR_Expansion_Start','Volume_Z20_Delta','Foreign_Ratio_Delta_3d','Total_Ratio_Delta_3d','Bull_Emerging_Score',
        'Bear_Emerging_Score','Range_Compression_Score','Breakout_Readiness','Trend_Exhaustion_Score','Entry_Readiness',
        'Breakout_Risk_Next3','Reversal_Risk_Next3','Exit_Hazard_Score','Proba_Delta_3d','Trend_Confidence_Delta','Range_Confidence_Delta',
        'Regime_Label','Regime_Confidence','Next_Regime_Prob_Bull','Next_Regime_Prob_Bear','Next_Regime_Prob_Range','Transition_Label',
    ]
    payload = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'dataset_path': str(dataset_path),
        'status': 'ok' if len(df_out) > 0 else 'empty_dataset',
        'tickers_requested': len(tickers),
        'rows_generated': int(len(df_out)),
        'quality_report': quality_report,
        'ticker_reports': ticker_reports[:200],
        'selected_feature_count': int(len(selected_features)),
        'advanced_feature_columns_present': [c for c in advanced_cols if c in df_out.columns],
        'advanced_feature_column_count': int(sum(1 for c in advanced_cols if c in df_out.columns)),
        'feature_manifest_path': str(_features.feature_manifest_path),
    }
    _write_runtime(payload)
    print(f"🧪 training data builder status: {payload['status']} | rows={payload['rows_generated']}")
    print(f"📄 report: {RUNTIME_PATH}")
    return df_out


if __name__ == '__main__':
    tickers = get_dynamic_watchlist()
    generate_ml_dataset(tickers)
