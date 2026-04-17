import warnings
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd

from config import PARAMS, WATCH_LIST
from fts_service_api import (
    add_chip_data,
    get_active_strategy,
    inspect_stock,
    normalize_ticker_symbol,
    smart_download,
)

warnings.filterwarnings('ignore', category=UserWarning)


def _extract_scalar(x: Any, default: Any = None) -> Any:
    """Convert common pandas/numpy container values into a single scalar.

    This avoids ambiguous truth-value errors when a Series/array sneaks into
    downstream numeric or boolean logic.
    """
    try:
        if isinstance(x, pd.DataFrame):
            if x.empty:
                return default
            return _extract_scalar(x.iloc[-1, -1], default)
        if isinstance(x, pd.Series):
            if x.empty:
                return default
            return _extract_scalar(x.iloc[-1], default)
        if isinstance(x, pd.Index):
            if len(x) == 0:
                return default
            return _extract_scalar(x[-1], default)
        if isinstance(x, np.ndarray):
            if x.size == 0:
                return default
            return _extract_scalar(x.reshape(-1)[-1], default)
        if isinstance(x, (list, tuple)):
            if not x:
                return default
            return _extract_scalar(x[-1], default)
        return x
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        x = _extract_scalar(x, default)
        if x is None:
            return default
        na_flag = pd.isna(x)
        if isinstance(na_flag, (pd.Series, pd.DataFrame, np.ndarray, list, tuple, pd.Index)):
            return default
        if bool(na_flag):
            return default
        return float(x)
    except Exception:
        return default


def _safe_bool(x: Any, default: bool = False) -> bool:
    try:
        x = _extract_scalar(x, default)
        if x is None:
            return default
        if isinstance(x, str):
            t = x.strip().lower()
            if t in {'1', 'true', 't', 'yes', 'y'}:
                return True
            if t in {'0', 'false', 'f', 'no', 'n', ''}:
                return False
        return bool(x)
    except Exception:
        return default


def _row_get(row: Any, key: str, default: Any = 0.0) -> Any:
    try:
        if row is None:
            return default
        if isinstance(row, pd.Series):
            return row.get(key, default)
        if isinstance(row, dict):
            return row.get(key, default)
        return getattr(row, key, default)
    except Exception:
        return default


class DirectionBucket(str, Enum):
    LONG = 'LONG'
    SHORT = 'SHORT'


class RegimeBucket(str, Enum):
    TREND = 'TREND'
    RANGE = 'RANGE'
    NEUTRAL = 'NEUTRAL'


SHORT_TOKENS = ('空', 'SHORT', 'SHORT_ENTRY', 'BEAR')
RANGE_TOKENS = ('盤整', '區間', 'RANGE', 'CONSOLIDATION')
TREND_TOKENS = ('趨勢', 'TREND', 'BREAKOUT', 'MOMENTUM')
LONG_SETUP_TOKENS = ('多', 'LONG', 'LONG_ENTRY', 'BULL')
LONG_REGIME_TOKENS = ('多頭', 'BULL', 'UPTREND', 'LONG')
SHORT_REGIME_TOKENS = ('空頭', 'BEAR', 'DOWNTREND', 'SHORT')
DEFAULT_REGIME_LABEL = RegimeBucket.RANGE.value


def _contains_any_token(raw: Any, tokens: tuple[str, ...]) -> bool:
    text = str(raw or '').strip().upper()
    return any(token.upper() in text for token in tokens)


def _direction_from_setup(setup_tag: str) -> int:
    return -1 if _contains_any_token(setup_tag, SHORT_TOKENS) else 1


def _is_supported_entry_setup(setup_tag: str) -> bool:
    return _contains_any_token(setup_tag, LONG_SETUP_TOKENS) or _contains_any_token(setup_tag, SHORT_TOKENS)


def _regime_trend_alignment(regime: Any, direction: int) -> bool:
    if direction < 0:
        return _contains_any_token(regime, SHORT_REGIME_TOKENS)
    return _contains_any_token(regime, LONG_REGIME_TOKENS)


def _normalize_regime_bucket(regime: Any) -> RegimeBucket:
    if _contains_any_token(regime, RANGE_TOKENS):
        return RegimeBucket.RANGE
    if _contains_any_token(regime, TREND_TOKENS):
        return RegimeBucket.TREND
    return RegimeBucket.NEUTRAL


def _strategy_bucket(setup_tag: str, regime: str, row: Any = None) -> str:
    direction = _direction_from_setup(setup_tag)
    range_conf = _safe_float(
        _row_get(row, 'Range_Confidence', _row_get(row, 'Range_Confidence_At_Label', 0.0)),
        0.0,
    )
    regime_bucket = _normalize_regime_bucket(regime)
    if regime_bucket == RegimeBucket.RANGE and range_conf >= float(PARAMS.get('RANGE_MIN_CONFIDENCE', 0.55)):
        return RegimeBucket.RANGE.value
    return DirectionBucket.SHORT.value if direction < 0 else DirectionBucket.LONG.value


def _fee_round_trip(p=PARAMS) -> float:
    fee_rate = float(p.get('FEE_RATE', 0.001425)) * float(p.get('FEE_DISCOUNT', 1.0))
    tax_rate = float(p.get('TAX_RATE', 0.003))
    return (2 * fee_rate) + tax_rate


def _strategy_overrides(strategy_bucket: str, p=PARAMS) -> dict[str, float | int]:
    if strategy_bucket == 'SHORT':
        return {
            'hold_days': int(p.get('SHORT_BACKTEST_HOLD_DAYS', p.get('ML_LABEL_HOLD_DAYS', 4))),
            'slippage_mult': float(p.get('SHORT_SLIPPAGE_MULTIPLIER', 1.25)),
            'tp_mult': float(p.get('SHORT_TP_MULTIPLIER', 0.9)),
            'sl_mult': float(p.get('SHORT_SL_MULTIPLIER', 1.0)),
        }
    if strategy_bucket == 'RANGE':
        return {
            'hold_days': int(p.get('RANGE_BACKTEST_HOLD_DAYS', max(2, int(p.get('ML_LABEL_HOLD_DAYS', 5)) - 2))),
            'slippage_mult': float(p.get('RANGE_SLIPPAGE_MULTIPLIER', 1.05)),
            'tp_mult': float(p.get('RANGE_TP_MULTIPLIER', 0.7)),
            'sl_mult': float(p.get('RANGE_SL_MULTIPLIER', 0.8)),
        }
    return {
        'hold_days': int(p.get('BT_MAX_HOLD_DAYS', p.get('ML_LABEL_HOLD_DAYS', 5))),
        'slippage_mult': 1.0,
        'tp_mult': 1.0,
        'sl_mult': 1.0,
    }


def _safe_get_exit_rules(strategy: Any, p: dict[str, Any], volatility_pct: float, trend_is_with_me: bool, adx_is_strong: bool, entry_score: float) -> tuple[float, float, bool]:
    """Normalize strategy exit-rule responses and fall back safely if needed."""
    default_sl = float(p.get('SL_MIN_PCT', 0.03))
    default_tp = float(p.get('TP_MIN_PCT', 0.06))
    try:
        if strategy is None or not hasattr(strategy, 'get_exit_rules'):
            return default_sl, default_tp, False
        raw = strategy.get_exit_rules(p, volatility_pct, trend_is_with_me, adx_is_strong, entry_score)
        if not isinstance(raw, (list, tuple)) or len(raw) < 3:
            return default_sl, default_tp, False
        sl_pct = max(0.001, _safe_float(raw[0], default_sl))
        tp_pct = max(0.001, _safe_float(raw[1], default_tp))
        ignore_tp = _safe_bool(raw[2], False)
        return sl_pct, tp_pct, ignore_tp
    except Exception:
        return default_sl, default_tp, False


def simulate_trade_path(price_df: pd.DataFrame, entry_idx: int, setup_tag: str, regime: str, p=PARAMS):
    if price_df is None or price_df.empty:
        return None
    if entry_idx + 1 >= len(price_df):
        return None
    signal_row = price_df.iloc[entry_idx]
    strategy_bucket = _strategy_bucket(setup_tag, regime, signal_row)
    overrides = _strategy_overrides(strategy_bucket, p)
    max_hold_days = max(1, int(overrides['hold_days']))
    slippage = float(p.get('MARKET_SLIPPAGE', 0.0015)) * float(overrides['slippage_mult'])
    fee_roundtrip = _fee_round_trip(p)

    entry_row = price_df.iloc[entry_idx + 1]
    entry_price = _safe_float(_row_get(entry_row, 'Open', np.nan), np.nan)
    if pd.isna(entry_price) or entry_price <= 0:
        return None

    direction = _direction_from_setup(setup_tag)
    strategy = get_active_strategy(setup_tag, regime=regime)
    curr_price = _safe_float(_row_get(signal_row, 'Close', entry_price), entry_price)
    volatility_pct = _safe_float(_row_get(signal_row, 'BB_std', 0.0), 0.0)
    volatility_pct = max(0.01, (volatility_pct * 1.5) / curr_price) if curr_price > 0 else 0.05
    trend_is_with_me = _regime_trend_alignment(regime, direction)
    adx_is_strong = _safe_float(_row_get(signal_row, 'ADX14', 0.0), 0.0) > float(p.get('ADX_TREND_THRESHOLD', 20))
    entry_score = _safe_float(_row_get(signal_row, 'Weighted_Buy_Score', _row_get(signal_row, 'Buy_Score', 0.0)), 0.0)
    sl_pct, tp_pct, ignore_tp = _safe_get_exit_rules(strategy, p, volatility_pct, trend_is_with_me, adx_is_strong, entry_score)
    sl_pct *= float(overrides['sl_mult'])
    tp_pct *= float(overrides['tp_mult'])

    if direction == 1:
        stop_price = entry_price * (1 - sl_pct)
        tp_stage_1 = entry_price * (1 + tp_pct * 0.5)
        tp_final = entry_price * (1 + tp_pct)
    else:
        stop_price = entry_price * (1 + sl_pct)
        tp_stage_1 = entry_price * (1 - tp_pct * 0.5)
        tp_final = entry_price * (1 - tp_pct)

    tp_stage = 0
    exit_reason = 'MAX_HOLD_EXIT'
    exit_idx = None
    exit_price = None
    mfe = 0.0
    mae = 0.0
    range_reversion = _safe_float(_row_get(signal_row, 'Range_Mean_Reversion_Score', 0.0), 0.0)
    last_idx = min(len(price_df) - 1, entry_idx + max_hold_days)
    if last_idx <= entry_idx:
        return None

    for i in range(entry_idx + 1, last_idx + 1):
        bar = price_df.iloc[i]
        day_open = _safe_float(_row_get(bar, 'Open', entry_price), entry_price)
        day_high = _safe_float(_row_get(bar, 'High', day_open), day_open)
        day_low = _safe_float(_row_get(bar, 'Low', day_open), day_open)
        day_close = _safe_float(_row_get(bar, 'Close', day_open), day_open)

        if direction == 1:
            mfe = max(mfe, (day_high - entry_price) / entry_price)
            mae = min(mae, (day_low - entry_price) / entry_price)
            if day_open <= stop_price:
                exit_reason = 'GAP_STOP'
                exit_idx = i
                exit_price = day_open
                break
            if day_low <= stop_price:
                exit_reason = 'INTRADAY_STOP'
                exit_idx = i
                exit_price = stop_price
                break
            if tp_stage == 0 and day_high >= tp_stage_1:
                tp_stage = 1
            if (not ignore_tp) and day_high >= tp_final:
                exit_reason = 'FINAL_TP'
                exit_idx = i
                exit_price = tp_final
                break
        else:
            mfe = max(mfe, (entry_price - day_low) / entry_price)
            mae = min(mae, (entry_price - day_high) / entry_price)
            if day_open >= stop_price:
                exit_reason = 'GAP_STOP'
                exit_idx = i
                exit_price = day_open
                break
            if day_high >= stop_price:
                exit_reason = 'INTRADAY_STOP'
                exit_idx = i
                exit_price = stop_price
                break
            if tp_stage == 0 and day_low <= tp_stage_1:
                tp_stage = 1
            if (not ignore_tp) and day_low <= tp_final:
                exit_reason = 'FINAL_TP'
                exit_idx = i
                exit_price = tp_final
                break

        if tp_stage == 1:
            if strategy_bucket == 'RANGE':
                exit_reason = 'RANGE_FAST_EXIT'
                exit_idx = i
                exit_price = day_close
                break
            if direction == 1:
                stop_price = max(stop_price, day_close * (1 - sl_pct))
            else:
                stop_price = min(stop_price, day_close * (1 + sl_pct))

    if exit_idx is None:
        exit_idx = last_idx
        exit_price = _safe_float(_row_get(price_df.iloc[last_idx], 'Close', entry_price), entry_price)

    if pd.isna(exit_price) or exit_price <= 0:
        return None

    if direction == 1:
        actual_exit = exit_price * (1 - slippage)
        gross_return = (actual_exit - entry_price) / entry_price
    else:
        actual_exit = exit_price * (1 + slippage)
        gross_return = (entry_price - actual_exit) / entry_price

    net_return = gross_return - fee_roundtrip
    gross_ev = gross_return
    net_ev = net_return
    holding_days = max(1, int(exit_idx - (entry_idx + 1) + 1))

    return {
        'entry_date': price_df.index[entry_idx + 1],
        'exit_date': price_df.index[exit_idx],
        'entry_price': round(entry_price, 4),
        'exit_price': round(float(actual_exit), 4),
        'direction': direction,
        'Direction_Bucket': 'SHORT' if direction < 0 else 'LONG',
        'Strategy_Bucket': strategy_bucket,
        'setup_tag': setup_tag,
        'regime': regime,
        'sl_pct': round(sl_pct * 100, 3),
        'tp_pct': round(tp_pct * 100, 3),
        'ignore_tp': int(ignore_tp),
        'tp_stage_reached': int(tp_stage),
        'exit_reason': exit_reason,
        'Exit_Type': exit_reason,
        'Entry_Type': 'NEXT_OPEN',
        'holding_days': holding_days,
        'Trade_Holding_Days': holding_days,
        'gross_return_pct': round(gross_return * 100, 4),
        'net_return_pct': round(net_return * 100, 4),
        'Gross_EV': round(gross_ev * 100, 4),
        'Net_EV': round(net_ev * 100, 4),
        'MFE': round(mfe * 100, 4),
        'MAE': round(mae * 100, 4),
        'Range_Reversion_Score_At_Entry': round(range_reversion, 4),
    }


def backtest_single_ticker(ticker, period='3y', p=PARAMS):
    ticker = normalize_ticker_symbol(ticker)
    df = smart_download(ticker, period=period)
    if df is None or df.empty:
        return pd.DataFrame()
    df = add_chip_data(df, ticker)
    if df is None or df.empty:
        return pd.DataFrame()

    result = inspect_stock(ticker, preloaded_df=df, p=p)
    if not isinstance(result, dict) or '計算後資料' not in result:
        return pd.DataFrame()

    computed_df = result['計算後資料']
    if not isinstance(computed_df, pd.DataFrame) or computed_df.empty:
        return pd.DataFrame()

    required_any = {'Open', 'High', 'Low', 'Close'}
    if not required_any.issubset(set(map(str, computed_df.columns))):
        return pd.DataFrame()

    computed_df = computed_df.copy().sort_index()
    trades = []
    for i in range(len(computed_df) - 2):
        row = computed_df.iloc[i]
        setup_tag = str(_row_get(row, 'Golden_Type', '無')).strip()
        regime = str(_row_get(row, 'Regime', DEFAULT_REGIME_LABEL)).strip()
        if not _is_supported_entry_setup(setup_tag):
            continue
        trade = simulate_trade_path(computed_df, i, setup_tag, regime, p=p)
        if trade is None:
            continue
        trade['Ticker'] = ticker
        trade['Weighted_Buy_Score'] = round(_safe_float(_row_get(row, 'Weighted_Buy_Score', 0.0), 0.0), 3)
        trade['Weighted_Sell_Score'] = round(_safe_float(_row_get(row, 'Weighted_Sell_Score', 0.0), 0.0), 3)
        trade['Score_Gap'] = round(_safe_float(_row_get(row, 'Score_Gap', 0.0), 0.0), 3)
        trades.append(trade)
    return pd.DataFrame(trades)


def summarize_backtest(df_trades: pd.DataFrame):
    if df_trades is None or df_trades.empty:
        return {
            'sample_size': 0,
            'win_rate': 0.0,
            'avg_return': 0.0,
            'profit_factor': 0.0,
            'avg_holding_days': 0.0,
            'max_drawdown_like': 0.0,
        }

    returns = pd.to_numeric(df_trades.get('net_return_pct'), errors='coerce').dropna()
    if returns.empty:
        return {
            'sample_size': 0,
            'win_rate': 0.0,
            'avg_return': 0.0,
            'profit_factor': 0.0,
            'avg_holding_days': 0.0,
            'max_drawdown_like': 0.0,
        }

    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    gross_profit = wins.sum() if len(wins) > 0 else 0.0
    gross_loss = abs(losses.sum()) if len(losses) > 0 else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 99.9
    cum = returns.cumsum()
    peak = cum.cummax()
    dd = cum - peak

    out = {
        'sample_size': int(len(returns)),
        'win_rate': round(float((returns > 0).mean()) * 100, 2),
        'avg_return': round(float(returns.mean()), 4),
        'profit_factor': round(float(profit_factor), 3),
        'avg_holding_days': round(float(pd.to_numeric(df_trades.get('holding_days'), errors='coerce').fillna(0).mean()), 2),
        'max_drawdown_like': round(float(dd.min()), 4),
    }
    if 'Strategy_Bucket' in df_trades.columns:
        out['by_strategy_bucket'] = {
            str(k): {
                'trades': int(len(v)),
                'win_rate': round(float((pd.to_numeric(v['net_return_pct'], errors='coerce').fillna(0) > 0).mean()) * 100, 2),
                'avg_return': round(float(pd.to_numeric(v['net_return_pct'], errors='coerce').fillna(0).mean()), 4),
            }
            for k, v in df_trades.groupby('Strategy_Bucket')
        }
    return out


if __name__ == '__main__':
    all_trades = []
    for ticker in WATCH_LIST:
        tdf = backtest_single_ticker(ticker)
        if tdf is not None and not tdf.empty:
            all_trades.append(tdf)
    if all_trades:
        final_df = pd.concat(all_trades, ignore_index=True)
        print(final_df.tail(20))
        print(summarize_backtest(final_df))
    else:
        print('⚠️ 無可用回測結果。')
