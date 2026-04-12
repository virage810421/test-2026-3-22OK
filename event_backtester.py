import warnings
import numpy as np
import pandas as pd

from config import PARAMS, WATCH_LIST
from screening import smart_download, add_chip_data, inspect_stock, normalize_ticker_symbol
from strategies import get_active_strategy

warnings.filterwarnings('ignore', category=UserWarning)


def _safe_float(x, default=0.0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def _direction_from_setup(setup_tag: str) -> int:
    tag = str(setup_tag)
    return -1 if ('空' in tag or 'Short' in tag) else 1


def _strategy_bucket(setup_tag: str, regime: str, row=None) -> str:
    direction = _direction_from_setup(setup_tag)
    range_conf = _safe_float((row or {}).get('Range_Confidence', (row or {}).get('Range_Confidence_At_Label', 0.0)), 0.0)
    if ('盤整' in str(regime)) and range_conf >= float(PARAMS.get('RANGE_MIN_CONFIDENCE', 0.55)):
        return 'RANGE'
    return 'SHORT' if direction < 0 else 'LONG'


def _fee_round_trip(p=PARAMS):
    fee_rate = float(p.get('FEE_RATE', 0.001425)) * float(p.get('FEE_DISCOUNT', 1.0))
    tax_rate = float(p.get('TAX_RATE', 0.003))
    return (2 * fee_rate) + tax_rate


def _strategy_overrides(strategy_bucket: str, p=PARAMS):
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


def simulate_trade_path(price_df: pd.DataFrame, entry_idx: int, setup_tag: str, regime: str, p=PARAMS):
    if entry_idx + 1 >= len(price_df):
        return None
    signal_row = price_df.iloc[entry_idx]
    strategy_bucket = _strategy_bucket(setup_tag, regime, signal_row)
    overrides = _strategy_overrides(strategy_bucket, p)
    max_hold_days = int(overrides['hold_days'])
    slippage = float(p.get('MARKET_SLIPPAGE', 0.0015)) * float(overrides['slippage_mult'])
    fee_roundtrip = _fee_round_trip(p)
    entry_row = price_df.iloc[entry_idx + 1]
    entry_price = _safe_float(entry_row.get('Open', np.nan), np.nan)
    if pd.isna(entry_price) or entry_price <= 0:
        return None
    direction = _direction_from_setup(setup_tag)
    strategy = get_active_strategy(setup_tag, regime=regime)
    curr_price = _safe_float(signal_row.get('Close', entry_price), entry_price)
    volatility_pct = _safe_float(signal_row.get('BB_std', 0.0), 0.0)
    if curr_price > 0:
        volatility_pct = max(0.01, (volatility_pct * 1.5) / curr_price)
    else:
        volatility_pct = 0.05
    trend_is_with_me = ('多頭' in str(regime) and direction == 1) or ('空頭' in str(regime) and direction == -1)
    adx_is_strong = _safe_float(signal_row.get('ADX14', 0.0), 0.0) > float(p.get('ADX_TREND_THRESHOLD', 20))
    entry_score = _safe_float(signal_row.get('Weighted_Buy_Score', signal_row.get('Buy_Score', 0.0)), 0.0)
    sl_pct, tp_pct, ignore_tp = strategy.get_exit_rules(p, volatility_pct, trend_is_with_me, adx_is_strong, entry_score)
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
    range_reversion = _safe_float(signal_row.get('Range_Mean_Reversion_Score', 0.0), 0.0)
    last_idx = min(len(price_df) - 1, entry_idx + max_hold_days)

    for i in range(entry_idx + 1, last_idx + 1):
        bar = price_df.iloc[i]
        day_open = _safe_float(bar.get('Open', entry_price), entry_price)
        day_high = _safe_float(bar.get('High', day_open), day_open)
        day_low = _safe_float(bar.get('Low', day_open), day_open)
        day_close = _safe_float(bar.get('Close', day_open), day_open)
        if direction == 1:
            mfe = max(mfe, (day_high - entry_price) / entry_price)
            mae = min(mae, (day_low - entry_price) / entry_price)
            if day_open <= stop_price:
                exit_reason = 'GAP_STOP'; exit_idx = i; exit_price = day_open; break
            if day_low <= stop_price:
                exit_reason = 'INTRADAY_STOP'; exit_idx = i; exit_price = stop_price; break
            if tp_stage == 0 and day_high >= tp_stage_1:
                tp_stage = 1
            if (not ignore_tp) and day_high >= tp_final:
                exit_reason = 'FINAL_TP'; exit_idx = i; exit_price = tp_final; break
        else:
            mfe = max(mfe, (entry_price - day_low) / entry_price)
            mae = min(mae, (entry_price - day_high) / entry_price)
            if day_open >= stop_price:
                exit_reason = 'GAP_STOP'; exit_idx = i; exit_price = day_open; break
            if day_high >= stop_price:
                exit_reason = 'INTRADAY_STOP'; exit_idx = i; exit_price = stop_price; break
            if tp_stage == 0 and day_low <= tp_stage_1:
                tp_stage = 1
            if (not ignore_tp) and day_low <= tp_final:
                exit_reason = 'FINAL_TP'; exit_idx = i; exit_price = tp_final; break
        if tp_stage == 1:
            if strategy_bucket == 'RANGE':
                exit_reason = 'RANGE_FAST_EXIT'; exit_idx = i; exit_price = day_close; break
            if direction == 1:
                stop_price = max(stop_price, day_close * (1 - sl_pct))
            else:
                stop_price = min(stop_price, day_close * (1 + sl_pct))

    if exit_idx is None:
        exit_idx = last_idx
        exit_price = _safe_float(price_df.iloc[last_idx].get('Close', entry_price), entry_price)
    if direction == 1:
        actual_exit = exit_price * (1 - slippage)
        gross_return = (actual_exit - entry_price) / entry_price
    else:
        actual_exit = exit_price * (1 + slippage)
        gross_return = (entry_price - actual_exit) / entry_price
    net_return = gross_return - fee_roundtrip
    gross_ev = gross_return
    net_ev = net_return
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
        'holding_days': int(exit_idx - (entry_idx + 1) + 1),
        'Trade_Holding_Days': int(exit_idx - (entry_idx + 1) + 1),
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
    if df.empty:
        return pd.DataFrame()
    df = add_chip_data(df, ticker)
    result = inspect_stock(ticker, preloaded_df=df, p=p)
    if not result or '計算後資料' not in result:
        return pd.DataFrame()
    computed_df = result['計算後資料'].copy()
    if computed_df.empty:
        return pd.DataFrame()
    trades = []
    for i in range(len(computed_df) - 2):
        row = computed_df.iloc[i]
        setup_tag = str(row.get('Golden_Type', '無')).strip()
        regime = str(row.get('Regime', '區間盤整')).strip()
        if setup_tag not in ('多方進場', '空方進場'):
            continue
        trade = simulate_trade_path(computed_df, i, setup_tag, regime, p=p)
        if trade is None:
            continue
        trade['Ticker'] = ticker
        trade['Weighted_Buy_Score'] = round(_safe_float(row.get('Weighted_Buy_Score', 0.0), 0.0), 3)
        trade['Weighted_Sell_Score'] = round(_safe_float(row.get('Weighted_Sell_Score', 0.0), 0.0), 3)
        trade['Score_Gap'] = round(_safe_float(row.get('Score_Gap', 0.0), 0.0), 3)
        trades.append(trade)
    return pd.DataFrame(trades)


def summarize_backtest(df_trades: pd.DataFrame):
    if df_trades is None or df_trades.empty:
        return {'sample_size': 0, 'win_rate': 0.0, 'avg_return': 0.0, 'profit_factor': 0.0, 'avg_holding_days': 0.0, 'max_drawdown_like': 0.0}
    returns = pd.to_numeric(df_trades['net_return_pct'], errors='coerce').dropna()
    if returns.empty:
        return {'sample_size': 0, 'win_rate': 0.0, 'avg_return': 0.0, 'profit_factor': 0.0, 'avg_holding_days': 0.0, 'max_drawdown_like': 0.0}
    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    gross_profit = wins.sum() if len(wins) > 0 else 0.0
    gross_loss = abs(losses.sum()) if len(losses) > 0 else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 99.9
    cum = returns.cumsum(); peak = cum.cummax(); dd = cum - peak
    out = {
        'sample_size': int(len(returns)),
        'win_rate': round(float((returns > 0).mean()) * 100, 2),
        'avg_return': round(float(returns.mean()), 4),
        'profit_factor': round(float(profit_factor), 3),
        'avg_holding_days': round(float(pd.to_numeric(df_trades['holding_days'], errors='coerce').fillna(0).mean()), 2),
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
        if not tdf.empty:
            all_trades.append(tdf)
    if all_trades:
        final_df = pd.concat(all_trades, ignore_index=True)
        print(final_df.tail(20))
        print(summarize_backtest(final_df))
    else:
        print('⚠️ 無可用回測結果。')
