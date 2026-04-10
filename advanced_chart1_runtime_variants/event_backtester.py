import os
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

from .config import PARAMS, WATCH_LIST
from .screening import smart_download, add_chip_data, inspect_stock, normalize_ticker_symbol
from .strategies import get_active_strategy

warnings.filterwarnings("ignore", category=UserWarning)


def _safe_float(x, default=0.0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def _direction_from_setup(setup_tag: str) -> int:
    tag = str(setup_tag)
    return -1 if ("空" in tag or "Short" in tag) else 1


def _fee_round_trip(p=PARAMS):
    fee_rate = float(p.get("FEE_RATE", 0.001425)) * float(p.get("FEE_DISCOUNT", 1.0))
    tax_rate = float(p.get("TAX_RATE", 0.003))
    return (2 * fee_rate) + tax_rate


def simulate_trade_path(
    price_df: pd.DataFrame,
    entry_idx: int,
    setup_tag: str,
    regime: str,
    p=PARAMS,
):
    """
    事件驅動模擬：
    - 次日開盤進場
    - 逐日檢查停損 / 第一階段停利 / 最終停利
    - 最後若都沒觸發，於最長持有日收盤出場
    """
    max_hold_days = int(p.get("BT_MAX_HOLD_DAYS", p.get("ML_LABEL_HOLD_DAYS", 5)))
    slippage = float(p.get("MARKET_SLIPPAGE", 0.0015))
    fee_roundtrip = _fee_round_trip(p)

    if entry_idx + 1 >= len(price_df):
        return None

    entry_row = price_df.iloc[entry_idx + 1]
    entry_price = _safe_float(entry_row.get("Open", np.nan), np.nan)
    if pd.isna(entry_price) or entry_price <= 0:
        return None

    direction = _direction_from_setup(setup_tag)
    strategy = get_active_strategy(setup_tag)

    # 用訊號日最後一根資料作為策略出場規則參考
    signal_row = price_df.iloc[entry_idx]
    curr_price = _safe_float(signal_row.get("Close", entry_price), entry_price)
    volatility_pct = _safe_float(signal_row.get("BB_std", 0.0), 0.0)
    if curr_price > 0:
        volatility_pct = max(0.01, (volatility_pct * 1.5) / curr_price)
    else:
        volatility_pct = 0.05

    trend_is_with_me = ("多頭" in str(regime) and direction == 1) or ("空頭" in str(regime) and direction == -1)
    adx_is_strong = _safe_float(signal_row.get("ADX14", 0.0), 0.0) > float(p.get("ADX_TREND_THRESHOLD", 20))
    entry_score = _safe_float(signal_row.get("Weighted_Buy_Score", signal_row.get("Buy_Score", 0.0)), 0.0)

    sl_pct, tp_pct, ignore_tp = strategy.get_exit_rules(
        p, volatility_pct, trend_is_with_me, adx_is_strong, entry_score
    )

    tp_stage = 0
    exit_reason = "MAX_HOLD_EXIT"
    exit_idx = None
    exit_price = None

    if direction == 1:
        stop_price = entry_price * (1 - sl_pct)
        tp_stage_1 = entry_price * (1 + tp_pct * 0.5)
        tp_final = entry_price * (1 + tp_pct)
    else:
        stop_price = entry_price * (1 + sl_pct)
        tp_stage_1 = entry_price * (1 - tp_pct * 0.5)
        tp_final = entry_price * (1 - tp_pct)

    last_idx = min(len(price_df) - 1, entry_idx + max_hold_days)

    for i in range(entry_idx + 1, last_idx + 1):
        bar = price_df.iloc[i]
        day_open = _safe_float(bar.get("Open", entry_price), entry_price)
        day_high = _safe_float(bar.get("High", day_open), day_open)
        day_low = _safe_float(bar.get("Low", day_open), day_open)
        day_close = _safe_float(bar.get("Close", day_open), day_open)

        if direction == 1:
            # 先檢查跳空停損
            if day_open <= stop_price:
                exit_reason = "GAP_STOP"
                exit_idx = i
                exit_price = day_open
                break

            # 再檢查日內停損
            if day_low <= stop_price:
                exit_reason = "INTRADAY_STOP"
                exit_idx = i
                exit_price = stop_price
                break

            # 第一階段停利
            if tp_stage == 0 and day_high >= tp_stage_1:
                tp_stage = 1

            # 最終停利
            if (not ignore_tp) and day_high >= tp_final:
                exit_reason = "FINAL_TP"
                exit_idx = i
                exit_price = tp_final
                break

        else:
            if day_open >= stop_price:
                exit_reason = "GAP_STOP"
                exit_idx = i
                exit_price = day_open
                break

            if day_high >= stop_price:
                exit_reason = "INTRADAY_STOP"
                exit_idx = i
                exit_price = stop_price
                break

            if tp_stage == 0 and day_low <= tp_stage_1:
                tp_stage = 1

            if (not ignore_tp) and day_low <= tp_final:
                exit_reason = "FINAL_TP"
                exit_idx = i
                exit_price = tp_final
                break

        # 若第一階段已達成，動態追蹤停損
        if tp_stage == 1:
            if direction == 1:
                stop_price = max(stop_price, day_close * (1 - sl_pct))
            else:
                stop_price = min(stop_price, day_close * (1 + sl_pct))

    if exit_idx is None:
        exit_idx = last_idx
        exit_price = _safe_float(price_df.iloc[last_idx].get("Close", entry_price), entry_price)

    # 模擬滑價
    if direction == 1:
        actual_exit = exit_price * (1 - slippage)
        gross_return = (actual_exit - entry_price) / entry_price
    else:
        actual_exit = exit_price * (1 + slippage)
        gross_return = (entry_price - actual_exit) / entry_price

    net_return = gross_return - fee_roundtrip

    return {
        "entry_date": price_df.index[entry_idx + 1],
        "exit_date": price_df.index[exit_idx],
        "entry_price": round(entry_price, 4),
        "exit_price": round(float(actual_exit), 4),
        "direction": direction,
        "setup_tag": setup_tag,
        "regime": regime,
        "sl_pct": round(sl_pct * 100, 3),
        "tp_pct": round(tp_pct * 100, 3),
        "ignore_tp": int(ignore_tp),
        "tp_stage_reached": int(tp_stage),
        "exit_reason": exit_reason,
        "holding_days": int(exit_idx - (entry_idx + 1) + 1),
        "gross_return_pct": round(gross_return * 100, 4),
        "net_return_pct": round(net_return * 100, 4),
    }


def backtest_single_ticker(ticker, period="3y", p=PARAMS):
    ticker = normalize_ticker_symbol(ticker)
    df = smart_download(ticker, period=period)
    if df.empty:
        return pd.DataFrame()

    df = add_chip_data(df, ticker)
    result = inspect_stock(ticker, preloaded_df=df, p=p)
    if not result or "計算後資料" not in result:
        return pd.DataFrame()

    computed_df = result["計算後資料"].copy()
    if computed_df.empty:
        return pd.DataFrame()

    trades = []

    for i in range(len(computed_df) - 2):
        row = computed_df.iloc[i]
        setup_tag = str(row.get("Golden_Type", "無")).strip()
        regime = str(row.get("Regime", "區間盤整")).strip()

        if setup_tag not in ("多方進場", "空方進場"):
            continue

        trade = simulate_trade_path(computed_df, i, setup_tag, regime, p=p)
        if trade is None:
            continue

        trade["Ticker"] = ticker
        trade["Weighted_Buy_Score"] = round(_safe_float(row.get("Weighted_Buy_Score", 0.0), 0.0), 3)
        trade["Weighted_Sell_Score"] = round(_safe_float(row.get("Weighted_Sell_Score", 0.0), 0.0), 3)
        trade["Score_Gap"] = round(_safe_float(row.get("Score_Gap", 0.0), 0.0), 3)
        trades.append(trade)

    return pd.DataFrame(trades)


def summarize_backtest(df_trades: pd.DataFrame):
    if df_trades is None or df_trades.empty:
        return {
            "sample_size": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "profit_factor": 0.0,
            "avg_holding_days": 0.0,
            "max_drawdown_like": 0.0,
        }

    returns = pd.to_numeric(df_trades["net_return_pct"], errors="coerce").dropna()
    if returns.empty:
        return {
            "sample_size": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "profit_factor": 0.0,
            "avg_holding_days": 0.0,
            "max_drawdown_like": 0.0,
        }

    wins = returns[returns > 0]
    losses = returns[returns <= 0]

    gross_profit = wins.sum() if len(wins) > 0 else 0.0
    gross_loss = abs(losses.sum()) if len(losses) > 0 else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 99.9

    cum = returns.cumsum()
    peak = cum.cummax()
    dd = cum - peak
    mdd_like = abs(dd.min()) if len(dd) > 0 else 0.0

    holding_days = pd.to_numeric(df_trades["holding_days"], errors="coerce").dropna()

    return {
        "sample_size": int(len(returns)),
        "win_rate": round(float((returns > 0).mean()), 4),
        "avg_return": round(float(returns.mean()), 4),
        "profit_factor": round(float(profit_factor), 4),
        "avg_holding_days": round(float(holding_days.mean()), 2) if not holding_days.empty else 0.0,
        "max_drawdown_like": round(float(mdd_like), 4),
    }


def run_batch_backtest(tickers=None, period="3y", output_dir="backtest_reports", p=PARAMS):
    if tickers is None:
        tickers = WATCH_LIST

    os.makedirs(output_dir, exist_ok=True)

    all_trades = []
    summary_rows = []

    for ticker in tickers:
        print(f"📡 回測中：{ticker}")
        df_trades = backtest_single_ticker(ticker, period=period, p=p)
        if df_trades.empty:
            continue

        summary = summarize_backtest(df_trades)
        summary["Ticker"] = normalize_ticker_symbol(ticker)
        summary_rows.append(summary)
        all_trades.append(df_trades)

    if all_trades:
        df_all = pd.concat(all_trades, ignore_index=True)
        df_all.to_csv(os.path.join(output_dir, "event_backtest_trades.csv"), index=False, encoding="utf-8-sig")
    else:
        df_all = pd.DataFrame()

    df_summary = pd.DataFrame(summary_rows)
    if not df_summary.empty:
        df_summary.sort_values(["avg_return", "win_rate", "profit_factor"], ascending=False, inplace=True)
        df_summary.to_csv(os.path.join(output_dir, "event_backtest_summary.csv"), index=False, encoding="utf-8-sig")

    return df_all, df_summary


if __name__ == "__main__":
    batch = WATCH_LIST[:5] if WATCH_LIST else ["2330.TW", "2317.TW", "2454.TW"]
    trades, summary = run_batch_backtest(batch)
    print("=" * 70)
    print("✅ 事件驅動回測完成")
    print(summary.head(20).to_string(index=False) if not summary.empty else "無摘要")