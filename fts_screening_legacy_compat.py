# -*- coding: utf-8 -*-
"""Legacy helper shims absorbed from advanced_chart(1).zip/screening.py.

目的：保留舊 helper 名稱，避免第一級橋接後舊模組找不到函式。
真正的 inspect_stock / smart_download / AI feature 主線仍由 fts_* service 處理。
"""
from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

try:
    from config import PARAMS, FINMIND_API_TOKEN  # type: ignore
except Exception:  # pragma: no cover
    PARAMS = {}
    FINMIND_API_TOKEN = ''

try:
    from FinMind.data import DataLoader  # type: ignore
except Exception:  # pragma: no cover
    DataLoader = None


def _build_finmind_loader():
    if DataLoader is None:
        return None
    try:
        if FINMIND_API_TOKEN:
            try:
                return DataLoader(token=FINMIND_API_TOKEN)
            except TypeError:
                dl = DataLoader()
                if hasattr(dl, 'login_by_token'):
                    dl.login_by_token(api_token=FINMIND_API_TOKEN)
                return dl
    except Exception:
        return None
    return None


def add_fundamental_filter(ticker: str, p: Mapping[str, Any] = PARAMS):
    dl = _build_finmind_loader()
    pure_ticker = str(ticker).split('.')[0]
    if dl is None:
        return {"營收年增率(%)": 0.000, "營業利益率(%)": 0.000, "基本面總分": 0}
    try:
        rev_df = dl.taiwan_stock_month_revenue(stock_id=pure_ticker)
        rev_yoy = float(rev_df.iloc[-1].get('revenue_year_growth', 0.0)) if not rev_df.empty else 0.0
        st_df = dl.taiwan_stock_financial_statement(stock_id=pure_ticker)
        op_margin = 0.0
        if not st_df.empty and 'type' in st_df.columns:
            op_margin_row = st_df[st_df['type'] == 'OperatingProfitMargin']
            if not op_margin_row.empty:
                op_margin = float(op_margin_row.iloc[-1].get('value', 0.0))
        f_score = 0
        if rev_yoy > float(p.get('FUNDAMENTAL_YOY_BASE', 0)):
            f_score += 1
        if rev_yoy > float(p.get('FUNDAMENTAL_YOY_EXCELLENT', 20)):
            f_score += 1
        if op_margin > float(p.get('FUNDAMENTAL_OPM_BASE', 0)):
            f_score += 1
        if op_margin < float(p.get('FUNDAMENTAL_OPM_BASE', 0)):
            f_score -= 2
        return {"營收年增率(%)": rev_yoy, "營業利益率(%)": op_margin, "基本面總分": f_score}
    except Exception:
        return {"營收年增率(%)": 0.000, "營業利益率(%)": 0.000, "基本面總分": 0}


def apply_slippage(price: float, direction: int, slippage: float):
    return float(price) * (1 + float(slippage) * int(direction))


def get_exit_price(entry_price: float, open_price: float, sl_pct: float, direction: int):
    stop_price = float(entry_price) * (1 - float(sl_pct) * int(direction))
    if (int(direction) == 1 and float(open_price) < stop_price) or (int(direction) == -1 and float(open_price) > stop_price):
        return float(open_price)
    return stop_price


def get_tp_price(entry_price: float, open_price: float, tp_pct: float, direction: int):
    target_price = float(entry_price) * (1 + float(tp_pct) * int(direction))
    if (int(direction) == 1 and float(open_price) > target_price) or (int(direction) == -1 and float(open_price) < target_price):
        return float(open_price)
    return target_price


def calculate_pnl(direction: int, entry_price: float, exit_price: float, shares: float, fee_rate: float, tax_rate: float):
    invested = float(entry_price) * float(shares)
    if int(direction) == 1:
        entry_cost = invested * (1 + float(fee_rate))
        exit_value = float(exit_price) * float(shares) * (1 - float(fee_rate) - float(tax_rate))
        pnl = exit_value - entry_cost
    else:
        entry_value = invested * (1 - float(fee_rate) - float(tax_rate))
        exit_cost = float(exit_price) * float(shares) * (1 + float(fee_rate))
        pnl = entry_value - exit_cost
    return pnl, invested


def _get_score_weights(p: Mapping[str, Any] = PARAMS):
    return {
        'c2_rsi': float(p.get('W_C2_RSI', 0.5)),
        'c3_volume': float(p.get('W_C3_VOLUME', 0.5)),
        'c4_macd': float(p.get('W_C4_MACD', 1.5)),
        'c5_boll_reversal': float(p.get('W_C5_BOLL', 0.5)),
        'c6_bbi': float(p.get('W_C6_BBI', 2.0)),
        'c7_foreign': float(p.get('W_C7_FOREIGN', 0.7)),
        'c8_dmi_adx': float(p.get('W_C8_DMI_ADX', 2.0)),
        'c9_total_ratio': float(p.get('W_C9_TOTAL_RATIO', 0.3)),
    }


def _apply_weighted_scores(df: pd.DataFrame, p: Mapping[str, Any] = PARAMS):
    out = df.copy()
    weights = _get_score_weights(p)
    buy_cols = [f'buy_c{i}' for i in range(2, 10)]
    sell_cols = [f'sell_c{i}' for i in range(2, 10)]
    for col in buy_cols + sell_cols:
        if col not in out.columns:
            out[col] = 0
    out['Weighted_Buy_Score'] = (
        out['buy_c2'] * weights['c2_rsi'] +
        out['buy_c3'] * weights['c3_volume'] +
        out['buy_c4'] * weights['c4_macd'] +
        out['buy_c5'] * weights['c5_boll_reversal'] +
        out['buy_c6'] * weights['c6_bbi'] +
        out['buy_c7'] * weights['c7_foreign'] +
        out['buy_c8'] * weights['c8_dmi_adx'] +
        out['buy_c9'] * weights['c9_total_ratio']
    )
    out['Weighted_Sell_Score'] = (
        out['sell_c2'] * weights['c2_rsi'] +
        out['sell_c3'] * weights['c3_volume'] +
        out['sell_c4'] * weights['c4_macd'] +
        out['sell_c5'] * weights['c5_boll_reversal'] +
        out['sell_c6'] * weights['c6_bbi'] +
        out['sell_c7'] * weights['c7_foreign'] +
        out['sell_c8'] * weights['c8_dmi_adx'] +
        out['sell_c9'] * weights['c9_total_ratio']
    )
    out['Score_Gap'] = out['Weighted_Buy_Score'] - out['Weighted_Sell_Score']
    return out


def _assign_golden_type(df: pd.DataFrame, trigger_score: float):
    out = df.copy()
    trigger = max(2.0, float(trigger_score))
    buy_active = out.get('Weighted_Buy_Score', pd.Series(0, index=out.index)) >= trigger
    sell_active = out.get('Weighted_Sell_Score', pd.Series(0, index=out.index)) >= trigger
    out['Signal_Conflict'] = (buy_active & sell_active).astype(int)
    out['Golden_Type'] = '無'
    out.loc[buy_active & (~sell_active), 'Golden_Type'] = '多方進場'
    out.loc[sell_active & (~buy_active), 'Golden_Type'] = '空方進場'
    both = buy_active & sell_active
    out.loc[both & (out['Weighted_Buy_Score'] > out['Weighted_Sell_Score']), 'Golden_Type'] = '多方進場'
    out.loc[both & (out['Weighted_Sell_Score'] > out['Weighted_Buy_Score']), 'Golden_Type'] = '空方進場'
    return out


def _compute_realized_signal_stats(df: pd.DataFrame, p: Mapping[str, Any] = PARAMS, hold_days: int = 5):
    fee_rate = float(p.get('FEE_RATE', 0.001425)) * float(p.get('FEE_DISCOUNT', 1.0))
    tax_rate = float(p.get('TAX_RATE', 0.003))
    min_samples = int(p.get('MIN_SIGNAL_SAMPLE_SIZE', 8))
    if df is None or df.empty or len(df) <= hold_days + 1:
        return {
            '系統勝率(%)': 50.0, '累計報酬率(%)': 0.0, '期望值': 0.0,
            '平均獲利(%)': 0.0, '平均虧損(%)': 0.0, '歷史訊號樣本數': 0,
            'Kelly建議倉位': 0.0, 'Realized_Signal_Returns': [],
        }
    realized_returns = []
    valid_df = df.copy().reset_index().rename(columns={df.index.name or 'index': 'TradeDate'})
    for i in range(1, len(valid_df) - hold_days):
        setup = str(valid_df.loc[i - 1, 'Golden_Type']).strip()
        if setup not in ('多方進場', '空方進場'):
            continue
        entry_open = valid_df.loc[i, 'Open']
        exit_close = valid_df.loc[i + hold_days - 1, 'Close']
        if pd.isna(entry_open) or pd.isna(exit_close) or float(entry_open) <= 0:
            continue
        direction = 1 if setup == '多方進場' else -1
        gross_ret = direction * ((float(exit_close) - float(entry_open)) / float(entry_open))
        net_ret = gross_ret - (2 * fee_rate + tax_rate)
        realized_returns.append(float(net_ret * 100.0))
    sample_size = len(realized_returns)
    if sample_size == 0:
        return {
            '系統勝率(%)': 50.0, '累計報酬率(%)': 0.0, '期望值': 0.0,
            '平均獲利(%)': 0.0, '平均虧損(%)': 0.0, '歷史訊號樣本數': 0,
            'Kelly建議倉位': 0.0, 'Realized_Signal_Returns': [],
        }
    wins = [r for r in realized_returns if r > 0]
    losses = [r for r in realized_returns if r <= 0]
    win_rate = (len(wins) / sample_size) * 100.0
    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = abs(float(np.mean(losses))) if losses else 0.0
    expectancy = float(np.mean(realized_returns))
    total_profit = float(np.sum(realized_returns))
    reward_risk_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0
    p_win = len(wins) / sample_size
    q_loss = 1 - p_win
    kelly_fraction = p_win - (q_loss / reward_risk_ratio) if reward_risk_ratio > 0 and expectancy > 0 else 0.0
    safe_kelly = max(0.0, min(0.30, kelly_fraction * 0.5))
    if sample_size < min_samples:
        shrink = sample_size / float(min_samples)
        win_rate = 50.0 + (win_rate - 50.0) * shrink
        expectancy = expectancy * shrink
        total_profit = total_profit * shrink
        safe_kelly = safe_kelly * shrink
    return {
        '系統勝率(%)': round(win_rate, 2),
        '累計報酬率(%)': round(total_profit, 2),
        '期望值': round(expectancy, 4),
        '平均獲利(%)': round(avg_win, 4),
        '平均虧損(%)': round(avg_loss, 4),
        '歷史訊號樣本數': sample_size,
        'Kelly建議倉位': round(safe_kelly, 4),
        'Realized_Signal_Returns': realized_returns,
    }
