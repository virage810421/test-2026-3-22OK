# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    bucket: str
    source: str
    description: str
    is_new_in_v83_feature_stack: bool = False
    mounted_in_live_path: bool = True
    percentile_backed: bool = False
    event_calendar_precise: bool = False
    feature_family: str = 'shared'
    strategy_scope: str = 'SHARED'
    approval_scope: str = 'candidate_only'
    is_live_safe: bool = True


def _fs(name: str, bucket: str, source: str, description: str,
        is_new: bool = False, mounted: bool = True,
        percentile_backed: bool = False, event_calendar_precise: bool = False,
        feature_family: str = 'shared', strategy_scope: str = 'SHARED',
        approval_scope: str = 'candidate_only', is_live_safe: bool = True) -> FeatureSpec:
    return FeatureSpec(
        name, bucket, source, description, is_new, mounted, percentile_backed, event_calendar_precise,
        feature_family, strategy_scope, approval_scope, is_live_safe
    )


FEATURE_SPECS: Dict[str, FeatureSpec] = {
    'K_Body_Pct': _fs('K_Body_Pct', 'price_action', 'OHLCV', 'Candlestick body percentage.'),
    'Upper_Shadow': _fs('Upper_Shadow', 'price_action', 'OHLCV', 'Upper shadow relative size.'),
    'Lower_Shadow': _fs('Lower_Shadow', 'price_action', 'OHLCV', 'Lower shadow relative size.'),
    'Dist_to_MA20': _fs('Dist_to_MA20', 'price_action', 'OHLCV', 'Distance to 20-day moving average.'),
    'Volume_Ratio': _fs('Volume_Ratio', 'tradeability', 'OHLCV', 'Volume divided by 20-day volume MA.'),
    'BB_Width': _fs('BB_Width', 'volatility', 'OHLCV', 'Bollinger band width.'),
    'RSI': _fs('RSI', 'momentum', 'OHLCV', 'Relative strength index.'),
    'MACD_Hist': _fs('MACD_Hist', 'momentum', 'OHLCV', 'MACD histogram.'),
    'ADX': _fs('ADX', 'trend', 'OHLCV', 'Average directional index.'),
    'Foreign_Ratio': _fs('Foreign_Ratio', 'chip_flow', 'SQL daily_chip_data', 'Foreign net flow divided by price proxy.'),
    'Trust_Ratio': _fs('Trust_Ratio', 'chip_flow', 'SQL daily_chip_data', 'Trust net flow divided by price proxy.'),
    'Total_Ratio': _fs('Total_Ratio', 'chip_flow', 'SQL daily_chip_data', 'Combined net flow divided by price proxy.'),
    'Foreign_Consec_Days': _fs('Foreign_Consec_Days', 'chip_flow', 'SQL daily_chip_data', 'Consecutive positive foreign-flow days.'),
    'Trust_Consec_Days': _fs('Trust_Consec_Days', 'chip_flow', 'SQL daily_chip_data', 'Consecutive positive trust-flow days.'),
    'Weighted_Buy_Score': _fs('Weighted_Buy_Score', 'signal_score', 'screening/scoring', 'Weighted bullish score.'),
    'Weighted_Sell_Score': _fs('Weighted_Sell_Score', 'signal_score', 'screening/scoring', 'Weighted bearish score.'),
    'Score_Gap': _fs('Score_Gap', 'signal_score', 'screening/scoring', 'Buy score minus sell score.'),
    'Signal_Conflict': _fs('Signal_Conflict', 'risk', 'screening/scoring', 'Both buy and sell conditions active.'),
    'Trap_Signal': _fs('Trap_Signal', 'pattern', 'OHLCV', 'Fake breakout / bear trap state.'),
    'Vol_Squeeze': _fs('Vol_Squeeze', 'volatility', 'OHLCV', 'Low-volatility squeeze flag.'),
    'Absorption': _fs('Absorption', 'chip_flow', 'OHLCV + chip', 'Negative price move with positive chip support.'),
    'MR_Long_Spring': _fs('MR_Long_Spring', 'mean_reversion', 'OHLCV + chip', 'Spring-like long setup.'),
    'MR_Short_Trap': _fs('MR_Short_Trap', 'mean_reversion', 'OHLCV', 'Short-trap mean reversion setup.'),
    'MR_Long_Accumulation': _fs('MR_Long_Accumulation', 'mean_reversion', 'OHLCV + chip', 'Long accumulation state.'),
    'MR_Short_Distribution': _fs('MR_Short_Distribution', 'mean_reversion', 'OHLCV + chip', 'Short distribution state.'),
    'ATR14': _fs('ATR14', 'volatility', 'OHLCV', 'Average true range over 14 bars.', True),
    'ATR_Pct': _fs('ATR_Pct', 'volatility', 'OHLCV', 'ATR divided by close price.', True),
    'ATR_Pctl_252': _fs('ATR_Pctl_252', 'volatility', 'OHLCV', 'Rolling percentile rank of ATR_Pct over 252 bars.', True, True, True),
    'RealizedVol_20': _fs('RealizedVol_20', 'volatility', 'OHLCV', '20-day annualized realized volatility.', True),
    'RealizedVol_60': _fs('RealizedVol_60', 'volatility', 'OHLCV', '60-day annualized realized volatility.', True),
    'Gap_Pct': _fs('Gap_Pct', 'price_action', 'OHLCV', 'Open versus previous close gap.', True),
    'Overnight_Return': _fs('Overnight_Return', 'price_action', 'OHLCV', 'Previous close to open return.', True),
    'Intraday_Return': _fs('Intraday_Return', 'price_action', 'OHLCV', 'Open to close return.', True),
    'Turnover_Proxy': _fs('Turnover_Proxy', 'tradeability', 'OHLCV', 'Price times volume proxy for turnover.', True),
    'ADV20_Proxy': _fs('ADV20_Proxy', 'tradeability', 'OHLCV', '20-day average turnover proxy.', True),
    'DollarVol20_Proxy': _fs('DollarVol20_Proxy', 'tradeability', 'OHLCV', '20-day average dollar volume proxy.', True),
    'Volume_Z20': _fs('Volume_Z20', 'tradeability', 'OHLCV', '20-day z-score of volume.', True),
    'Return_Z20': _fs('Return_Z20', 'momentum', 'OHLCV', '20-day z-score of daily return.', True),
    'RS_vs_Market_20': _fs('RS_vs_Market_20', 'cross_sectional', 'Benchmark close / market snapshot', '20-day relative strength versus market benchmark.', True),
    'RS_vs_Sector_20': _fs('RS_vs_Sector_20', 'cross_sectional', 'Sector snapshot / sector classifier', '20-day relative strength versus sector benchmark.', True),
    'Revenue_YoY_Rank': _fs('Revenue_YoY_Rank', 'fundamentals', 'monthly revenue / full-market percentile snapshot', 'Official full-market percentile of revenue YoY.', True, True, True),
    'Chip_Total_Ratio_Rank': _fs('Chip_Total_Ratio_Rank', 'chip_flow', 'daily chip / full-market percentile snapshot', 'Official full-market percentile of total chip ratio.', True, True, True),
    'Event_Days_Since_Revenue': _fs('Event_Days_Since_Revenue', 'events', 'monthly revenue calendar', 'Days since most recent revenue-release date.', True, True, False, True),
    'Earnings_Window_Flag': _fs('Earnings_Window_Flag', 'events', 'quarterly fundamentals calendar', '1 if within earnings-event window.', True, True, False, True),
    'Regime_TrendStrength_X_ScoreGap': _fs('Regime_TrendStrength_X_ScoreGap', 'interaction', 'OHLCV + scoring', 'ADX normalized multiplied by score gap.', True),
    'Volatility_X_SignalConflict': _fs('Volatility_X_SignalConflict', 'interaction', 'OHLCV + scoring', 'ATR percentile multiplied by signal conflict.', True),
    'RS_vs_Market_20_Pctl': _fs('RS_vs_Market_20_Pctl', 'cross_sectional', 'full-market percentile snapshot', 'Official full-market percentile of RS_vs_Market_20.', True, True, True),
    'RS_vs_Sector_20_Pctl': _fs('RS_vs_Sector_20_Pctl', 'cross_sectional', 'full-market percentile snapshot', 'Official full-market percentile of RS_vs_Sector_20.', True, True, True),
    'Revenue_YoY_Pctl': _fs('Revenue_YoY_Pctl', 'fundamentals', 'full-market percentile snapshot', 'Official full-market percentile of revenue YoY.', True, True, True),
    'Chip_Total_Ratio_Pctl': _fs('Chip_Total_Ratio_Pctl', 'chip_flow', 'full-market percentile snapshot', 'Official full-market percentile of total chip ratio.', True, True, True),
    'Turnover_Pctl': _fs('Turnover_Pctl', 'tradeability', 'full-market percentile snapshot', 'Official full-market percentile of turnover proxy.', True, True, True),
    'ADV20_Pctl': _fs('ADV20_Pctl', 'tradeability', 'full-market percentile snapshot', 'Official full-market percentile of ADV20 proxy.', True, True, True),
    'ATR_Pct_Pctl': _fs('ATR_Pct_Pctl', 'volatility', 'full-market percentile snapshot', 'Official full-market percentile of ATR_Pct.', True, True, True),
    'RealizedVol_20_Pctl': _fs('RealizedVol_20_Pctl', 'volatility', 'full-market percentile snapshot', 'Official full-market percentile of RealizedVol_20.', True, True, True),
    'Event_Days_To_Revenue': _fs('Event_Days_To_Revenue', 'events', 'monthly revenue calendar', 'Days until next revenue-release date.', True, True, False, True),
    'Revenue_Window_1': _fs('Revenue_Window_1', 'events', 'monthly revenue calendar', 'Revenue event window 1 day.', True, True, False, True),
    'Revenue_Window_3': _fs('Revenue_Window_3', 'events', 'monthly revenue calendar', 'Revenue event window 3 day.', True, True, False, True),
    'Revenue_Window_5': _fs('Revenue_Window_5', 'events', 'monthly revenue calendar', 'Revenue event window 5 day.', True, True, False, True),
    'Revenue_Window_10': _fs('Revenue_Window_10', 'events', 'monthly revenue calendar', 'Revenue event window 10 day.', True, True, False, True),
    'Event_Days_Since_Earnings': _fs('Event_Days_Since_Earnings', 'events', 'quarterly fundamentals calendar', 'Days since most recent earnings date.', True, True, False, True),
    'Event_Days_To_Earnings': _fs('Event_Days_To_Earnings', 'events', 'quarterly fundamentals calendar', 'Days until next earnings date.', True, True, False, True),
    'Earnings_Window_3': _fs('Earnings_Window_3', 'events', 'quarterly fundamentals calendar', 'Earnings event window 3 day.', True, True, False, True),
    'Earnings_Window_7': _fs('Earnings_Window_7', 'events', 'quarterly fundamentals calendar', 'Earnings event window 7 day.', True, True, False, True),
    'Earnings_Window_14': _fs('Earnings_Window_14', 'events', 'quarterly fundamentals calendar', 'Earnings event window 14 day.', True, True, False, True),
    'Dividend_Window_7': _fs('Dividend_Window_7', 'events', 'dividend calendar', 'Dividend event window 7 day.', True, True, False, True),
}

FEATURE_BUCKETS: Dict[str, List[str]] = {}
for name, spec in FEATURE_SPECS.items():
    FEATURE_BUCKETS.setdefault(spec.bucket, []).append(name)

PRIORITY_NEW_FEATURES_20: List[str] = [
    'ATR14', 'ATR_Pct', 'ATR_Pctl_252', 'RealizedVol_20', 'RealizedVol_60',
    'Gap_Pct', 'Overnight_Return', 'Intraday_Return', 'Turnover_Proxy', 'ADV20_Proxy',
    'DollarVol20_Proxy', 'Volume_Z20', 'Return_Z20', 'RS_vs_Market_20', 'RS_vs_Sector_20',
    'Revenue_YoY_Rank', 'Chip_Total_Ratio_Rank', 'Event_Days_Since_Revenue',
    'Earnings_Window_Flag', 'Regime_TrendStrength_X_ScoreGap',
]


# ----- 文件8：safe directional/range feature overlay (training/research first, no direct live default) -----
FEATURE_SPECS.update({
    'Short_Failed_Rebound': _fs('Short_Failed_Rebound', 'directional_short', 'OHLCV + MA/RSI', 'Failed rebound below MA20 with weak close.', True, False, False, False, 'directional', 'SHORT_ONLY', 'research_and_training', False),
    'Short_Weak_Bounce': _fs('Short_Weak_Bounce', 'directional_short', 'OHLCV', 'Weak intraday bounce with soft close.', True, False, False, False, 'directional', 'SHORT_ONLY', 'research_and_training', False),
    'Short_Distribution_Pressure': _fs('Short_Distribution_Pressure', 'directional_short', 'OHLCV + chip', 'Distribution pressure under negative chip flow.', True, False, False, False, 'directional', 'SHORT_ONLY', 'research_and_training', False),
    'Short_Breakdown_Followthrough': _fs('Short_Breakdown_Followthrough', 'directional_short', 'OHLCV', 'Breakdown continuation under MA20.', True, False, False, False, 'directional', 'SHORT_ONLY', 'research_and_training', False),
    'Short_Upper_Shadow_Pressure': _fs('Short_Upper_Shadow_Pressure', 'directional_short', 'OHLCV', 'Upper-shadow selling pressure.', True, False, False, False, 'directional', 'SHORT_ONLY', 'research_and_training', False),
    'Short_GapDown_Continuation': _fs('Short_GapDown_Continuation', 'directional_short', 'OHLCV', 'Gap-down with weak intraday recovery.', True, False, False, False, 'directional', 'SHORT_ONLY', 'research_and_training', False),
    'Short_Below_MA20_FailedRetake': _fs('Short_Below_MA20_FailedRetake', 'directional_short', 'OHLCV + MA', 'Below-MA20 failed retake.', True, False, False, False, 'directional', 'SHORT_ONLY', 'research_and_training', False),
    'Short_RS_Weakness': _fs('Short_RS_Weakness', 'directional_short', 'cross_sectional', 'Relative-strength weakness.', True, False, True, False, 'directional', 'SHORT_ONLY', 'research_and_training', False),
    'Range_Position_Pct': _fs('Range_Position_Pct', 'directional_range', 'OHLCV rolling range', 'Position of close inside rolling range.', True, False, False, False, 'directional', 'RANGE_ONLY', 'research_and_training', False),
    'Distance_To_Range_Top': _fs('Distance_To_Range_Top', 'directional_range', 'OHLCV rolling range', 'Distance to rolling range top.', True, False, False, False, 'directional', 'RANGE_ONLY', 'research_and_training', False),
    'Distance_To_Range_Bottom': _fs('Distance_To_Range_Bottom', 'directional_range', 'OHLCV rolling range', 'Distance to rolling range bottom.', True, False, False, False, 'directional', 'RANGE_ONLY', 'research_and_training', False),
    'Range_Mean_Reversion_Score': _fs('Range_Mean_Reversion_Score', 'directional_range', 'OHLCV + RSI + Bollinger', 'Mean-reversion score inside range.', True, False, False, False, 'directional', 'RANGE_ONLY', 'research_and_training', False),
    'Range_Exhaustion_Score': _fs('Range_Exhaustion_Score', 'directional_range', 'OHLCV + RSI', 'Exhaustion score near range edge.', True, False, False, False, 'directional', 'RANGE_ONLY', 'research_and_training', False),
    'Range_Width_Pct': _fs('Range_Width_Pct', 'directional_range', 'OHLCV rolling range', 'Normalized rolling range width.', True, False, False, False, 'directional', 'RANGE_ONLY', 'research_and_training', False),
    'Range_Center_Distance': _fs('Range_Center_Distance', 'directional_range', 'OHLCV rolling range', 'Distance from rolling range center.', True, False, False, False, 'directional', 'RANGE_ONLY', 'research_and_training', False),
    'Range_Bounce_Quality': _fs('Range_Bounce_Quality', 'directional_range', 'OHLCV', 'Bounce quality near range bottom.', True, False, False, False, 'directional', 'RANGE_ONLY', 'research_and_training', False),
    'Range_Fade_Quality': _fs('Range_Fade_Quality', 'directional_range', 'OHLCV', 'Fade quality near range top.', True, False, False, False, 'directional', 'RANGE_ONLY', 'research_and_training', False),
    'Range_Confidence': _fs('Range_Confidence', 'regime_confidence', 'regime service', 'Probability-like confidence that current state is range-bound.', True, False, False, False, 'regime', 'RANGE_ONLY', 'research_and_training', False),
    'Trend_Confidence': _fs('Trend_Confidence', 'regime_confidence', 'regime service', 'Probability-like confidence that current state is trending.', True, False, False, False, 'regime', 'SHARED', 'research_and_training', False),
    'Range_Width_Pctl': _fs('Range_Width_Pctl', 'regime_confidence', 'regime service', 'Percentile of range width.', True, False, True, False, 'regime', 'RANGE_ONLY', 'research_and_training', False),
    'MA_Slope_Flatness': _fs('MA_Slope_Flatness', 'regime_confidence', 'regime service', 'Flatness of MA slope.', True, False, False, False, 'regime', 'RANGE_ONLY', 'research_and_training', False),
    'BB_Width_Pctl': _fs('BB_Width_Pctl', 'regime_confidence', 'regime service', 'Percentile of Bollinger width.', True, False, True, False, 'regime', 'RANGE_ONLY', 'research_and_training', False),
    'ADX_Low_Regime_Flag': _fs('ADX_Low_Regime_Flag', 'regime_confidence', 'regime service', 'Flag for low-ADX range-like regime.', True, False, False, False, 'regime', 'RANGE_ONLY', 'research_and_training', False),
})

FEATURE_BUCKETS = {}
for name, spec in FEATURE_SPECS.items():
    FEATURE_BUCKETS.setdefault(spec.bucket, []).append(name)

STRATEGY_SCOPE_GROUPS: Dict[str, List[str]] = {'SHARED': [], 'LONG_ONLY': [], 'SHORT_ONLY': [], 'RANGE_ONLY': []}
for name, spec in FEATURE_SPECS.items():
    STRATEGY_SCOPE_GROUPS.setdefault(spec.strategy_scope, []).append(name)

LIVE_SAFE_FEATURES: List[str] = [name for name, spec in FEATURE_SPECS.items() if spec.is_live_safe]

def get_feature_list(strategy_scope: str = 'ALL', live_safe_only: bool = False) -> List[str]:
    if strategy_scope == 'ALL':
        names = list(FEATURE_SPECS.keys())
    else:
        names = list(STRATEGY_SCOPE_GROUPS.get(strategy_scope, [])) + STRATEGY_SCOPE_GROUPS.get('SHARED', [])
        names = list(dict.fromkeys(names))
    if live_safe_only:
        names = [n for n in names if n in LIVE_SAFE_FEATURES]
    return names

def get_training_feature_groups() -> Dict[str, List[str]]:
    return {k: get_feature_list(k, live_safe_only=False) for k in ['SHARED', 'LONG_ONLY', 'SHORT_ONLY', 'RANGE_ONLY']}

def get_live_feature_groups() -> Dict[str, List[str]]:
    return {k: get_feature_list(k, live_safe_only=True) for k in ['SHARED', 'LONG_ONLY', 'SHORT_ONLY', 'RANGE_ONLY']}
