import numpy as np
import pandas as pd

# ==========================================
# 🗣️ 市場語言工廠 (Market Language Translation)
# 負責將人類的「看盤直覺」翻譯成 1/0 的機器開關
# ==========================================

def detect_trend_regime(df, p):
    """定義市場環境 (Regime)"""
    adx_strong = df['ADX14'] >= p.get('ADX_TREND_THRESHOLD', 20)
    is_bull = (df['Close'] > df['BBI']) & adx_strong
    is_bear = (df['Close'] < df['BBI']) & adx_strong
    
    # 回傳文字標籤
    return np.where(is_bull, '趨勢多頭', np.where(is_bear, '趨勢空頭', '區間盤整'))

def is_vol_breakout(df, multiplier=1.5):
    """語言：是否爆量？"""
    return (df['Volume'] > (df['Vol_MA20'] * multiplier))

def is_price_breakout(df):
    """語言：是否帶量突破 BBI 均線？"""
    return (df['Close'] > df['BBI']) & (df['Close'].shift(1) <= df['BBI'].shift(1))

def is_oversold(df):
    """語言：是否嚴重超賣 (跌破布林下軌 + RSI極低)？"""
    return (df['Low'] <= df['BB_Lower']) & (df['RSI'] < 30)

def is_smart_money_buying(df):
    """語言：聰明錢(外資+投信)是否正在買進？"""
    return (df.get('Foreign_Net', 0) > 0) & (df.get('Trust_Net', 0) > 0)

# (您可以把 screening.py 裡面的 detect_divergence 原封不動搬到這裡)
def is_bottom_divergence(price_series, indicator_series, atr_series, atr_mult=0.8):
    """語言：是否發生底背離？(需接上您原本的 detect_divergence 演算法)"""
    # 這裡為簡化展示，實際可直接呼叫您寫好的背離偵測函數
    pass