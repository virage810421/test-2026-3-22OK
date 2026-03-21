import yfinance as yf
import numpy as np
from scipy.signal import find_peaks

def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# 1. 獲取數據
df = yf.download("2330.TW", start="2025-01-01") # 以台積電為例

# 2. 計算 RSI
df['RSI'] = calculate_rsi(df['Close'], length=14)
df.dropna(inplace=True)

# 3. 尋找股價與 RSI 的局部低點 (Valleys)
# distance 代表兩個低點之間至少隔幾天，依策略調整
price_lows, _ = find_peaks(-df['Low'], distance=5) 
rsi_lows, _ = find_peaks(-df['RSI'], distance=5)

# 4. 簡易背離判斷 (以最後兩個低點為例)
if len(price_lows) >= 2:
    p1, p2 = price_lows[-2], price_lows[-1] # p2 是最近的低點
    
    # 底背離邏輯：
    # 股價：p2 的低點 < p1 的低點 (創新低)
    # RSI： p2 的 RSI > p1 的 RSI (沒創新低，反而轉強)
    if df['Low'].iloc[p2] < df['Low'].iloc[p1] and df['RSI'].iloc[p2] > df['RSI'].iloc[p1]:
        print(f"检测到底背離！日期：{df.index[p2].date()}")