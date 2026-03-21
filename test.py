import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import matplotlib.pyplot as plt

# -------------------------------
# 手寫 RSI 函數
# -------------------------------
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# -------------------------------
# 1. 取得股價資料
# -------------------------------
df = yf.download("2330.TW", start="2023-01-01")
df['RSI'] = calculate_rsi(df['Close'], period=14)
df.dropna(inplace=True)

# -------------------------------
# 2. 轉成一維 numpy array
# -------------------------------
low_prices = df['Low'].astype(float).to_numpy().ravel()
rsi_values = df['RSI'].astype(float).to_numpy().ravel()

# -------------------------------
# 3. 找低點
# -------------------------------
price_lows, _ = find_peaks(-low_prices, distance=5)
rsi_lows, _ = find_peaks(-rsi_values, distance=5)

# -------------------------------
# 4. 背離判斷
# -------------------------------
divergence_dates = []
for i in range(1, len(price_lows)):
    p1, p2 = price_lows[i-1], price_lows[i]
    if low_prices[p2] < low_prices[p1] and rsi_values[p2] > rsi_values[p1]:
        divergence_dates.append(df.index[p2])
        print(f"检测到底背離！日期：{df.index[p2].date()}")

# -------------------------------
# 5. 畫圖
# -------------------------------
plt.figure(figsize=(14,8))

# 股價
plt.subplot(2,1,1)
plt.plot(df.index, df['Close'], label='Close Price', color='blue')
plt.scatter(df.index[price_lows], df['Low'].iloc[price_lows], color='red', label='Price Lows')
for d in divergence_dates:
    plt.scatter(d, df['Low'].loc[d], color='orange', s=100, marker='*', label='Divergence')
plt.title("2330.TW 股價與低點 / 底背離")
plt.legend()

# RSI
plt.subplot(2,1,2)
plt.plot(df.index, df['RSI'], label='RSI', color='green')
plt.scatter(df.index[rsi_lows], df['RSI'].iloc[rsi_lows], color='purple', label='RSI Lows')
plt.axhline(30, color='gray', linestyle='--', label='Oversold 30')
plt.title("RSI 指標")
plt.legend()

plt.tight_layout()
plt.show()