import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# -------------------------------
# 1. 數據獲取與指標計算 (保持邏輯)
# -------------------------------
ticker = "2330.TW"
# 抓取較長時間範圍，以便於縮放選擇
data = yf.download(ticker, start="2023-01-01") 
df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()

# RSI 計算
delta = df['Close'].diff()
gain = delta.where(delta > 0, 0)
loss = -delta.where(delta < 0, 0)
avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
df['RSI'] = 100 - (100 / (1 + avg_gain / avg_loss))

# MACD 計算
df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()
df['MACD_Hist'] = (df['EMA12'] - df['EMA26']) - (df['EMA12'] - df['EMA26']).ewm(span=9, adjust=False).mean()
df.dropna(inplace=True)

# -------------------------------
# 2. 背離偵測函數
# -------------------------------
def find_divergence(price_series, indicator_series, is_top=True):
    if is_top:
        peaks, _ = find_peaks(price_series, distance=7, prominence=0.01 * np.mean(price_series))
    else:
        peaks, _ = find_peaks(-price_series, distance=7, prominence=0.01 * np.mean(price_series))
    
    div_signals = []
    for i in range(1, len(peaks)):
        p1, p2 = peaks[i-1], peaks[i]
        if is_top:
            if price_series[p2] > price_series[p1] and indicator_series[p2] < indicator_series[p1] and indicator_series[p2] > 55:
                div_signals.append((p1, p2))
        else:
            if price_series[p2] < price_series[p1] and indicator_series[p2] > indicator_series[p1] and indicator_series[p2] < 45:
                div_signals.append((p1, p2))
    return div_signals

rsi_top = find_divergence(df['High'].values, df['RSI'].values, True)
rsi_bot = find_divergence(df['Low'].values, df['RSI'].values, False)
macd_top = find_divergence(df['High'].values, df['MACD_Hist'].values, True)
macd_bot = find_divergence(df['Low'].values, df['MACD_Hist'].values, False)

# -------------------------------
# 3. 繪製圖表與加入「時間選擇器」
# -------------------------------
fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.02, 
                    row_heights=[0.6, 0.2, 0.2])

# 主圖：K線
fig.add_trace(go.Candlestick(x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'], name='股價'), row=1, col=1)

# RSI 子圖
fig.add_trace(go.Scatter(x=df.index, y=df['RSI'], line=dict(color='#9c27b0', width=2), name='RSI'), row=2, col=1)
fig.add_hrect(y0=70, y1=100, fillcolor="red", opacity=0.1, line_width=0, row=2, col=1)
fig.add_hrect(y0=0, y1=30, fillcolor="green", opacity=0.1, line_width=0, row=2, col=1)

# MACD 子圖
colors = ['#ef5350' if x > 0 else '#26a69a' for x in df['MACD_Hist']]
fig.add_trace(go.Bar(x=df.index, y=df['MACD_Hist'], marker_color=colors, name='MACD 柱狀圖'), row=3, col=1)

# 繪製背離線 (簡化調用)
def plot_div(div_list, p_data, i_data, color, name, i_row):
    for p1, p2 in div_list:
        fig.add_trace(go.Scatter(x=[df.index[p1], df.index[p2]], y=[p_data[p1], p_data[p2]], line=dict(color=color, width=3), mode='lines+markers', showlegend=False), row=1, col=1)
        fig.add_trace(go.Scatter(x=[df.index[p1], df.index[p2]], y=[i_data[p1], i_data[p2]], line=dict(color=color, width=3), mode='lines+markers', name=name), row=i_row, col=1)

plot_div(rsi_top, df['High'].values, df['RSI'].values, '#ff5252', 'RSI 頂背離', 2)
plot_div(rsi_bot, df['Low'].values, df['RSI'].values, '#00e676', 'RSI 底背離', 2)
plot_div(macd_top, df['High'].values, df['MACD_Hist'].values, '#ffa726', 'MACD 頂背離', 3)
plot_div(macd_bot, df['Low'].values, df['MACD_Hist'].values, '#29b6f6', 'MACD 底背離', 3)

# -------------------------------
# 4. 重點：加入月份/日期選擇按鈕 (Range Selector)
# -------------------------------
fig.update_xaxes(
    rangebreaks=[dict(bounds=["sat", "mon"])], # 移除假日
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1月", step="month", stepmode="backward"),
            dict(count=3, label="3月", step="month", stepmode="backward"),
            dict(count=6, label="6月", step="month", stepmode="backward"),
            dict(count=1, label="今年", step="year", stepmode="todate"),
            dict(step="all", label="全部")
        ]),
        bgcolor="rgba(150, 150, 150, 0.1)", # 按鈕背景色
        activecolor="gold",                # 選中時的顏色
        x=0, y=1.05                        # 按鈕放置位置 (主圖上方)
    ),
    rangeslider=dict(visible=True),         # 下方滑桿
    type="date"
)

# 佈局微調
fig.update_layout(
    height=1000,
    template='plotly_dark',
    title_text=f"{ticker} 互動式背離分析儀表板",
    xaxis_rangeslider_visible=False, # 隱藏子圖內建滑桿以避免衝突
    hovermode='x unified',
    margin=dict(t=120) # 留空間給上方的日期按鈕
)

fig.show()