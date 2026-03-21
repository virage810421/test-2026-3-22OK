import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# -------------------------------
# 1. 數據獲取與指標計算 (保持不變)
# -------------------------------
ticker = "2330.TW"
data = yf.download(ticker, start="2024-01-01") 
df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()

delta = df['Close'].diff()
gain = delta.where(delta > 0, 0)
loss = -delta.where(delta < 0, 0)
avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
df['RSI'] = 100 - (100 / (1 + avg_gain / avg_loss))

df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()
df['MACD_Line'] = df['EMA12'] - df['EMA26']
df['Signal_Line'] = df['MACD_Line'].ewm(span=9, adjust=False).mean()
df['MACD_Hist'] = df['MACD_Line'] - df['Signal_Line']
df.dropna(inplace=True)

# -------------------------------
# 2. 背離偵測 (保持不變)
# -------------------------------
def find_divergence(price_series, indicator_series, is_top=True):
    peaks, _ = find_peaks(price_series if is_top else -price_series, distance=10, prominence=0.01 * np.mean(price_series))
    div_signals = []
    for i in range(1, len(peaks)):
        p1, p2 = peaks[i-1], peaks[i]
        if is_top:
            if price_series[p2] > price_series[p1] and indicator_series[p2] < indicator_series[p1]:
                div_signals.append((p1, p2))
        else:
            if price_series[p2] < price_series[p1] and indicator_series[p2] > indicator_series[p1]:
                div_signals.append((p1, p2))
    return div_signals

rsi_top_div = find_divergence(df['High'].values, df['RSI'].values, True)
macd_top_div = find_divergence(df['High'].values, df['MACD_Line'].values, True)
macd_bot_div = find_divergence(df['Low'].values, df['MACD_Line'].values, False)

# -------------------------------
# 3. 繪製圖表 (!!! 調整比例處 !!!)
# -------------------------------
# 修改重點：row_heights 由 [0.93, 0.035, 0.035] 改為 [0.86, 0.07, 0.07]
fig = make_subplots(
    rows=3, cols=1, 
    shared_xaxes=True, 
    vertical_spacing=0.03, # 增加間距讓畫面不擁擠
    row_heights=[0.70, 0.10, 0.10] 
)

# [其餘繪圖邏輯保持不變...]
fig.add_trace(go.Candlestick(x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'], name='股價'), row=1, col=1)
fig.add_trace(go.Scatter(x=df.index, y=df['RSI'], line=dict(color='#9c27b0', width=1.5), name='RSI'), row=2, col=1)

colors = ['#ef5350' if x > 0 else '#26a69a' for x in df['MACD_Hist']]
fig.add_trace(go.Bar(x=df.index, y=df['MACD_Hist'], marker_color=colors, name='MACD 柱狀'), row=3, col=1)
fig.add_trace(go.Scatter(x=df.index, y=df['MACD_Line'], line=dict(color='#29b6f6', width=1.2), name='MACD 快線'), row=3, col=1)

def plot_div(div_list, p_data, i_data, color, name, i_row):
    for p1, p2 in div_list:
        fig.add_trace(go.Scatter(x=[df.index[p1], df.index[p2]], y=[p_data[p1], p_data[p2]], 
                                 line=dict(color=color, width=3, dash='dash'), mode='lines+markers', showlegend=False), row=1, col=1)
        fig.add_trace(go.Scatter(x=[df.index[p1], df.index[p2]], y=[i_data[p1], i_data[p2]], 
                                 line=dict(color=color, width=3), mode='lines+markers', name=name), row=i_row, col=1)

plot_div(rsi_top_div, df['High'].values, df['RSI'].values, '#ff1744', 'RSI 頂背離', 2)
plot_div(macd_top_div, df['High'].values, df['MACD_Line'].values, '#ffea00', 'MACD 頂背離', 3)
plot_div(macd_bot_div, df['Low'].values, df['MACD_Line'].values, '#00e676', 'MACD 底背離', 3)

# -------------------------------
# 4. 製作動態切換按鈕
# -------------------------------
view_options = [60, 80, 100, 120, 140]
buttons = []

for count in view_options:
    start_date = df.index[-count] if len(df) >= count else df.index[0]
    end_date = df.index[-1]
    
    buttons.append(dict(
        label=f"顯示 {count} 筆",
        method="relayout",
        args=[{"xaxis.range": [start_date, end_date]}]
    ))

# -------------------------------
# 5. 佈局設定與鎖定右側
# -------------------------------
initial_start = df.index[-60] if len(df) >= 60 else df.index[0]
initial_end = df.index[-1]

fig.update_layout(
    height=1000,
    template='plotly_dark',
    title_text=f"{ticker} 比例調整儀表板 (指標放大版)",
    hovermode='x unified',
    dragmode='pan',
    margin=dict(t=120, b=50, l=50, r=50),
    xaxis_rangeslider_visible=False,
    updatemenus=[dict(
        buttons=buttons,
        direction="down",
        showactive=True,
        x=0.01, xanchor="left", y=1.1, yanchor="top",
        bgcolor="rgba(70, 70, 70, 0.8)", font=dict(color="white")
    )]
)

fig.update_xaxes(
    rangebreaks=[dict(bounds=["sat", "mon"])],
    range=[initial_start, initial_end],
    autorange=False,
    constrain="domain",
    type="date"
)

fig.show()