import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# -------------------------------
# 1. 數據獲取與指標計算 (新增 BBands)
# -------------------------------
ticker = "2330.TW"
# 抓取較長時間範圍，以便於縮放選擇
data = yf.download(ticker, start="2023-01-01") 
df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()

# A. RSI 計算
delta = df['Close'].diff()
gain = delta.where(delta > 0, 0)
loss = -delta.where(delta < 0, 0)
avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
df['RSI'] = 100 - (100 / (1 + avg_gain / avg_loss))

# B. MACD 計算
df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()
df['MACD_Hist'] = (df['EMA12'] - df['EMA26']) - (df['EMA12'] - df['EMA26']).ewm(span=9, adjust=False).mean()

# C. 【新增】布林通道計算 (20日均線, 2倍標準差)
df['MA20'] = df['Close'].rolling(window=20).mean()
df['BB_std'] = df['Close'].rolling(window=20).std()
df['BB_Upper'] = df['MA20'] + (df['BB_std'] * 2)
df['BB_Lower'] = df['MA20'] - (df['BB_std'] * 2)

df.dropna(inplace=True)

# -------------------------------
# 2. 強化版背離偵測函數 (保持個別參數邏輯)
# -------------------------------
def find_divergence(price_series, indicator_series, is_top=True, 
                     distance=7, prominence_pc=0.01, threshold=None):
    # 尋找價格波峰/波谷
    prominence_val = prominence_pc * np.mean(price_series)
    if is_top:
        peaks, _ = find_peaks(price_series, distance=distance, prominence=prominence_val)
    else:
        peaks, _ = find_peaks(-price_series, distance=distance, prominence=prominence_val)
    
    div_signals = []
    for i in range(1, len(peaks)):
        p1, p2 = peaks[i-1], peaks[i]
        
        if is_top:
            # 頂背離：價格創新高 (p2>p1) 但指標降低 (p2<p1)
            cond_price = price_series[p2] > price_series[p1]
            cond_indicator = indicator_series[p2] < indicator_series[p1]
            # 門檻檢查 (例如指標必須在高檔)
            cond_thresh = True if threshold is None else indicator_series[p2] > threshold
            
            if cond_price and cond_indicator and cond_thresh:
                div_signals.append((p1, p2))
        else:
            # 底背離：價格創新低 (p2<p1) 但指標提高 (p2>p1)
            cond_price = price_series[p2] < price_series[p1]
            cond_indicator = indicator_series[p2] > indicator_series[p1]
            # 門檻檢查 (例如指標必須在低檔)
            cond_thresh = True if threshold is None else indicator_series[p2] < threshold
            
            if cond_price and cond_indicator and cond_thresh:
                div_signals.append((p1, p2))
                
    return div_signals

# --- 設定個別參數 (微調 MACD 距離以減少重複標籤) ---
rsi_top = find_divergence(df['High'].values, df['RSI'].values, is_top=True, 
                          distance=7, prominence_pc=0.01, threshold=55)
rsi_bot = find_divergence(df['Low'].values, df['RSI'].values, is_top=False, 
                          distance=7, prominence_pc=0.01, threshold=45)
# 將 MACD 距離從 7 調高到 10，讓畫面更乾淨
macd_top = find_divergence(df['High'].values, df['MACD_Hist'].values, is_top=True, 
                           distance=7, prominence_pc=0.005, threshold=0)
macd_bot = find_divergence(df['Low'].values, df['MACD_Hist'].values, is_top=False, 
                           distance=7, prominence_pc=0.005, threshold=0)

# -------------------------------
# 3. 繪製圖表 (UI 升級版: 4 子圖)
# -------------------------------
# 調整 row_heights 以容納成交量
fig = make_subplots(
    rows=4, cols=1, 
    shared_xaxes=True, 
    vertical_spacing=0.025, # 縮小間距
    row_heights=[0.45, 0.15, 0.2, 0.2], # 主圖最大，成交量最小
    subplot_titles=("價格走勢、布林通道與背離訊號", "成交量 (Volume)", "RSI 強弱指標", "MACD 柱狀圖")
)

# A. 主圖：K線 + 布林通道
# K線
fig.add_trace(go.Candlestick(
    x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'], 
    name='股價', opacity=0.8, showlegend=False
), row=1, col=1)

# 【新增】布林通道線條
fig.add_trace(go.Scatter(x=df.index, y=df['BB_Upper'], line=dict(color='#555', width=1, dash='dot'), name='BB 上軌', showlegend=False), row=1, col=1)
fig.add_trace(go.Scatter(x=df.index, y=df['BB_Lower'], line=dict(color='#555', width=1, dash='dot'), name='BB 下軌', showlegend=False), row=1, col=1)
# 填充布林通道區域
fig.add_trace(go.Scatter(
    x=df.index.tolist() + df.index.tolist()[::-1],
    y=df['BB_Upper'].tolist() + df['BB_Lower'].tolist()[::-1],
    fill='toself', fillcolor='rgba(100, 100, 100, 0.1)',
    line=dict(color='rgba(0,0,0,0)'), name='BB 區域', showlegend=False
), row=1, col=1)

# B. 【新增】成交量子圖
vol_colors = ['#FF5252' if df['Close'].iloc[i] < df['Open'].iloc[i] else '#00E676' for i in range(len(df))]
fig.add_trace(go.Bar(
    x=df.index, y=df['Volume'], 
    marker_color=vol_colors, name='成交量', opacity=0.6
), row=2, col=1)

# C. RSI 子圖
fig.add_trace(go.Scatter(x=df.index, y=df['RSI'], line=dict(color='#BA68C8', width=2), name='RSI'), row=3, col=1)
fig.add_hrect(y0=70, y1=100, fillcolor="red", opacity=0.08, line_width=0, row=3, col=1)
fig.add_hrect(y0=0, y1=30, fillcolor="green", opacity=0.08, line_width=0, row=3, col=1)
fig.add_shape(type="line", x0=df.index[0], y0=50, x1=df.index[-1], y1=50, line=dict(color="#555", width=1, dash="dash"), row=3, col=1)

# D. MACD 子圖
macd_colors = ['#FF5252' if x > 0 else '#00E676' for x in df['MACD_Hist']]
fig.add_trace(go.Bar(x=df.index, y=df['MACD_Hist'], marker_color=macd_colors, name='MACD Hist'), row=4, col=1)

# E. 強化背離繪圖函數 (加入自動標註)
def plot_div_pro(div_list, p_data, i_data, color, name, i_row):
    for p1, p2 in div_list:
        # 在主圖畫虛線與大點點
        fig.add_trace(go.Scatter(
            x=[df.index[p1], df.index[p2]], y=[p_data[p1], p_data[p2]], 
            line=dict(color=color, width=3, dash='dot'),
            marker=dict(size=10, symbol='circle-open', line=dict(width=2)),
            mode='lines+markers', showlegend=False, hoverinfo='skip'
        ), row=1, col=1)
        
        # 在指標圖畫實線
        fig.add_trace(go.Scatter(
            x=[df.index[p1], df.index[p2]], y=[i_data[p1], i_data[p2]], 
            line=dict(color=color, width=3),
            mode='lines+markers', name=name
        ), row=i_row, col=1)
        
        # 加入文字標籤與箭頭 (稍微拉高位置以避免擋到BBands)
        fig.add_annotation(
            x=df.index[p2], y=p_data[p2],
            text=name, showarrow=True, arrowhead=2,
            arrowcolor=color, bgcolor=color, font=dict(color="white"),
            ax=0, ay=-50, row=1, col=1
        )

# 執行繪圖
plot_div_pro(rsi_top, df['High'].values, df['RSI'].values, '#FF5252', 'RSI 頂背', 3)
plot_div_pro(rsi_bot, df['Low'].values, df['RSI'].values, '#00E676', 'RSI 底背', 3)
plot_div_pro(macd_top, df['High'].values, df['MACD_Hist'].values, '#FFB74D', 'MACD 頂背', 4)
plot_div_pro(macd_bot, df['Low'].values, df['MACD_Hist'].values, '#4FC3F7', 'MACD 底背', 4)

# -------------------------------
# 4. 佈局細節調整 (完美復刻專業感)
# -------------------------------
fig.update_xaxes(
    rangebreaks=[dict(bounds=["sat", "mon"])],
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1M", step="month", stepmode="backward"),
            dict(count=3, label="3M", step="month", stepmode="backward"),
            dict(count=6, label="6M", step="month", stepmode="backward"),
            dict(count=1, label="YTD", step="year", stepmode="todate"),
            dict(step="all", label="All")
        ]),
        bgcolor="#222", activecolor="gold", font=dict(color="white"),
        x=0, y=1.12
    ),
    rangeslider=dict(visible=True, thickness=0.04), # 下方的範圍滑桿
    gridcolor='#333',
    tickfont=dict(color='#999')
)

fig.update_yaxes(gridcolor='#333', fixedrange=False, tickfont=dict(color='#999'))
# 成交量 y 軸隱藏
fig.update_yaxes(showticklabels=False, row=2, col=1)

fig.update_layout(
    height=1100, # 增加高度以容納 4 個子圖
    template='plotly_dark',
    paper_bgcolor='#111', # 深黑色背景
    plot_bgcolor='#111',
    title=dict(
        text=f"<b>{ticker} 進階互動式分析儀表板</b><br><span style='font-size:12px;color:#999'>含布林通道、成交量與多重背離偵測</span>",
        x=0.5, font=dict(size=24, color='gold')
    ),
    hovermode='x unified',
    margin=dict(t=150, b=50, l=50, r=50),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
)

fig.show()