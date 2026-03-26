import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "browser"
from plotly.subplots import make_subplots
from config import PARAMS

# ==========================================
# ⚙️ 核心封裝：精密儀表板模組 (純視覺展示 + 9分制訊號同步)
# ==========================================
# 👇 1. 這裡新增了 win_rate 和 total_profit 兩個接收參數
def draw_chart(ticker, preloaded_df=None, win_rate="N/A", total_profit="N/A", p=PARAMS):
    print(f"\n[系統提示] 啟動 {ticker} 的精密繪圖引擎...")

    # 1. 領取大腦傳過來的「講義」
    if preloaded_df is not None:
        df = preloaded_df.copy() 
    else:
        # 如果是手腳沒傳資料，才自己下載 (通常不會發生)
        return
    # -------------------------------
    # 1. 啟動外部新聞雷達
    # -------------------------------
    try:
        ticker_obj = yf.Ticker(ticker)
        news_data = ticker_obj.news
        news_text = "<b>📡 最新外部情報：</b><br>"
        
        if news_data:
            for n in news_data[:3]: 
                title = n.get('title', '無標題')
                if len(title) > 35: title = title[:35] + "..."
                publisher = n.get('publisher', '未知')
                news_text += f"• {title} <i>({publisher})</i><br>"
        else:
            news_text += "目前無最新情報"
    except Exception:
        news_text = "<b>📡 外部情報雷達連線失敗</b>"

    # -------------------------------
    # 2. 數據獲取與全套指標計算 (全面改用 PARAMS 參數字典)
    # -------------------------------
    if preloaded_df is not None:
        df = preloaded_df.copy()
        if df.empty: return
    else:
        data = yf.download(ticker, period="2y", progress=False) 
        if data.empty: return
        df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()

# 🛡️ 終極防撞牆：一次檢查「有沒有料」、「夠不夠長」、「有沒有分數」
    if df.empty or len(df) < 10 or 'Buy_Score' not in df.columns:
        print(f"⚠️ {ticker} 繪圖引擎警告：資料不完整 (K線數: {len(df)})，已安全跳過。")
        

    # RSI 與動態區間 (全面換成 p['RSI_PERIOD'])
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(alpha=1/p['RSI_PERIOD'], min_periods=p['RSI_PERIOD'], adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/p['RSI_PERIOD'], min_periods=p['RSI_PERIOD'], adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))
    df['DZ_Upper'] = df['RSI'].rolling(p['RSI_PERIOD']).mean() + (df['RSI'].rolling(p['RSI_PERIOD']).std() * 1.5)
    df['DZ_Lower'] = df['RSI'].rolling(p['RSI_PERIOD']).mean() - (df['RSI'].rolling(p['RSI_PERIOD']).std() * 1.5)

    # MACD (全面換成 p['MACD_FAST'], p['MACD_SLOW'], p['MACD_SIGNAL'])
    df['EMA12'] = df['Close'].ewm(span=p['MACD_FAST'], adjust=False).mean()
    df['EMA26'] = df['Close'].ewm(span=p['MACD_SLOW'], adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['MACD_Signal'] = df['DIF'].ewm(span=p['MACD_SIGNAL'], adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['MACD_Signal']) * 2

    # BBI (利用迴圈自動讀取陣列參數)
    bbi_cols = []
    for days in p['BBI_PERIODS']:
        col_name = f'MA{days}'
        df[col_name] = df['Close'].rolling(window=days).mean()
        bbi_cols.append(df[col_name])
    df['BBI'] = sum(bbi_cols) / len(p['BBI_PERIODS'])
    
    # ATR 計算 (供畫背離線使用)
    df['TR'] = np.maximum.reduce([df['High'] - df['Low'], (df['High'] - df['Close'].shift(1)).abs(), (df['Low'] - df['Close'].shift(1)).abs()])
    df['ATR'] = df['TR'].ewm(alpha=1/14, adjust=False).mean()
    

    # 🛡️ 保留防撞氣囊 (確保 Screening 沒給你壞講義)
    if df.empty or len(df) < 10:
        print(f"⚠️ {ticker} 資料量不足以顯示圖表。")
        return

    # 🛡️ 防撞護欄：改檢查是否有 Buy_Score 欄位
    if 'Buy_Score' not in df.columns or df.empty:
        print(f"⚠️ {ticker} 繪圖引擎警告：資料表不完整或為空，跳過繪圖。")
        return
    # -------------------------------
    # 3. 雙向 9 分制邏輯閘 (產生圖表買賣三角形)
    # -------------------------------
    # 這裡我們用最簡化的方式算出 Buy_Score 和 Sell_Score 讓圖表能標示訊號
    buy_c1 = df['Low'] <= df['BB_Lower']
    buy_c2 = df['RSI'] < df['DZ_Lower']
    buy_c3 = (df['Volume'] > (df['Vol_MA20'] * 1.1)) & (df['Close'] > df['Open'])
    buy_c4 = (df['MACD_Hist'] > df['MACD_Hist'].shift(1)) & (df['DIF'] < 0)
    buy_c6 = (df['Close'] > df['BBI']) & (df['Close'].shift(1) <= df['BBI'].shift(1))
    
    sell_c1 = df['High'] >= df['BB_Upper']
    sell_c2 = df['RSI'] > df['DZ_Upper']
    sell_c3 = (df['Volume'] > (df['Vol_MA20'] * 1.1)) & (df['Close'] < df['Open'])
    sell_c4 = (df['MACD_Hist'] < df['MACD_Hist'].shift(1)) & (df['DIF'] > 0)
    sell_c6 = (df['Close'] < df['BBI']) & (df['Close'].shift(1) >= df['BBI'].shift(1))

    # 畫圖時只要達到 3 分 (弱訊號) 或 4 分 (強訊號) 就顯示標記
    df['Buy_Score'] = (buy_c1 | buy_c2).astype(int) + buy_c3.astype(int) + buy_c4.astype(int) + buy_c6.astype(int)
    df['Sell_Score'] = (sell_c1 | sell_c2).astype(int) + sell_c3.astype(int) + sell_c4.astype(int) + sell_c6.astype(int)
    
    df['Buy_Signal'] = np.where(df['Buy_Score'] >= 3, df['Low'] * 0.98, np.nan)
    df['Sell_Signal'] = np.where(df['Sell_Score'] >= 3, df['High'] * 1.02, np.nan)

    # -------------------------------
    # 4. 繪製圖表 (UI 升級版: 4 子圖)
    # -------------------------------
    color_up, color_down = '#FF5252', '#00E676' # 台股配色
    vol_colors = [color_down if df['Close'].iloc[i] < df['Open'].iloc[i] else color_up for i in range(len(df))]

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.025, 
        row_heights=[0.45, 0.15, 0.2, 0.2], 
        subplot_titles=("價格走勢、布林通道與季線", "成交量 (Volume)", "RSI 強弱指標", "MACD 柱狀圖")
    )

    # K線與指標
    fig.add_trace(go.Candlestick(x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'], increasing_line_color=color_up, decreasing_line_color=color_down, name='股價', opacity=0.8, showlegend=False), row=1, col=1)
    df['MA_LONG'] = df['Close'].rolling(window=p['MA_LONG']).mean()
    fig.add_trace(go.Scatter(x=df.index, y=df['MA_LONG'], line=dict(color='#2196F3', width=2), name=f"長天期均線 (MA{p['MA_LONG']})"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['BB_Upper'], line=dict(color='#555', width=1, dash='dot'), name='BB 上軌', showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['BB_Lower'], line=dict(color='#555', width=1, dash='dot'), name='BB 下軌', showlegend=False), row=1, col=1)
    

    # 買賣三角形標記
    fig.add_trace(go.Scatter(x=df.index, y=df['Buy_Signal'], mode='markers', marker=dict(symbol='triangle-up', size=12, color=color_up, line=dict(width=1, color='white')), name='買入訊號', hoverinfo='x+y'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['Sell_Signal'], mode='markers', marker=dict(symbol='triangle-down', size=12, color=color_down, line=dict(width=1, color='white')), name='賣出訊號', hoverinfo='x+y'), row=1, col=1)

    # 副圖
    fig.add_trace(go.Bar(x=df.index, y=df['Volume'], marker_color=vol_colors, name='成交量', opacity=0.6), row=2, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['RSI'], line=dict(color='#BA68C8', width=2), name='RSI'), row=3, col=1)
    fig.add_hrect(y0=70, y1=100, fillcolor="red", opacity=0.08, line_width=0, row=3, col=1)
    fig.add_hrect(y0=0, y1=30, fillcolor="green", opacity=0.08, line_width=0, row=3, col=1)
    
    macd_colors = [color_up if x > 0 else color_down for x in df['MACD_Hist']]
    fig.add_trace(go.Bar(x=df.index, y=df['MACD_Hist'], marker_color=macd_colors, name='MACD 柱狀圖'), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['DIF'], line=dict(color='gold', width=1.5), name='DIF (快線)'), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['MACD_Signal'], line=dict(color='#00BFFF', width=1.5), name='Signal (慢線)'), row=4, col=1)

    # -------------------------------
    # 5. scipy 科學背離畫線邏輯
    # -------------------------------
    def plot_div_pro(is_top, price_col, ind_col, color, name, row_num, distance=7, atr_mult=1.0, threshold=None):
        dynamic_prominence = df['ATR'].values * atr_mult
        p_data, i_data = df[price_col].values, df[ind_col].values
        
        peaks, _ = find_peaks(p_data if is_top else -p_data, distance=distance, prominence=dynamic_prominence)
        
        div_list = []
        for i in range(1, len(peaks)):
            p1, p2 = peaks[i-1], peaks[i]
            cond_price = (p_data[p2] > p_data[p1]) if is_top else (p_data[p2] < p_data[p1])
            cond_ind = (i_data[p2] < i_data[p1]) if is_top else (i_data[p2] > i_data[p1])
            cond_th = True if threshold is None else (i_data[p2] > threshold if is_top else i_data[p2] < threshold)
            
            if cond_price and cond_ind and cond_th: div_list.append((p1, p2))

        ay_offset = -50 if is_top else 50
        for p1, p2 in div_list:
            fig.add_trace(go.Scatter(x=[df.index[p1], df.index[p2]], y=[p_data[p1], p_data[p2]], line=dict(color=color, width=2, dash='dot'), mode='lines', showlegend=False), row=1, col=1)
            fig.add_trace(go.Scatter(x=[df.index[p1], df.index[p2]], y=[i_data[p1], i_data[p2]], line=dict(color=color, width=2), mode='lines', showlegend=False), row=row_num, col=1)
            fig.add_annotation(x=df.index[p2], y=p_data[p2], text=name, showarrow=True, arrowhead=2, arrowcolor=color, bgcolor=color, font=dict(color="#111", size=10), ax=0, ay=ay_offset, row=1, col=1)

    plot_div_pro(True, 'High', 'RSI', '#FF5252', 'RSI 頂背', 3, threshold=55)
    plot_div_pro(False, 'Low', 'RSI', '#00E676', 'RSI 底背', 3, threshold=45)
    plot_div_pro(True, 'High', 'DIF', '#FFB74D', 'MACD 頂背', 4)
    plot_div_pro(False, 'Low', 'DIF', '#4FC3F7', 'MACD 底背', 4)

    # -------------------------------
    # 6. 佈局調整與顯示
    # -------------------------------
    fig.update_xaxes(
        rangebreaks=[dict(bounds=["sat", "mon"])], 
        rangeslider=dict(visible=False), 
        gridcolor='#222',
        fixedrange=False,  # 🔓 解鎖 X 軸滑鼠縮放限制
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1個月", step="month", stepmode="backward"),
                dict(count=3, label="3個月", step="month", stepmode="backward"),
                dict(count=6, label="半年", step="month", stepmode="backward"),
                dict(count=1, label="今年以來", step="year", stepmode="todate"),
                dict(step="all", label="全部")
            ]),
            bgcolor="rgba(30, 30, 30, 0.8)", activecolor="#FFB74D", font=dict(color="white"), x=0, y=1.08 
        )
    )
    
    # 🔓 解鎖 Y 軸滑鼠縮放限制，並隱藏第二個子圖 (成交量) 的 Y 軸刻度以保持乾淨
    fig.update_yaxes(gridcolor='#222', fixedrange=False)
    fig.update_yaxes(showticklabels=False, row=2, col=1)
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])], rangeslider=dict(visible=False), gridcolor='#222')
    fig.update_yaxes(gridcolor='#222')

    fig.update_layout(
        height=900, template='plotly_dark', paper_bgcolor='#0a0a0a', plot_bgcolor='#0a0a0a',
        title=dict(text=f"<b>{ticker} 結構化分析儀表板</b>", x=0.5, font=dict(size=22, color='gold'), y=0.98),
        hovermode='x unified', margin=dict(t=100, b=30, l=50, r=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    # 貼上新聞面板
    fig.add_annotation(text=news_text, align='left', showarrow=False, xref='paper', yref='paper', x=0.01, y=0.98, bgcolor='rgba(30, 30, 30, 0.7)', bordercolor='gold', borderwidth=1, borderpad=8, font=dict(size=11, color='#E0E0E0'))
    
    # 【新增】顯示當前訊號狀態面板，取代舊版的回測文字
    try:
        signal_text = f"<b>💡 訊號觀測站</b><br>多方得分: {int(df['Buy_Score'].iloc[-1])}/4<br>空方得分: {int(df['Sell_Score'].iloc[-1])}/4"
    except (IndexError, KeyError, ValueError):
        signal_text = "<b>💡 訊號觀測站</b><br>得分: 計算中..."
    
    if win_rate is not None and total_profit is not None:
        color_prof = '#FF5252' if float(total_profit) > 0 else '#00E676' # 台股賺錢是紅色
        signal_text += f"<br>──────────<br>系統勝率: {float(win_rate):.2f}%<br>累計報酬: <span style='color:{color_prof}'>{float(total_profit):.2f}%</span>"

    fig.add_annotation(text=signal_text, align='left', showarrow=False, xref='paper', yref='paper', x=0.99, y=0.6, bgcolor='rgba(10, 40, 20, 0.8)', bordercolor='#00BFFF', borderwidth=1.5, borderpad=10, font=dict(size=13, color='#F5F5F5'))

    print(f"✅ {ticker} 實戰儀表板(靜態圖片)生成中，請稍候彈出視窗...")
    fig.show() # ✅ 彈出圖表
# ==========================================
# 🚀 手動單機測試開關
# ==========================================
if __name__ == "__main__":

    test_targets =  [
        "2330.TW"
    ]
    print("啟動手動測試模式，開始批次分析...\n")
    for ticker in test_targets:
        draw_chart(ticker)    