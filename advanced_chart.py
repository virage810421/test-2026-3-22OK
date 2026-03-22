import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ==========================================
# ⚙️ 核心封裝：精密儀表板模組
# ==========================================
def draw_chart(ticker):
    # 【新增這顆指示燈】
    print(f"\n[系統提示] 收到海選雷達傳來的訊號！正在啟動 {ticker} 的精密繪圖引擎...")
    """
    這是一個可以被外部呼叫的繪圖模組。
    傳入股票代號 (ticker)，就會自動生成該股票的 4 層精密儀表板。
    """
    # -------------------------------
    # 1. 數據獲取與指標計算 (新增 BBands)
    # -------------------------------
    
    # 【新增】啟動外部新聞雷達 (攔截最新 3 則情報)
    try:
        ticker_obj = yf.Ticker(ticker)
        news_data = ticker_obj.news
        news_text = "<b>📡 最新外部情報雷達：</b><br>"
        
        if news_data:
            for n in news_data[:3]: # 只取前 3 筆，避免面板太大擋住視線
                title = n.get('title', '無標題')
                # 如果標題太長，稍微截斷以保持面板整潔
                if len(title) > 35:
                    title = title[:35] + "..."
                publisher = n.get('publisher', '未知來源')
                news_text += f"• {title} <i>({publisher})</i><br>"
        else:
            news_text += "目前無最新情報"
    except Exception as e:
        news_text = "<b>📡 外部情報雷達連線失敗</b>"


    # 抓取較長時間範圍，以便於縮放選擇
    data = yf.download(ticker, start="2023-01-01", progress=False) 
    if data.empty:
        print(f"無法獲取 {ticker} 的歷史資料。")
        return
        
    df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()

    # 根據市場設定漲跌顏色 (台股: 漲紅跌綠, 美股: 漲綠跌紅)
    is_taiwan_market = True 
    color_up = '#FF5252' if is_taiwan_market else '#00E676'
    color_down = '#00E676' if is_taiwan_market else '#FF5252'

    # 後續成交量計算改為：
    vol_colors = [color_down if df['Close'].iloc[i] < df['Open'].iloc[i] else color_up for i in range(len(df))]

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
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['MACD_Signal'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['MACD_Signal']) * 2

    # C. 【新增】布林通道計算 (20日均線, 2倍標準差)
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['BB_std'] = df['Close'].rolling(window=20).std()
    df['BB_Upper'] = df['MA20'] + (df['BB_std'] * 2)
    df['BB_Lower'] = df['MA20'] - (df['BB_std'] * 2)

    # 【新增】動態公差感測器：ATR (真實波動幅度) 計算
    df['Prev_Close'] = df['Close'].shift(1)
    df['TR'] = df[['High', 'Low', 'Prev_Close']].apply(
        lambda x: max(x['High'] - x['Low'], abs(x['High'] - x['Prev_Close']), abs(x['Low'] - x['Prev_Close'])), 
        axis=1
    )
    df['ATR'] = df['TR'].rolling(window=14).mean() # 採用14日平滑處理
    df.drop(['Prev_Close', 'TR'], axis=1, inplace=True)
    df.dropna(inplace=True)

    df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
    # D. 【新增】買賣訊號邏輯 (複合條件連動)
    buy_condition = (df['Low'] <= df['BB_Lower']) & (df['RSI'] < 35) & (df['Volume'] > df['Vol_MA20'] * 1.5)
    sell_condition = (df['High'] >= df['BB_Upper']) & (df['RSI'] > 65) & (df['Volume'] > df['Vol_MA20'] * 1.5)

    df['Buy_Signal'] = np.where(buy_condition, df['Low'] * 0.98, np.nan)
    df['Sell_Signal'] = np.where(sell_condition, df['High'] * 1.02, np.nan)

    # 【新增】⚙️ 策略回測引擎 (State Machine)
    position = 0 
    entry_price = 0
    trades = [] 

    for index, row in df.iterrows():
        if position == 0 and not pd.isna(row['Buy_Signal']):
            position = 1
            entry_price = row['Close'] 
            entry_date = index
        elif position == 1 and not pd.isna(row['Sell_Signal']):
            exit_price = row['Close']
            profit_pct = (exit_price - entry_price) / entry_price * 100 
            trades.append({
                '進場日': entry_date, '出場日': index,
                '進場價': entry_price, '出場價': exit_price,
                '報酬率(%)': profit_pct
            })
            position = 0 

    if len(trades) > 0:
        trades_df = pd.DataFrame(trades)
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['報酬率(%)'] > 0])
        win_rate = winning_trades / total_trades * 100
        total_profit = trades_df['報酬率(%)'].sum() 
        backtest_text = (
            f"<b>⚙️ 策略回測報告 (BBands+RSI)</b><br>"
            f"• 總交易趟數：{total_trades} 次<br>"
            f"• 系統勝率：<span style='color:gold'>{win_rate:.1f}%</span><br>"
            f"• 累計報酬率：<span style='color:{color_up if total_profit > 0 else color_down}'>{total_profit:.2f}%</span><br>"
        )
    else:
        backtest_text = "<b>⚙️ 策略回測報告</b><br>• 在此區間內無完整交易觸發"

    # -------------------------------
    # 2. 強化版背離偵測函數 (導入 ATR 動態公差)
    # -------------------------------
    def find_divergence(price_series, indicator_series, atr_series, is_top=True, distance=7, atr_mult=1.0, threshold=None):
        dynamic_prominence = atr_series * atr_mult
        if is_top:
            peaks, _ = find_peaks(price_series, distance=distance, prominence=dynamic_prominence)
        else:
            peaks, _ = find_peaks(-price_series, distance=distance, prominence=dynamic_prominence)
        
        div_signals = []
        for i in range(1, len(peaks)):
            p1, p2 = peaks[i-1], peaks[i]
            if is_top:
                cond_price = price_series[p2] > price_series[p1]
                cond_indicator = indicator_series[p2] < indicator_series[p1]
                cond_thresh = True if threshold is None else indicator_series[p2] > threshold
                if cond_price and cond_indicator and cond_thresh:
                    div_signals.append((p1, p2))
            else:
                cond_price = price_series[p2] < price_series[p1]
                cond_indicator = indicator_series[p2] > indicator_series[p1]
                cond_thresh = True if threshold is None else indicator_series[p2] < threshold
                if cond_price and cond_indicator and cond_thresh:
                    div_signals.append((p1, p2))
        return div_signals

    rsi_top = find_divergence(df['High'].values, df['RSI'].values, df['ATR'].values, is_top=True, distance=7, atr_mult=1.0, threshold=55)
    rsi_bot = find_divergence(df['Low'].values, df['RSI'].values, df['ATR'].values, is_top=False, distance=7, atr_mult=1.0, threshold=45)
    macd_top = find_divergence(df['High'].values, df['DIF'].values, df['ATR'].values, is_top=True, distance=7, atr_mult=0.8, threshold=0)
    macd_bot = find_divergence(df['Low'].values, df['DIF'].values, df['ATR'].values, is_top=False, distance=7, atr_mult=0.8, threshold=0)

    # -------------------------------
    # 3. 繪製圖表 (UI 升級版: 4 子圖)
    # -------------------------------
    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.025, 
        row_heights=[0.45, 0.15, 0.2, 0.2], 
        subplot_titles=("價格走勢、布林通道與背離訊號", "成交量 (Volume)", "RSI 強弱指標", "MACD 柱狀圖")
    )

    fig.add_trace(go.Candlestick(x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'], 
        increasing_line_color=color_up, decreasing_line_color=color_down, name='股價', opacity=0.8, showlegend=False), row=1, col=1)

    fig.add_trace(go.Scatter(x=df.index, y=df['BB_Upper'], line=dict(color='#555', width=1, dash='dot'), name='BB 上軌', showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['BB_Lower'], line=dict(color='#555', width=1, dash='dot'), name='BB 下軌', showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index.tolist() + df.index.tolist()[::-1], y=df['BB_Upper'].tolist() + df['BB_Lower'].tolist()[::-1],
        fill='toself', fillcolor='rgba(100, 100, 100, 0.1)', line=dict(color='rgba(0,0,0,0)'), name='BB 區域', showlegend=False), row=1, col=1)

    fig.add_trace(go.Scatter(x=df.index, y=df['Buy_Signal'], mode='markers', marker=dict(symbol='triangle-up', size=12, color=color_up, line=dict(width=1, color='white')), name='買入訊號 (Buy)', hoverinfo='x+y'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['Sell_Signal'], mode='markers', marker=dict(symbol='triangle-down', size=12, color=color_down, line=dict(width=1, color='white')), name='賣出訊號 (Sell)', hoverinfo='x+y'), row=1, col=1)

    fig.add_trace(go.Bar(x=df.index, y=df['Volume'], marker_color=vol_colors, name='成交量', opacity=0.6), row=2, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['RSI'], line=dict(color='#BA68C8', width=2), name='RSI'), row=3, col=1)
    fig.add_hrect(y0=70, y1=100, fillcolor="red", opacity=0.08, line_width=0, row=3, col=1)
    fig.add_hrect(y0=0, y1=30, fillcolor="green", opacity=0.08, line_width=0, row=3, col=1)
    fig.add_shape(type="line", x0=df.index[0], y0=50, x1=df.index[-1], y1=50, line=dict(color="#555", width=1, dash="dash"), row=3, col=1)

    macd_colors = [color_up if x > 0 else color_down for x in df['MACD_Hist']]
    fig.add_trace(go.Bar(x=df.index, y=df['MACD_Hist'], marker_color=macd_colors, name='MACD 柱狀圖'), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['DIF'], line=dict(color='gold', width=1.5), name='DIF (快線)'), row=4, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['MACD_Signal'], line=dict(color='#00BFFF', width=1.5), name='Signal (慢線)'), row=4, col=1)

    def plot_div_pro(div_list, p_data, i_data, color, name, i_row, is_top=True):
        ay_offset = -50 if is_top else 50
        for p1, p2 in div_list:
            fig.add_trace(go.Scatter(x=[df.index[p1], df.index[p2]], y=[p_data[p1], p_data[p2]], line=dict(color=color, width=2, dash='dot'), marker=dict(size=8, symbol='circle-open', line=dict(width=2)), mode='lines+markers', showlegend=False, hoverinfo='skip'), row=1, col=1)
            fig.add_trace(go.Scatter(x=[df.index[p1], df.index[p2]], y=[i_data[p1], i_data[p2]], line=dict(color=color, width=2), mode='lines+markers', name=name, showlegend=False), row=i_row, col=1)
            fig.add_annotation(x=df.index[p2], y=p_data[p2], text=name, showarrow=True, arrowhead=2, arrowsize=1, arrowcolor=color, bgcolor=color, font=dict(color="#111", size=10), ax=0, ay=ay_offset, row=1, col=1)

    plot_div_pro(rsi_top, df['High'].values, df['RSI'].values, '#FF5252', 'RSI 頂背', 3, is_top=True)
    plot_div_pro(rsi_bot, df['Low'].values, df['RSI'].values, '#00E676', 'RSI 底背', 3, is_top=False)
    plot_div_pro(macd_top, df['High'].values, df['DIF'].values, '#FFB74D', 'MACD 頂背', 4, is_top=True)
    plot_div_pro(macd_bot, df['Low'].values, df['DIF'].values, '#4FC3F7', 'MACD 底背', 4, is_top=False)

    # -------------------------------
    # 4. 佈局細節調整
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
            bgcolor="#333", activecolor="gold", font=dict(color="white"), x=0, y=1.05 
        ),
        rangeslider=dict(visible=False), gridcolor='#222', tickfont=dict(color='#999')
    )
    fig.update_yaxes(gridcolor='#222', fixedrange=False, tickfont=dict(color='#999'))
    fig.update_yaxes(showticklabels=False, row=2, col=1) 

    fig.update_layout(
        height=1000, template='plotly_dark', paper_bgcolor='#0a0a0a', plot_bgcolor='#0a0a0a',
        title=dict(text=f"<b>{ticker} 結構化分析儀表板</b><br><span style='font-size:12px;color:#999'>布林通道 / 價格動能 / 多重背離偵測</span>", x=0.5, font=dict(size=22, color='gold'), y=0.98),
        hovermode='x unified', margin=dict(t=100, b=30, l=50, r=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.add_annotation(text=news_text, align='left', showarrow=False, xref='paper', yref='paper', x=0.01, y=0.98, bgcolor='rgba(30, 30, 30, 0.7)', bordercolor='gold', borderwidth=1, borderpad=8, font=dict(size=11, color='#E0E0E0'))
    fig.add_annotation(text=backtest_text, align='left', showarrow=False, xref='paper', yref='paper', x=0.99, y=0.60, bgcolor='rgba(10, 40, 20, 0.8)', bordercolor='#00E676', borderwidth=1.5, borderpad=12, font=dict(size=13, color='#F5F5F5'))

    fig.show()

# ==========================================
# 🚀 手動單機測試開關
# ==========================================
if __name__ == "__main__":
    # 當你沒有啟動海選，只是單獨執行這支程式時，會自動跑這裡
    test_target = "2330.TW"
    print(f"啟動手動測試模式，正在渲染 {test_target} 的精密圖紙...")
    draw_chart(test_target)