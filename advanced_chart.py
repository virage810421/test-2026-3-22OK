import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import find_peaks
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "browser"
from plotly.subplots import make_subplots

# ==========================================
# ⚙️ 核心封裝：精密儀表板模組
# ==========================================
def draw_chart(ticker, preloaded_df=None):
    print(f"\n[系統提示] 收到海選雷達傳來的訊號！正在啟動 {ticker} 的精密繪圖引擎...")
    
    
    # -------------------------------
    # 1. 數據獲取與指標計算 (新增 BBands)
    # -------------------------------
    if preloaded_df is not None:
        df = preloaded_df.copy()
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


    # -------------------------------
    # ⚡️ 批次下載切換邏輯 (資料來源分流)
    # -------------------------------
    if preloaded_df is not None:
        df = preloaded_df.copy()
        if df.empty:
            print(f"⚠️ {ticker} 的預載資料為空，跳過繪圖。")
            return
    else:
        data = yf.download(ticker, period="2y", progress=False) 
        if data.empty:
            print(f"⚠️ 無法獲取 {ticker} 的歷史資料。")
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
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))

    # B. MACD 計算
    df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['MACD_Signal'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['MACD_Signal']) * 2

   # C. 布林通道與成交量計算
    df['MA20'] = df['Close'].rolling(window=20).mean()
    df['MA60'] = df['Close'].rolling(window=60).mean()
    df['BB_std'] = df['Close'].rolling(window=20).std()
    df['BB_Upper'] = df['MA20'] + (df['BB_std'] * 2)
    df['BB_Lower'] = df['MA20'] - (df['BB_std'] * 2)
    df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()

    # ==========================================
# ⚙️ 動態公差感測器：優化版 ATR 計算 (插入此處)
# ==========================================
# 1. 向量化計算前一收盤價
    df['Prev_Close'] = df['Close'].shift(1)

# 2. 你的向量化優化邏輯：計算 TR (真實波幅)
    df['真實波幅 (TR)'] = np.maximum.reduce([
    df['High'] - df['Low'],
    (df['High'] - df['Prev_Close']).abs(),
    (df['Low'] - df['Prev_Close']).abs()
    ])

# 3. 計算 ATR (改用 .ewm 威爾德平滑法，比 .rolling 更精準)
    window_atr = 14
    df['ATR'] = df['真實波幅 (TR)'].ewm(alpha=1/window_atr, adjust=False).mean()

# 4. 清理暫存欄位，並移除初始 NaN (確保後續邏輯不報錯)
    df.drop(['Prev_Close'], axis=1, inplace=True) # 暫時保留 TR 給背離偵測，或之後一起刪
    df.dropna(inplace=True)

    # D. ⚙️ 計分型邏輯閘 (滿分 4 分，得 3 分觸發)
    buy_c1 = df['Low'] <= df['BB_Lower']
    buy_c2 = df['RSI'] < 35
    buy_c3 = df['Volume'] > (df['Vol_MA20'] * 1.1)
    buy_c4 = (df['MACD_Hist'] > df['MACD_Hist'].shift(1)) & (df['DIF'] < 0)
    df['Buy_Score'] = buy_c1.astype(int) + buy_c2.astype(int) + buy_c3.astype(int) + buy_c4.astype(int)

    sell_c1 = df['High'] >= df['BB_Upper']
    sell_c2 = df['RSI'] > 65
    sell_c3 = df['Volume'] > (df['Vol_MA20'] * 1.1)
    sell_c4 = (df['MACD_Hist'] < df['MACD_Hist'].shift(1)) & (df['DIF'] > 0)
    df['Sell_Score'] = sell_c1.astype(int) + sell_c2.astype(int) + sell_c3.astype(int) + sell_c4.astype(int)

    # 圖表專屬輸出：輸出「價格座標」，讓 Plotly 知道要在哪裡畫出三角形箭頭
    df['Buy_Signal'] = np.where(df['Buy_Score'] >= 3, df['Low'] * 0.98, np.nan)
    df['Sell_Signal'] = np.where(df['Sell_Score'] >= 3, df['High'] * 1.02, np.nan)
    

    # 【升級】⚙️ 策略回測引擎 (雙向作動：做多 + 放空)
    # 💡 【新增區塊】：設定機台摩擦係數 (交易成本)
    fee = 0.0015      # 單趟手續費 (0.15%)
    slippage = 0.001  # 單趟滑價 (0.1%)
    # 先算好「來回一趟」的總耗損(%)，讓迴圈內的運算更快速
    round_trip_cost_pct = (fee + slippage) * 100 * 2 

    stop_loss = 0.05     # 🛑 停損設定：-5% (虧損 5% 強制斷電)
    take_profit = 0.15   # 🎯 停利設定：+15% (獲利 15% 提早收割)

    position = 0  # 狀態變數：0 代表空手，1 代表持有多單，-1 代表持有空單
    entry_price = 0
    trades = []

    for index, row in df.iterrows():
        # 狀況 A：【空手】狀態下，尋找進場點
        if position == 0:
            buy_val = row['Buy_Score']
            sell_val = row['Sell_Score']

            # 買方力道強於賣方，且達到進場門檻
            if buy_val > sell_val and not pd.isna(row['Buy_Signal']):
                position = 1
                entry_price = row['Close']
                entry_date = index
                trade_type = "做多(Long)"

            # 賣方力道強於買方，且達到進場門檻
            elif sell_val > buy_val and not pd.isna(row['Sell_Signal']):
                position = -1
                entry_price = row['Close']
                entry_date = index
                trade_type = "放空(Short)"

        # 狀況 B：持有【多單】時，偵測三道防線
        elif position == 1:
            # 即時計算當下報酬率 (%)
            current_profit_pct = ((row['Close'] - entry_price) / entry_price * 100) - round_trip_cost_pct
            
            exit_triggered = False
            exit_reason = ""
            
            if current_profit_pct <= -(stop_loss * 100):
                exit_triggered = True
                exit_reason = "🛑停損斷電"
            elif current_profit_pct >= (take_profit * 100):
                exit_triggered = True
                exit_reason = "🎯停利收割"
            elif not pd.isna(row['Sell_Signal']):
                exit_triggered = True
                exit_reason = "🔄訊號平倉"
                
            if exit_triggered:
                trades.append({
                    '方向': trade_type, '進場日': entry_date, '出場日': index,
                    '進場價': entry_price, '出場價': row['Close'], 
                    '報酬率(%)': current_profit_pct, '出場原因': exit_reason
                })
                position = 0 # 恢復空手

        # 狀況 C：持有【空單】時，偵測三道防線
        elif position == -1:
            # 即時計算當下報酬率 (%) (放空是越跌越賺)
            current_profit_pct = ((entry_price - row['Close']) / entry_price * 100) - round_trip_cost_pct
            
            exit_triggered = False
            exit_reason = ""
            
            if current_profit_pct <= -(stop_loss * 100):
                exit_triggered = True
                exit_reason = "🛑停損斷電"
            elif current_profit_pct >= (take_profit * 100):
                exit_triggered = True
                exit_reason = "🎯停利收割"
            elif not pd.isna(row['Buy_Signal']):
                exit_triggered = True
                exit_reason = "🔄訊號回補"
                
            if exit_triggered:
                trades.append({
                    '方向': trade_type, '進場日': entry_date, '出場日': index,
                    '進場價': entry_price, '出場價': row['Close'], 
                    '報酬率(%)': current_profit_pct, '出場原因': exit_reason
                })
                position = 0 # 恢復空手

    # 🚨 關鍵除錯機制：期末強制平倉 (把隱藏的套牢單逼出來算總帳)
    if position != 0:
        last_date = df.index[-1]
        last_price = df.iloc[-1]['Close']
        if position == 1:
            final_profit_pct = ((last_price - entry_price) / entry_price * 100) - round_trip_cost_pct
        else:
            final_profit_pct = ((entry_price - last_price) / entry_price * 100) - round_trip_cost_pct
            
        trades.append({
            '方向': trade_type, '進場日': entry_date, '出場日': last_date,
            '進場價': entry_price, '出場價': last_price, 
            '報酬率(%)': final_profit_pct, '出場原因': "⚠️期末強制結算"
        })

    # 結算總成績單
    if len(trades) > 0:
        trades_df = pd.DataFrame(trades)
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['報酬率(%)'] > 0])
        win_rate = winning_trades / total_trades * 100
        total_profit = trades_df['報酬率(%)'].sum() 
        
        # 區分做多和做空的次數，讓報表更細緻
        long_trades = len(trades_df[trades_df['方向'] == '做多(Long)'])
        short_trades = len(trades_df[trades_df['方向'] == '放空(Short)'])
        
        backtest_text = (
            f"<b>⚙️ 雙向策略回測報告 (多/空)</b><br>"
            f"• 總交易：{total_trades} 次 (多:{long_trades} / 空:{short_trades})<br>"
            f"• 系統勝率：<span style='color:gold'>{win_rate:.3f}%</span><br>"
            f"• 累計報酬率：<span style='color:{color_up if total_profit > 0 else color_down}'>{total_profit:.3f}%</span><br>"
        )

        # 列印終端機交易明細
        print(f"\n========================================================")
        print(f"📊 {ticker} 歷史模擬交易明細 (共 {total_trades} 趟):")
        
        log_df = trades_df.copy()
        log_df['進場日'] = log_df['進場日'].dt.strftime('%Y-%m-%d')
        log_df['出場日'] = log_df['出場日'].dt.strftime('%Y-%m-%d')
        log_df['進場價'] = log_df['進場價'].round(2)
        log_df['出場價'] = log_df['出場價'].round(2)
        log_df['報酬率(%)'] = log_df['報酬率(%)'].round(3) # 百分比控制 3 位小數
        
        print(log_df.to_string(index=False))
        print(f"========================================================\n")

    else:
        backtest_text = "<b>⚙️ 雙向策略回測報告</b><br>• 在此區間內無完整交易觸發"

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
    # 【新增】將 60 日季線畫在主圖上，作為多空分界線
    fig.add_trace(go.Scatter(x=df.index, y=df['MA60'], line=dict(color='#2196F3', width=2), name='季線 (MA60)'), row=1, col=1)
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

    test_targets =  [
        "2330.TW", "2454.TW", "2303.TW", "2337.TW", 
        "2317.TW", "2382.TW", "3231.TW", "2356.TW", "2376.TW", 
        "2603.TW", "2609.TW", "2615.TW", 
        "2881.TW", "2882.TW", "2884.TW", "2886.TW", "2891.TW",
        "1503.TW", "1519.TW", "1513.TW"
    ]
    print("啟動手動測試模式，開始批次分析...\n")
    for ticker in test_targets:
        draw_chart(ticker)    