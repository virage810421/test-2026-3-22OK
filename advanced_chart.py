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
def draw_chart(ticker, preloaded_df=None, backtest_info=None):
    print(f"\n[系統提示] 收到海選雷達傳來的訊號！正在啟動 {ticker} 的精密繪圖引擎...")
    
    
    # -------------------------------
    # 1. 數據獲取與指標計算 (新增 BBands)
    # -------------------------------
   # 【新增】啟動外部新聞雷達 (攔截最新 3 則情報)
    try:
        ticker_obj = yf.Ticker(ticker)
        news_data = ticker_obj.news
        news_text = "<b>📡 最新外部情報雷達：</b><br>"
        
        if news_data:
            for n in news_data[:3]: 
                title = n.get('title', '無標題')
                if len(title) > 35:
                    title = title[:35] + "..."
                publisher = n.get('publisher', '未知來源')
                news_text += f"• {title} <i>({publisher})</i><br>"
        else:
            news_text += "目前無最新情報"
    except Exception as e:
        news_text = "<b>📡 外部情報雷達連線失敗</b>"

    # ==========================================
    # 🔌 核心更動：只接收大腦資料，不自己做任何運算
    # ==========================================
    if preloaded_df is None or preloaded_df.empty:
        print(f"⚠️ 拒絕渲染：{ticker} 沒有接收到大腦傳來的資料表。")
        return

    # 直接拷貝大腦算好的 DataFrame (裡面已經有 Buy_Signal, Sell_Signal, RSI, MACD 等) [cite: 52, 53, 64]
    df = preloaded_df.copy()

    # 確保純畫圖需要的視覺輔助線存在 (如果大腦沒給的話，顯示器自己補)
    if 'MA60' not in df.columns:
        df['MA60'] = df['Close'].rolling(window=60).mean()
    if 'ATR' not in df.columns:
        df['Prev_Close'] = df['Close'].shift(1)
        df['真實波幅 (TR)'] = np.maximum.reduce([
            df['High'] - df['Low'],
            (df['High'] - df['Prev_Close']).abs(),
            (df['Low'] - df['Prev_Close']).abs()
        ])
        df['ATR'] = df['真實波幅 (TR)'].ewm(alpha=1/14, adjust=False).mean()
        df.drop(['Prev_Close'], axis=1, inplace=True)

    # 確保不會因為空值導致畫圖報錯
    if len(df) == 0:
        print(f"⚠️ {ticker} 拒絕渲染：資料表內無有效行數。")
        return

    # 根據市場設定漲跌顏色 
    is_taiwan_market = True 
    color_up = '#FF5252' if is_taiwan_market else '#00E676'
    color_down = '#00E676' if is_taiwan_market else '#FF5252'
    vol_colors = [color_down if df['Close'].iloc[i] < df['Open'].iloc[i] else color_up for i in range(len(df))]

    # 簡化右上角面板 (因為回測已經在大腦處理完了)
    # 把原本這行刪除：
    # backtest_text = "<b>⚙️ 訊號圖表模式</b><br>• 已成功載入大腦決策訊號與運算結果"

    # 替換成這段（動態讀取大腦成績單）：
    if backtest_info:
        win_rate = backtest_info.get("系統勝率(%)", "0.000")
        total_profit = backtest_info.get("累計報酬率(%)", "0.000")
        # 根據台股習慣：賺錢(正)為紅，賠錢(負)為綠
        profit_color = '#FF5252' if float(total_profit) > 0 else '#00E676'
        
        backtest_text = (
            f"<b>⚙️ 策略回測報告</b><br>"
            f"• 系統勝率：<span style='color:gold'>{win_rate}%</span><br>"
            f"• 累計報酬率：<span style='color:{profit_color}'>{total_profit}%</span>"
        )
    else:
        backtest_text = "<b>⚙️ 訊號圖表模式</b><br>• 純視覺化渲染 (無回測資料)"

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