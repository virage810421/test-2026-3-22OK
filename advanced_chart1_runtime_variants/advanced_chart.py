import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.io as pio
pio.renderers.default = "browser"
from plotly.subplots import make_subplots
from .config import PARAMS

# ==========================================
# ⚙️ 精密儀表板模組（升級版）
# ==========================================
def draw_chart(
    ticker,
    preloaded_df=None,
    win_rate="N/A",
    total_profit="N/A",
    expected_value="N/A",
    ai_proba="N/A",
    signal_confidence="N/A",
    sample_size="N/A",
    p=PARAMS
):
    print(f"\n[系統提示] 啟動 {ticker} 的精密繪圖引擎（升級版）...")

    if preloaded_df is not None:
        df = preloaded_df.copy()
    else:
        data = yf.download(ticker, period="2y", progress=False)
        if data.empty:
            return
        df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()

    try:
        ticker_obj = yf.Ticker(ticker)
        news_data = ticker_obj.news
        news_text = "<b>📡 最新外部情報：</b><br>"

        if news_data:
            for n in news_data[:3]:
                title = n.get("title", "無標題")
                if len(title) > 35:
                    title = title[:35] + "."
                publisher = n.get("publisher", "未知")
                news_text += f"• {title} <i>({publisher})</i><br>"
        else:
            news_text += "目前無最新情報"
    except Exception:
        news_text = "<b>📡 外部情報雷達連線失敗</b>"

    if df.empty or len(df) < 10:
        print(f"⚠️ {ticker} 繪圖引擎警告：資料不完整，已安全跳過。")
        return

    trigger_score = max(2, p.get("TRIGGER_SCORE", 2))
    df["Buy_Signal"] = np.where(df["Buy_Score"] >= trigger_score, df["Low"] * 0.985, np.nan)
    df["Sell_Signal"] = np.where(df["Sell_Score"] >= trigger_score, df["High"] * 1.015, np.nan)

    color_up, color_down = '#FF5252', '#00E676'
    vol_colors = [color_down if df["Close"].iloc[i] < df["Open"].iloc[i] else color_up for i in range(len(df))]

    fig = make_subplots(
        rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.02,
        row_heights=[0.35, 0.15, 0.20, 0.15, 0.15],
        subplot_titles=("價格走勢與布林通道線", "成交量", "三大法人買賣超", "RSI 強弱指標", "MACD 柱狀圖")
    )

    fig.add_trace(
        go.Candlestick(
            x=df.index, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"],
            increasing_line_color=color_up, decreasing_line_color=color_down,
            name="股價", opacity=0.8, showlegend=False
        ),
        row=1, col=1
    )

    df["MA_LONG"] = df["Close"].rolling(window=p["MA_LONG"]).mean()
    fig.add_trace(go.Scatter(x=df.index, y=df["MA_LONG"], line=dict(color="#2196F3", width=2), name=f"MA{p['MA_LONG']}"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["BB_Upper"], line=dict(color="#555", width=1, dash="dot"), showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["BB_Lower"], line=dict(color="#555", width=1, dash="dot"), showlegend=False), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df.index, y=df["Buy_Signal"], mode="markers",
        marker=dict(symbol="triangle-up", size=12, color=color_up, line=dict(width=1, color="white")),
        name="買入訊號", hoverinfo="x+y"
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df.index, y=df["Sell_Signal"], mode="markers",
        marker=dict(symbol="triangle-down", size=12, color=color_down, line=dict(width=1, color="white")),
        name="賣出訊號", hoverinfo="x+y"
    ), row=1, col=1)

    fig.add_trace(go.Bar(x=df.index, y=df["Volume"], marker_color=vol_colors, name="成交量", opacity=0.6), row=2, col=1)

    has_chip_data = False
    if "Foreign_Net" in df.columns:
        fig.add_trace(go.Bar(x=df.index, y=df["Foreign_Net"], name="外資", marker_color="#00BCD4", opacity=0.8), row=3, col=1)
        has_chip_data = True
    if "Trust_Net" in df.columns:
        fig.add_trace(go.Bar(x=df.index, y=df["Trust_Net"], name="投信", marker_color="#FF9800", opacity=0.8), row=3, col=1)
        has_chip_data = True
    if "Dealers_Net" in df.columns:
        fig.add_trace(go.Bar(x=df.index, y=df["Dealers_Net"], name="自營商", marker_color="#9C27B0", opacity=0.8), row=3, col=1)
        has_chip_data = True
    if not has_chip_data:
        fig.add_annotation(
            text="(⚠️ 籌碼無資料)", xref="paper", yref="paper",
            x=0.5, y=0.5, row=3, col=1, showarrow=False,
            font=dict(color="white", size=14)
        )

    fig.add_trace(go.Scatter(x=df.index, y=df["RSI"], line=dict(color="#BA68C8", width=2), name="RSI"), row=4, col=1)
    fig.add_hrect(y0=70, y1=100, fillcolor="red", opacity=0.08, line_width=0, row=4, col=1)
    fig.add_hrect(y0=0, y1=30, fillcolor="green", opacity=0.08, line_width=0, row=4, col=1)

    macd_colors = [color_up if x > 0 else color_down for x in df["MACD_Hist"]]
    fig.add_trace(go.Bar(x=df.index, y=df["MACD_Hist"], marker_color=macd_colors, name="MACD Hist", opacity=0.7), row=5, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["DIF"], line=dict(color="#FFD54F", width=1.5), name="DIF"), row=5, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MACD_Signal"], line=dict(color="#4FC3F7", width=1.5), name="Signal"), row=5, col=1)

    fig.update_yaxes(gridcolor="#222", fixedrange=False)
    fig.update_yaxes(showticklabels=False, row=2, col=1)
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])], rangeslider=dict(visible=False), gridcolor="#222")

    fig.update_layout(
        height=980,
        barmode="group",
        template="plotly_dark",
        paper_bgcolor="#0a0a0a",
        plot_bgcolor="#0a0a0a",
        title=dict(text=f"<b>{ticker} 精密戰略分析儀表板（升級版）</b>", x=0.5, font=dict(size=22, color="gold"), y=0.98),
        hovermode="x unified",
        margin=dict(t=100, b=30, l=50, r=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.add_annotation(
        text=news_text, align="left", showarrow=False,
        xref="paper", yref="paper", x=0.01, y=0.98,
        bgcolor="rgba(30, 30, 30, 0.7)", bordercolor="gold", borderwidth=1,
        borderpad=8, font=dict(size=11, color="#E0E0E0")
    )

    try:
        max_score = 8
        current_regime = df["Regime"].iloc[-1] if "Regime" in df.columns else "未知"
        golden_tag = df["Golden_Type"].iloc[-1] if "Golden_Type" in df.columns else "無"
        display_tag = golden_tag if golden_tag != "無" else "傳統波段"

        signal_text = (
            f"<b>💡 訊號觀測站</b><br>"
            f"大環境: <span style='color:gold'><b>{current_regime}</b></span><br>"
            f"今日陣型: <span style='color:#00BFFF'><b>{display_tag}</b></span><br>"
            f"多方得分: {int(df['Buy_Score'].iloc[-1])}/{max_score}<br>"
            f"空方得分: {int(df['Sell_Score'].iloc[-1])}/{max_score}"
        )
    except Exception:
        signal_text = "<b>💡 訊號觀測站</b><br>得分: 計算中"

    if win_rate is not None and total_profit is not None and win_rate != "N/A":
        color_prof = "#FF5252" if float(total_profit) > 0 else "#00E676"
        ev_val = float(expected_value) if expected_value != "N/A" else 0.0
        color_ev = "#FF5252" if ev_val > 0 else "#00E676"

        signal_text += (
            f"<br>──────────<br>"
            f"系統勝率: {float(win_rate):.2f}%<br>"
            f"累計報酬: <span style='color:{color_prof}'>{float(total_profit):.2f}%</span><br>"
            f"Realized EV: <span style='color:{color_ev}'><b>{ev_val:.3f}%</b></span>"
        )

    if ai_proba != "N/A":
        signal_text += f"<br>AI勝率: {float(ai_proba):.2%}"
    if signal_confidence != "N/A":
        signal_text += f"<br>訊號信心: {float(signal_confidence):.2%}"
    if sample_size != "N/A":
        signal_text += f"<br>樣本數: {int(sample_size)}"

    fig.add_annotation(
        text=signal_text, align="left", showarrow=False,
        xref="paper", yref="paper", x=0.99, y=0.6,
        bgcolor="rgba(10, 40, 20, 0.8)", bordercolor="#00BFFF", borderwidth=1.5,
        borderpad=10, font=dict(size=13, color="#F5F5F5")
    )

    print(f"✅ {ticker} 實戰儀表板彈出中，請注意網頁視窗。")
    fig.show()