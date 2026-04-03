# dashboard.py
import dash
from dash import dcc, html
import plotly.graph_objs as go
import pandas as pd
import pyodbc
from dash.dependencies import Input, Output
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ 資料庫連線設定 (直接對接大帳房)
# ==========================================
DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

# 初始化 Dash 網頁應用程式 (使用深色主題背景)
app = dash.Dash(__name__)
app.title = "量化交易戰情室"

# ==========================================
# 📊 資料抓取與處理模組
# ==========================================
def fetch_trade_data():
    """從 SQL 實時抓取歷史交易紀錄"""
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = "SELECT * FROM backtest_history WHERE [結餘本金] IS NOT NULL ORDER BY [出場時間] ASC"
            df = pd.read_sql(query, conn)
            
            if df.empty: return pd.DataFrame()
            
            df['出場時間'] = pd.to_datetime(df['出場時間'])
            df['報酬率(%)'] = pd.to_numeric(df['報酬率(%)'], errors='coerce')
            df['淨損益金額'] = pd.to_numeric(df['淨損益金額'], errors='coerce')
            df['結餘本金'] = pd.to_numeric(df['結餘本金'], errors='coerce')
            
            # 計算勝負標記
            df['is_win'] = df['淨損益金額'] > 0
            df['win_rate_accum'] = df['is_win'].expanding().mean() * 100
            
            return df
    except Exception as e:
        print(f"資料庫連線錯誤: {e}")
        return pd.DataFrame()

# ==========================================
# 🎨 網頁前端佈局 (Layout)
# ==========================================
app.layout = html.Div(style={'backgroundColor': '#111111', 'color': '#ffffff', 'padding': '20px', 'fontFamily': 'Arial, sans-serif'}, children=[
    html.H1("📊 量化交易戰情指揮中心 (Hedge Fund Level)", style={'textAlign': 'center', 'color': 'gold'}),
    
    # 頂部狀態列
    html.Div(id='live-status-bar', style={'display': 'flex', 'justifyContent': 'space-around', 'fontSize': '24px', 'fontWeight': 'bold', 'margin': '20px 0', 'padding': '10px', 'backgroundColor': '#222', 'borderRadius': '10px'}),

    # 圖表區塊 (2x2 網格佈局)
    html.Div([
        html.Div([dcc.Graph(id='chart-equity')], style={'width': '48%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(id='chart-drawdown')], style={'width': '48%', 'display': 'inline-block', 'float': 'right'}),
    ]),
    
    html.Div([
        html.Div([dcc.Graph(id='chart-strategy')], style={'width': '48%', 'display': 'inline-block', 'marginTop': '20px'}),
        html.Div([dcc.Graph(id='chart-winrate')], style={'width': '48%', 'display': 'inline-block', 'float': 'right', 'marginTop': '20px'}),
    ]),

    # 定時更新器 (每 10 秒刷新一次)
    dcc.Interval(id='interval-component', interval=10000, n_intervals=0)
])

# ==========================================
# 🔄 即時更新回呼函數 (Callbacks)
# ==========================================
@app.callback(
    [Output('live-status-bar', 'children'),
     Output('chart-equity', 'figure'),
     Output('chart-drawdown', 'figure'),
     Output('chart-strategy', 'figure'),
     Output('chart-winrate', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n):
    df = fetch_trade_data()
    
    # 如果還沒有交易紀錄，給予空圖表
    if df.empty:
        empty_fig = go.Figure(layout=go.Layout(template='plotly_dark', plot_bgcolor='#111', paper_bgcolor='#111'))
        return ["等待交易資料寫入中...", empty_fig, empty_fig, empty_fig, empty_fig]

    # --- 1. 頂部數據列 ---
    current_equity = df['結餘本金'].iloc[-1]
    total_trades = len(df)
    current_winrate = df['win_rate_accum'].iloc[-1]
    
    status_bar = [
        html.Div(f"💰 目前總資金: ${current_equity:,.0f}", style={'color': '#00E676'}),
        html.Div(f"🎯 總交易筆數: {total_trades} 筆", style={'color': '#00BFFF'}),
        html.Div(f"🏆 系統總勝率: {current_winrate:.1f}%", style={'color': '#FF9800'})
    ]

    # --- 2. 資金曲線 (Equity Curve) ---
    fig_equity = go.Figure(layout=go.Layout(title='💰 資金成長曲線 (Equity Curve)', template='plotly_dark', plot_bgcolor='#111', paper_bgcolor='#111'))
    fig_equity.add_trace(go.Scatter(x=df['出場時間'], y=df['結餘本金'], mode='lines', fill='tozeroy', line=dict(color='#00E676', width=2), name='帳戶淨值'))

    # --- 3. 最大回撤 (MDD) ---
    peak = df['結餘本金'].cummax()
    drawdown = ((df['結餘本金'] - peak) / peak) * 100
    fig_dd = go.Figure(layout=go.Layout(title='📉 系統回撤監控 (Drawdown %)', template='plotly_dark', plot_bgcolor='#111', paper_bgcolor='#111'))
    fig_dd.add_trace(go.Scatter(x=df['出場時間'], y=drawdown, mode='lines', fill='tozeroy', line=dict(color='#FF5252', width=1.5), name='MDD'))
    fig_dd.add_hline(y=-10, line_dash="dash", line_color="orange", annotation_text="一級防護線")
    fig_dd.add_hline(y=-20, line_dash="dash", line_color="red", annotation_text="絕對熔斷線")

    # --- 4. 各陣型期望值貢獻 (Strategy Bar) ---
    strat_grouped = df.groupby('進場陣型')['報酬率(%)'].mean().sort_values(ascending=True)
    colors = ['#FF5252' if val < 0 else '#00BFFF' for val in strat_grouped.values]
    fig_strat = go.Figure(layout=go.Layout(title='🔥 陣型平均報酬率 (EV 追蹤)', template='plotly_dark', plot_bgcolor='#111', paper_bgcolor='#111'))
    fig_strat.add_trace(go.Bar(y=strat_grouped.index, x=strat_grouped.values, orientation='h', marker_color=colors))

    # --- 5. 勝率演進曲線 (Win Rate over time) ---
    fig_wr = go.Figure(layout=go.Layout(title='📈 勝率穩定度追蹤', template='plotly_dark', plot_bgcolor='#111', paper_bgcolor='#111'))
    fig_wr.add_trace(go.Scatter(x=df['出場時間'], y=df['win_rate_accum'], mode='lines', line=dict(color='#FF9800', width=2), name='累積勝率'))
    fig_wr.add_hline(y=50, line_dash="dot", line_color="gray")

    return status_bar, fig_equity, fig_dd, fig_strat, fig_wr

if __name__ == '__main__':
    print("🚀 正在啟動量化戰情指揮中心...")
    print("👉 請打開瀏覽器輸入: http://127.0.0.1:8050")
    app.run(debug=False, port=8050)         # ✨ 新版語法