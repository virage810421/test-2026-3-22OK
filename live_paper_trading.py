import time
import pandas as pd
from datetime import datetime
import yfinance as yf

# 假設你的第一支程式存為 advanced_chart.py，第二支存為 screening.py
from advanced_chart import draw_chart
from screening import inspect_stock, add_chip_data

# ==========================================
# 💼 虛擬帳戶與機台設定
# ==========================================
portfolio = {}       # 紀錄持倉狀態，格式: {'Ticker SYMBOL': {'進場價': 100, '方向': '做多(Long)'}}
trade_history = []   # 歷史交易紀錄
SCAN_INTERVAL = 300  # 盤中掃描間隔（秒），300秒 = 5分鐘
FEE_SLIPPAGE = 0.0025 # 單趟手續費+滑價假設 (0.25%)

watch_list = [
    "2330.TW", "2454.TW", "2303.TW", "2317.TW", 
    "2603.TW", "2881.TW", "1519.TW"
]

def run_live_simulation():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🟢 啟動盤中實戰模擬引擎...")
    
    while True:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        
        # 簡易休市判斷 (台灣時間 09:00 - 13:30)
        # 實戰中可視需求開啟此防護網
        # if now.hour < 9 or (now.hour == 13 and now.minute > 30) or now.hour > 13:
        #     print(f"[{current_time}] 目前非交易時間，系統待機中...")
        #     time.sleep(600)
        #     continue

        print(f"\n[{current_time}] 📡 啟動定時海選雷達，掃描 {len(watch_list)} 檔標的...")
        
        # 1. 批次下載最新即時/延遲報價
        try:
            batch_data = yf.download(watch_list, period="1mo", progress=False)
        except Exception as e:
            print(f"⚠️ 網路連線或下載失敗: {e}")
            time.sleep(60)
            continue

        for ticker in watch_list:
            if isinstance(batch_data.columns, pd.MultiIndex):
                ticker_df = batch_data.xs(ticker, axis=1, level=1).copy()
            else:
                ticker_df = batch_data.copy()
            
            ticker_df.dropna(how='all', inplace=True)
            if ticker_df.empty:
                continue
                
            # 加入籌碼資料
            ticker_df = add_chip_data(ticker_df, ticker)
            
            # 2. 進行策略檢驗
            result = inspect_stock(ticker, preloaded_df=ticker_df)
            if not result:
                continue
                
            status = result['今日系統燈號']
            current_price = result['最新收盤價']
            
            # 3. 執行模擬交易決策
            handle_paper_trade(ticker, current_price, status, ticker_df)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 掃描完成。進入冷卻等待 {SCAN_INTERVAL} 秒...")
        time.sleep(SCAN_INTERVAL)

def handle_paper_trade(ticker, current_price, status, ticker_df):
    """處理單一檔股票的模擬買賣邏輯"""
    has_position = ticker in portfolio
    
    # --- 狀況 A：偵測到買訊，且目前空手 ---
    if "強買訊" in status and not has_position:
        print(f"⚡ [進場觸發] {ticker} 產生 {status}！模擬買進價: {current_price}")
        portfolio[ticker] = {
            '進場價': current_price,
            '進場時間': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            '方向': '做多(Long)'
        }
        # 呼叫儀表板留下進場快照
        draw_chart(ticker, preloaded_df=ticker_df)

    # --- 狀況 B：偵測到賣訊，且目前持有多單 ---
    elif "強賣訊" in status and has_position:
        entry_data = portfolio.pop(ticker) # 取出並移除部位
        entry_price = entry_data['進場價']
        
        # 計算報酬率 (含摩擦成本)
        raw_profit_pct = (current_price - entry_price) / entry_price * 100
        net_profit_pct = raw_profit_pct - (FEE_SLIPPAGE * 100 * 2)
        
        trade_record = {
            'Ticker SYMBOL': ticker,
            '方向': entry_data['方向'],
            '進場時間': entry_data['進場時間'],
            '出場時間': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            '進場價': entry_price,
            '出場價': current_price,
            '報酬率(%)': f"{net_profit_pct:.3f}"
        }
        trade_history.append(trade_record)
        
        color = "🔴" if net_profit_pct > 0 else "🟢" # 台股紅漲綠跌
        print(f"💸 [出場觸發] {ticker} 產生 {status}！模擬平倉價: {current_price}")
        print(f"   {color} 單筆結算報酬率: {net_profit_pct:.3f}%")
        
        draw_chart(ticker, preloaded_df=ticker_df)

# ==========================================
# 啟動機台
# ==========================================
if __name__ == "__main__":
    try:
        run_live_simulation()
    except KeyboardInterrupt:
        print("\n🛑 接收到手動中斷指令，關閉實戰模擬引擎。")
        if trade_history:
            history_df = pd.DataFrame(trade_history)
            print("\n===================== 模擬交易明細表 =====================")
            print(history_df.to_string(index=False))
            print("==========================================================")
        else:
            print("本次執行無交易紀錄產生。")