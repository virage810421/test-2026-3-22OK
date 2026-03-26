import time
import pandas as pd
from datetime import datetime
import yfinance as yf

# 確保這兩個檔案跟這個腳本放在同一個資料夾
from advanced_chart import draw_chart
from screening import inspect_stock, add_chip_data

# ==========================================
# 💼 虛擬帳戶與機台設定
# ==========================================
portfolio = {}       # 紀錄持倉狀態，格式: {'Ticker SYMBOL': {'進場價': 100, '方向': '做多(Long)', '勝率': 0, '報酬': 0}}
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
        
        # 🛡️ 修正 BUG 1：拉長下載期間至半年，確保 MA60 算得出來
        try:
            batch_data = yf.download(watch_list, period="6mo", progress=False)
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
                
            # 加入籌碼資料 (注意：若 API 被鎖定，此處會回傳 0)
            ticker_df = add_chip_data(ticker_df, ticker)
            
            # 2. 進行策略檢驗
            result = inspect_stock(ticker, preloaded_df=ticker_df)
            if not result:
                continue
                
            status = result['今日系統燈號']
            current_price = result['最新收盤價']
            
            # 3. 執行模擬交易決策，並把 result 整包傳進去，方便取勝率
            handle_paper_trade(ticker, current_price, status, ticker_df, result)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 掃描完成。進入冷卻等待 {SCAN_INTERVAL} 秒...")
        time.sleep(SCAN_INTERVAL)

def handle_paper_trade(ticker, current_price, status, ticker_df, result_dict):
    """處理單一檔股票的模擬買賣邏輯"""
    has_position = ticker in portfolio
    win_rate = result_dict["系統勝率(%)"]
    total_prof = result_dict["累計報酬率(%)"]
    
    # --- 狀況 A：偵測到買訊，且目前空手 ---
    if "買訊" in status and not has_position:
        print(f"⚡ [進場觸發] {ticker} 產生 {status}！模擬【做多】買進價: {current_price}")
        portfolio[ticker] = {
            '進場價': current_price,
            '進場時間': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            '方向': '做多(Long)',
            '勝率': win_rate,
            '報酬': total_prof
        }
        # 🛡️ 修正 BUG 2：補上繪圖所需的參數
        draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)

    # --- 狀況 B：偵測到賣訊，且目前空手 (執行放空) ---
    elif "賣訊" in status and not has_position:
        print(f"⚡ [進場觸發] {ticker} 產生 {status}！模擬【放空】賣出價: {current_price}")
        portfolio[ticker] = {
            '進場價': current_price,
            '進場時間': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            '方向': '放空(Short)',
            '勝率': win_rate,
            '報酬': total_prof
        }
        draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)

    # --- 狀況 C：偵測到反向訊號，且目前持有部位 (執行平倉) ---
    elif has_position:
        entry_data = portfolio[ticker]
        entry_price = entry_data['進場價']
        is_long = entry_data['方向'] == '做多(Long)'
        
        # 多單遇到賣訊，或空單遇到買訊，就平倉
        if (is_long and "賣訊" in status) or (not is_long and "買訊" in status):
            entry_data = portfolio.pop(ticker) # 取出並移除部位
            
            # 計算報酬率 (含摩擦成本)
            if is_long:
                raw_profit_pct = (current_price - entry_price) / entry_price * 100
            else:
                raw_profit_pct = (entry_price - current_price) / entry_price * 100
                
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
            
            color = "🔴" if net_profit_pct > 0 else "🟢" 
            print(f"💸 [出場觸發] {ticker} 產生反向 {status}！模擬平倉價: {current_price}")
            print(f"   {color} 單筆結算報酬率: {net_profit_pct:.3f}%")
            
            draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)

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