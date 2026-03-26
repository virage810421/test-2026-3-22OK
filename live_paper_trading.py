import time
import pandas as pd
import pyodbc  
from datetime import datetime
import yfinance as yf
from advanced_chart import draw_chart
from screening import inspect_stock, add_chip_data
from config import PARAMS

# ==========================================
# 💼 虛擬帳戶、機台與資料庫設定
# ==========================================
portfolio = {}       
trade_history = []   
SCAN_INTERVAL = 300  
FEE_SLIPPAGE = 0.0025 

# 🔌 資料庫連線字串 (已設定為你的專屬伺服器)
DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  # 👈 你的專屬 SQL Server 實體名稱
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

watch_list = [
    # 權值與趨勢
    "2330.TW", "2454.TW", "2317.TW", "2303.TW", "2308.TW",
    # AI 伺服器
    "2382.TW", "3231.TW", "6669.TW", "2357.TW", "3034.TW",
    # 航運
    "2603.TW", "2609.TW", "2615.TW",
    # 金融
    "2881.TW", "2882.TW", "2891.TW",
    # 重電與傳產
    "1519.TW", "1513.TW", "2618.TW", "2002.TW"
]

def run_live_simulation():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🟢 啟動盤中實戰模擬引擎 (已掛載 SQL Server)...")
    
    while True:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")

        print(f"\n[{current_time}] 📡 啟動定時海選雷達，掃描 {len(watch_list)} 檔標的...")
        
        try:
            batch_data = yf.download(watch_list, period="2y", progress=False)
        except Exception as e:
            print(f"⚠️ 網路連線失敗: {e}"); time.sleep(60); continue

        for ticker in watch_list:
            time.sleep(1) 
            
            ticker_df = batch_data.xs(ticker, axis=1, level=1).copy() if isinstance(batch_data.columns, pd.MultiIndex) else batch_data.copy()
            ticker_df.dropna(how='all', inplace=True)
            if ticker_df.empty: continue
                
            ticker_df = add_chip_data(ticker_df, ticker)
            result = inspect_stock(ticker, preloaded_df=ticker_df)
        
            if result and "計算後資料" in result:
                computed_df = result['計算後資料']
                
                if computed_df.empty or len(computed_df) < PARAMS['MA_LONG']: 
                    continue 
                
                status = result['今日系統燈號']
                current_price = result['最新收盤價']
                
                handle_paper_trade(ticker, current_price, status, computed_df, result)
            else:
                continue
                
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 掃描完成。進入冷卻等待 {SCAN_INTERVAL} 秒...")
        time.sleep(SCAN_INTERVAL)

def handle_paper_trade(ticker, current_price, status, ticker_df, result_dict):
    """處理模擬買賣、同步大腦防線，並自動寫入 SQL Server"""
    has_position = ticker in portfolio
    win_rate = result_dict["系統勝率(%)"]
    total_prof = result_dict["累計報酬率(%)"]
    
    latest_row = ticker_df.iloc[-1] 
    BASE_CAPITAL = 100000 
    
    # --- 狀況 A：進場觸發 (寫入 active_positions) ---
    if ("買訊" in status or "賣訊" in status) and not has_position:
        trade_dir = '做多(Long)' if "買" in status else '放空(Short)'
        invest_amount = BASE_CAPITAL * (1.0 if "強" in status else 0.5)
        trend_is_bull = (latest_row['Close'] > latest_row['BBI']) and (latest_row['BBI'] > ticker_df.iloc[-2]['BBI'])
        entry_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 1. 更新本機記憶體
        portfolio[ticker] = {
            '進場價': current_price, '方向': trade_dir, '投入資金': invest_amount,
            '進場時間': entry_time,
            '進場分數': int(latest_row['Buy_Score'] if "買" in status else latest_row['Sell_Score']),
            '進場趨勢多頭': trend_is_bull
        }
        print(f"⚡ [進場] {ticker} ({status}) | 佈局: ${invest_amount:,.0f} | 價格: {current_price}")
        
        # 2. 同步寫入 SQL Server (目前持倉工作區)
        try:
            with pyodbc.connect(DB_CONN_STR) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO active_positions ([Ticker SYMBOL], [方向], [進場時間], [進場價])
                    VALUES (?, ?, ?, ?)
                ''', (ticker, trade_dir, entry_time, current_price))
                conn.commit()
        except Exception as e:
            print(f"⚠️ 資料庫寫入失敗 (進場): {e}")

        # 呼叫儀表板畫圖
        draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)

    # --- 狀況 B：部位控管 (寫入 trade_history 並清理 active_positions) ---
    elif has_position:
        d = portfolio[ticker]
        is_long = d['方向'] == '做多(Long)'
        
        raw_p = (current_price - d['進場價']) / d['進場價'] if is_long else (d['進場價'] - current_price) / d['進場價']
        net_p = (raw_p * 100) - (FEE_SLIPPAGE * 100 * 2)
        
        vol = (latest_row['BB_std'] * 1.5) / latest_row['Close']
        sl_line = max(PARAMS['SL_MIN_PCT'], min(vol, PARAMS['SL_MAX_PCT'])) * 100
        tp_line = (PARAMS['TP_TREND_PCT']*100) if (d['進場趨勢多頭'] and latest_row['ADX14'] > PARAMS['ADX_TREND_THRESHOLD']) else (PARAMS['TP_BASE_PCT']*100)
        if d['進場分數'] >= 8: tp_line = 999.0
            
        exit_msg = ""
        if net_p <= -sl_line: exit_msg = f"🛑 停損 (-{sl_line:.1f}%)"
        elif net_p >= tp_line: exit_msg = f"🎯 停利 (+{tp_line:.1f}%)"
        elif (is_long and "賣訊" in status) or (not is_long and "買訊" in status): exit_msg = f"🔄 反轉 ({status})"
            
        if exit_msg:
            d = portfolio.pop(ticker)
            net_amt = d['投入資金'] * (net_p / 100)
            exit_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 1. 更新本機記憶體
            trade_history.append({
                'Ticker SYMBOL': ticker, '方向': d['方向'], '淨損益': round(net_amt, 0), '報酬率(%)': f"{net_p:.3f}", '原因': exit_msg
            })
            print(f"💸 [出場] {ticker} {exit_msg} | 結算: ${net_amt:,.0f} ({net_p:.3f}%)")
            
            # 2. 同步更新 SQL Server (結帳移轉)
            try:
                with pyodbc.connect(DB_CONN_STR) as conn:
                    cursor = conn.cursor()
                    # A. 寫入歷史總帳 (嚴格遵守 3 位小數)
                    cursor.execute('''
                        INSERT INTO trade_history ([Ticker SYMBOL], [方向], [進場時間], [出場時間], [進場價], [出場價], [報酬率(%)])
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (ticker, d['方向'], d['進場時間'], exit_time, d['進場價'], current_price, round(net_p, 3)))
                    
                    # B. 刪除目前持倉紀錄
                    cursor.execute('''
                        DELETE FROM active_positions WHERE [Ticker SYMBOL] = ?
                    ''', (ticker,))
                    conn.commit()
            except Exception as e:
                print(f"⚠️ 資料庫更新失敗 (出場結算): {e}")
            
            # 呼叫儀表板畫圖
            draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)

if __name__ == "__main__":
    try:
        run_live_simulation()
    except KeyboardInterrupt:
        print("\n🛑 結束引擎。")
        if trade_history:
            print("\n" + "="*20 + " 本地端模擬交易明細表 " + "="*20)
            print(pd.DataFrame(trade_history).to_string(index=False))