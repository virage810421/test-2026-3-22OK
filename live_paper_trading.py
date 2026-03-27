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
portfolio = {}       # 現在裡面會存放「清單 (List)」，支援分批加碼
trade_history = []   
SCAN_INTERVAL = 300  
FEE_SLIPPAGE = 0.0025 

DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

watch_list = [
    "2330.TW", "2454.TW", "2317.TW", "2303.TW", "2308.TW",
    "2382.TW", "3231.TW", "6669.TW", "2357.TW", "3034.TW",
    "2603.TW", "2609.TW", "2615.TW",
    "2881.TW", "2882.TW", "2891.TW",
    "1519.TW", "1513.TW", "2618.TW", "2002.TW"
]

chip_cache = {}

# ==========================================
# 🔄 新增：與 SQL Server 進行開機同步
# ==========================================
def sync_portfolio_from_db():
    print("🔄 正在與 SQL Server 同步持倉狀態...")
    global portfolio
    portfolio = {ticker: [] for ticker in watch_list} # 初始化清單
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            df = pd.read_sql("SELECT * FROM active_positions", conn)
            for index, row in df.iterrows():
                ticker = row['Ticker SYMBOL']
                if ticker not in portfolio:
                    portfolio[ticker] = []
                
                # 將資料庫中的每一筆(批)單子加進來
                portfolio[ticker].append({
                    '進場價': float(row['進場價']),
                    '方向': row['方向'],
                    '進場時間': row['進場時間'].strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(row['進場時間']) else "未知",
                    '投入資金': float(row['投入資金']) if '投入資金' in row and pd.notnull(row['投入資金']) else 100000,
                    '進場分數': 5,      # 預設，因為資料庫目前沒存分數
                    '進場趨勢多頭': True  # 預設
                })
            
            total_batches = sum(len(batches) for batches in portfolio.values())
            print(f"✅ 同步完成！目前庫存共有 {total_batches} 批持倉。")
    except Exception as e:
        print(f"⚠️ 同步庫存失敗: {e}")

# ==========================================
# 🚀 盤中實戰模擬引擎
# ==========================================
def run_live_simulation():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🟢 啟動盤中實戰模擬引擎 (已掛載 SQL Server)...")
    
    # 🌟 啟動前先同步大腦與資料庫
    sync_portfolio_from_db()
    
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
                
            today_str = datetime.now().strftime("%Y-%m-%d")
            
            # 籌碼快取機制
            if ticker not in chip_cache or chip_cache[ticker].get('date') != today_str:
                ticker_df = add_chip_data(ticker_df, ticker)
                chip_cache[ticker] = {
                    'date': today_str,
                    'foreign': ticker_df['Foreign_Net'].copy() if 'Foreign_Net' in ticker_df.columns else None,
                    'trust': ticker_df['Trust_Net'].copy() if 'Trust_Net' in ticker_df.columns else None,
                    'dealers': ticker_df['Dealers_Net'].copy() if 'Dealers_Net' in ticker_df.columns else None
                }
            else:
                if chip_cache[ticker]['foreign'] is not None:
                    ticker_df = ticker_df.join(chip_cache[ticker]['foreign'], how='left')
                if chip_cache[ticker]['trust'] is not None:
                    ticker_df = ticker_df.join(chip_cache[ticker]['trust'], how='left')
                if chip_cache[ticker]['dealers'] is not None:
                    ticker_df = ticker_df.join(chip_cache[ticker]['dealers'], how='left')
                
                ticker_df['Foreign_Net'] = ticker_df.get('Foreign_Net', pd.Series(0, index=ticker_df.index)).ffill().fillna(0)
                ticker_df['Trust_Net'] = ticker_df.get('Trust_Net', pd.Series(0, index=ticker_df.index)).ffill().fillna(0)
                ticker_df['Dealers_Net'] = ticker_df.get('Dealers_Net', pd.Series(0, index=ticker_df.index)).ffill().fillna(0)

            result = inspect_stock(ticker, preloaded_df=ticker_df)
        
            if result and "計算後資料" in result:
                computed_df = result['計算後資料']
                if computed_df.empty or len(computed_df) < PARAMS['MA_LONG']: 
                    continue 
                
                status = result['今日系統燈號']
                current_price = result['最新收盤價']
                
                handle_paper_trade(ticker, current_price, status, computed_df, result)
                
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 掃描完成。進入冷卻等待 {SCAN_INTERVAL} 秒...")
        time.sleep(SCAN_INTERVAL)

# ==========================================
# 🎯 交易模組：支援分批加碼與平均成本計算
# ==========================================
def handle_paper_trade(ticker, current_price, status, ticker_df, result_dict):
    global portfolio
    if ticker not in portfolio:
        portfolio[ticker] = []
        
    positions = portfolio[ticker]
    has_position = len(positions) > 0
    
    win_rate = result_dict["系統勝率(%)"]
    total_prof = result_dict["累計報酬率(%)"]
    latest_row = ticker_df.iloc[-1] 
    
    BASE_CAPITAL = 100000 
    MAX_BATCHES = 3  # 🌟 最大持倉限制：同一檔股票最多加碼到 3 批
    
    # --- 狀況 A：進場 / 分批加碼觸發 ---
    if ("買訊" in status or "賣訊" in status):
        trade_dir = '做多(Long)' if "買" in status else '放空(Short)'
        
        # 檢查是否反向訊號（如果你滿手多單卻出現賣訊，那是出場，不是加碼空單）
        is_reverse_signal = has_position and positions[0]['方向'] != trade_dir
        
        # 🌟 加碼條件：不是反向訊號，且批次還沒滿
        if not is_reverse_signal and len(positions) < MAX_BATCHES:
            invest_amount = BASE_CAPITAL * (1.0 if "強" in status else 0.5)
            trend_is_bull = (latest_row['Close'] > latest_row['BBI']) and (latest_row['BBI'] > ticker_df.iloc[-2]['BBI'])
            entry_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 1. 寫入 SQL Server 
            try:
                with pyodbc.connect(DB_CONN_STR) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO active_positions ([Ticker SYMBOL], [方向], [進場時間], [進場價], [投入資金])
                        VALUES (?, ?, ?, ?, ?)
                    ''', (ticker, trade_dir, entry_time, current_price, invest_amount))
                    conn.commit()
                
                # 2. SQL 寫入成功後，更新本機記憶體
                positions.append({
                    '進場價': current_price, '方向': trade_dir, '投入資金': invest_amount,
                    '進場時間': entry_time,
                    '進場分數': int(latest_row['Buy_Score'] if "買" in status else latest_row['Sell_Score']),
                    '進場趨勢多頭': trend_is_bull
                })
                print(f"⚡ [進場] {ticker} 第 {len(positions)} 批 ({status}) | 佈局: ${invest_amount:,.0f} | 價格: {current_price}")
                draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)
            
            except Exception as e:
                print(f"⚠️ 資料庫寫入失敗 (進場): {e}")

    # --- 狀況 B：部位控管 (計算平均成本、全部平倉) ---
    if len(positions) > 0:
        is_long = positions[0]['方向'] == '做多(Long)'
        
        # 🌟 自動算平均成本
        total_invested = sum(p['投入資金'] for p in positions)
        avg_cost = sum(p['進場價'] * p['投入資金'] for p in positions) / total_invested
        
        # 用平均成本來計算現在的總損益
        raw_p = (current_price - avg_cost) / avg_cost if is_long else (avg_cost - current_price) / avg_cost
        net_p = (raw_p * 100) - (FEE_SLIPPAGE * 100 * 2)
        
        # 防線計算
        vol = (latest_row['BB_std'] * 1.5) / latest_row['Close']
        sl_line = max(PARAMS['SL_MIN_PCT'], min(vol, PARAMS['SL_MAX_PCT'])) * 100
        tp_line = (PARAMS['TP_TREND_PCT']*100) if (positions[0]['進場趨勢多頭'] and latest_row['ADX14'] > PARAMS['ADX_TREND_THRESHOLD']) else (PARAMS['TP_BASE_PCT']*100)
        if positions[0]['進場分數'] >= 8: tp_line = 999.0
            
        exit_msg = ""
        if net_p <= -sl_line: exit_msg = f"🛑 停損 (-{sl_line:.1f}%)"
        elif net_p >= tp_line: exit_msg = f"🎯 停利 (+{tp_line:.1f}%)"
        elif (is_long and "賣訊" in status) or (not is_long and "買訊" in status): exit_msg = f"🔄 反轉 ({status})"
            
        # 🌟 觸發出場：將所有批次一次全平倉
        if exit_msg:
            print(f"💸 [準備出場] {ticker} {exit_msg} | 平均成本: {avg_cost:.2f} | 總損益: {net_p:.3f}%")
            exit_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            try:
                with pyodbc.connect(DB_CONN_STR) as conn:
                    cursor = conn.cursor()
                    
                    # 1. 逐批結算，寫入歷史總帳
                    for batch in positions:
                        # 計算單批的真實損益
                        batch_raw_p = (current_price - batch['進場價']) / batch['進場價'] if is_long else (batch['進場價'] - current_price) / batch['進場價']
                        batch_net_p = (batch_raw_p * 100) - (FEE_SLIPPAGE * 100 * 2)
                        batch_net_amt = batch['投入資金'] * (batch_net_p / 100)
                        
                        cursor.execute('''
                            INSERT INTO trade_history ([Ticker SYMBOL], [方向], [進場時間], [出場時間], [進場價], [出場價], [報酬率(%)])
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (ticker, batch['方向'], batch['進場時間'], exit_time, batch['進場價'], current_price, round(batch_net_p, 3)))
                        
                        trade_history.append({
                            'Ticker SYMBOL': ticker, '方向': batch['方向'], '淨損益': round(batch_net_amt, 0), '報酬率(%)': f"{batch_net_p:.3f}", '原因': exit_msg
                        })
                    
                    # 2. 刪除資料庫中該檔股票的所有持倉紀錄
                    cursor.execute('DELETE FROM active_positions WHERE [Ticker SYMBOL] = ?', (ticker,))
                    conn.commit()
                
                # 3. 清空記憶體
                portfolio[ticker] = []
                print(f"✅ {ticker} 全部批次結算移轉完成！")
                draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)
                
            except pyodbc.Error as e:
                print(f"⚠️ 資料庫結帳失敗: {e}")

if __name__ == "__main__":
    try:
        run_live_simulation()
    except KeyboardInterrupt:
        print("\n🛑 結束引擎。")
        if trade_history:
            print("\n" + "="*20 + " 本地端模擬交易明細表 " + "="*20)
            print(pd.DataFrame(trade_history).to_string(index=False))