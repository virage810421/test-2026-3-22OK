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
CURRENT_EQUITY = 0.0
# ==========================================
# 💰 共享錢包管理模組 (負責扣款與領錢)
# ==========================================
def get_available_cash():
    """從 SQL 讀取目前剩餘多少現金"""
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT [可用現金] FROM account_info WHERE [帳戶名稱] = '我的實戰帳戶'")
            row = cursor.fetchone()
            return float(row[0]) if row else 0.0
    except Exception as e:
        print(f"⚠️ 讀取現金失敗: {e}")
        return 0.0

def update_account_cash(change_amount):
    """更新 SQL 中的現金餘額 (正數為領錢, 負數為扣款)"""
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE account_info SET [可用現金] = [可用現金] + ?, [最後更新時間] = ? WHERE [帳戶名稱] = '我的實戰帳戶'", 
                           (change_amount, datetime.now()))
            conn.commit()
    except Exception as e:
        print(f"⚠️ 更新帳戶現金失敗: {e}")

# ==========================================
# 🔄 新增：與 SQL Server 進行開機同步
# ==========================================
def sync_portfolio_from_db():
    print("🔄 正在與 SQL Server 同步持倉狀態...")
    global portfolio
    portfolio = {ticker: [] for ticker in watch_list} 
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            df = pd.read_sql("SELECT * FROM active_positions", conn)
            for index, row in df.iterrows():
                ticker = row['Ticker SYMBOL']
                if ticker not in portfolio:
                    portfolio[ticker] = []
                
                portfolio[ticker].append({
                    '進場價': float(row['進場價']),
                    '方向': row['方向'],
                    '進場時間': row['進場時間'].strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(row['進場時間']) else "未知",
                    # 🌟 移除 100000 預設值，直接讀取資料庫數值
                    '投入資金': float(row['投入資金']) if '投入資金' in row else 0.0,
                    '進場股數': 2000, # 假設你固定買 2 張，或從資料庫讀取
                    '進場分數': 3,
                    '進場趨勢多頭': True
                })
            print(f"✅ 同步完成！")
    except Exception as e:
        print(f"⚠️ 同步庫存失敗: {e}")

# ==========================================
# 🚀 盤中實戰模擬引擎
# ==========================================
def run_live_simulation():
    global CURRENT_EQUITY
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🟢 啟動盤中實戰模擬引擎 (已掛載 SQL Server)...")
    
   # 1. 先從資料庫同步持倉
    sync_portfolio_from_db()
    # 2. 🌟 就在這裡！讓 Python 變數與 SQL 銀行餘額同步
    CURRENT_EQUITY = get_available_cash()
    print(f"💰 帳戶開機完成：目前可用現金 ${CURRENT_EQUITY:,.0f}")
    print("-" * 50)

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
                # 🌟 [新增]：將策略成績單存入 SQL Server
                try:
                    with pyodbc.connect(DB_CONN_STR) as conn:
                        cursor = conn.cursor()
                        # 準備要存入的資料
                        log_time = datetime.now()
                        win_rate = float(result.get("系統勝率(%)", 0))
                        total_prof = float(result.get("累計報酬率(%)", 0))
                        status_tag = result.get("今日系統燈號", "無訊號")
                        
                        cursor.execute('''
                            INSERT INTO strategy_performance 
                            ([紀錄時間], [Ticker SYMBOL], [系統勝率(%)], [累計報酬率(%)], [今日燈號])
                            VALUES (?, ?, ?, ?, ?)
                        ''', (log_time, ticker, win_rate, total_prof, status_tag))
                        conn.commit()
                except Exception as e:
                    # 這裡用 pass 是為了不讓資料庫寫入的小錯誤中斷了寶貴的盤中掃描程序
                    print(f"⚠️ 績效成績單寫入失敗 ({ticker}): {e}")

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
    global portfolio, CURRENT_EQUITY # 🌟 確保能更新全域總資產變數
    if ticker not in portfolio:
        portfolio[ticker] = []
        
    positions = portfolio[ticker]
    has_position = len(positions) > 0
    
    win_rate = result_dict["系統勝率(%)"]
    total_prof = result_dict["累計報酬率(%)"]
    latest_row = ticker_df.iloc[-1] 
    
    MAX_BATCHES = 3 
    
    # --- 狀況 A：進場 / 分批加碼 ---
    if ("買訊" in status or "賣訊" in status):
        trade_dir = '做多(Long)' if "買" in status else '放空(Short)'
        is_reverse_signal = has_position and positions[0]['方向'] != trade_dir
        
        if not is_reverse_signal and len(positions) < MAX_BATCHES:
            TRADE_SHARES = 2000 
            fee_mult = (1 + (PARAMS['FEE_RATE'] * PARAMS['FEE_DISCOUNT']))
            total_buy_cost = current_price * TRADE_SHARES * fee_mult
            
            available_cash = get_available_cash()
            if available_cash >= total_buy_cost:
                entry_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                try:
                    with pyodbc.connect(DB_CONN_STR) as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO active_positions ([Ticker SYMBOL], [方向], [進場時間], [進場價], [投入資金])
                            VALUES (?, ?, ?, ?, ?)
                        ''', (ticker, trade_dir, entry_time, current_price, total_buy_cost))
                        conn.commit()
                    
                    update_account_cash(-total_buy_cost)
                    
                    positions.append({
                        '進場價': current_price, '方向': trade_dir, '投入資金': total_buy_cost,
                        '進場時間': entry_time, '進場股數': TRADE_SHARES,
                        '進場分數': int(latest_row['Buy_Score'] if "買" in status else latest_row['Sell_Score']),
                        '進場趨勢多頭': (latest_row['Close'] > latest_row['BBI'])
                    })
                    print(f"⚡ [扣款成功] {ticker} 買入 {TRADE_SHARES} 股 | 支出: ${total_buy_cost:,.0f}")
                except Exception as e:
                    print(f"⚠️ 進場失敗: {e}")
            else:
                print(f"❌ [餘額不足] {ticker} 無法進場")

    # --- 狀況 B：部位控管與平倉 ---
    if len(positions) > 0:
        is_long = positions[0]['方向'] == '做多(Long)'
        total_invested = sum(p['投入資金'] for p in positions)
        avg_cost = sum(p['進場價'] * p['投入資金'] for p in positions) / total_invested
        
        raw_p = (current_price - avg_cost) / avg_cost if is_long else (avg_cost - current_price) / avg_cost
        net_p = (raw_p * 100) - (FEE_SLIPPAGE * 100 * 2)
        
        vol = (latest_row['BB_std'] * 1.5) / latest_row['Close']
        sl_line = max(PARAMS['SL_MIN_PCT'], min(vol, PARAMS['SL_MAX_PCT'])) * 100
        tp_line = (PARAMS['TP_TREND_PCT']*100) if (positions[0]['進場趨勢多頭'] and latest_row['ADX14'] > PARAMS['ADX_TREND_THRESHOLD']) else (PARAMS['TP_BASE_PCT']*100)
        if positions[0]['進場分數'] >= 8: tp_line = 999.0
            
        exit_msg = ""
        if net_p <= -sl_line: exit_msg = f"🛑 停損 (-{sl_line:.1f}%)"
        elif net_p >= tp_line: exit_msg = f"🎯 停利 (+{tp_line:.1f}%)"
        elif (is_long and "賣訊" in status) or (not is_long and "買訊" in status): exit_msg = f"🔄 反轉 ({status})"
            
        if exit_msg:
            exit_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                with pyodbc.connect(DB_CONN_STR) as conn:
                    cursor = conn.cursor()
                    
                    # 🌟 獲取當前銀行餘額準備累加
                    current_bank_cash = get_available_cash()
                    total_cash_back = 0
                    
                    for batch in positions:
                        shares = batch.get('進場股數', 2000)
                        sell_net_mult = 1 - (PARAMS['FEE_RATE'] * PARAMS['FEE_DISCOUNT']) - PARAMS['TAX_RATE']
                        total_exit_proceeds = current_price * shares * sell_net_mult
                        
                        net_profit_cash = total_exit_proceeds - batch['投入資金']
                        profit_pct = (net_profit_cash / batch['投入資金']) * 100
                        
                        total_cash_back += total_exit_proceeds
                        current_bank_cash += total_exit_proceeds # 🌟 同步更新本金紀錄
                        
                        # 🌟 修正：加入 [結餘本金] 寫入
                        cursor.execute('''
                            INSERT INTO trade_history 
                            ([Ticker SYMBOL], [方向], [進場時間], [出場時間], [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金])
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (ticker, batch['方向'], batch['進場時間'], exit_time, batch['進場價'], 
                              current_price, round(profit_pct, 3), round(net_profit_cash, 0), round(current_bank_cash, 0)))

                    update_account_cash(total_cash_back)
                    cursor.execute('DELETE FROM active_positions WHERE [Ticker SYMBOL] = ?', (ticker,))
                    conn.commit()
                    
                    # 🌟 更新全域資產變數
                    CURRENT_EQUITY = current_bank_cash
                    print(f"💰 {ticker} 結案！領回: ${total_cash_back:,.0f} | 餘額: ${CURRENT_EQUITY:,.0f}")
                
                portfolio[ticker] = [] 
                draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)
                
            except Exception as e:
                print(f"⚠️ 出場結算失敗: {e}")

if __name__ == "__main__":
    try:
        run_live_simulation()
    except KeyboardInterrupt:
        print("\n🛑 結束引擎。")
        if trade_history:
            print("\n" + "="*20 + " 本地端模擬交易明細表 " + "="*20)
            print(pd.DataFrame(trade_history).to_string(index=False))