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
PEAK_EQUITY = 0.0      # 🌟 紀錄歷史最高淨值
IS_FROZEN = False      # 🌟 系統是否進入熔斷休眠
CURRENT_MDD_TIER = 1.0 # 🌟 新增：資金降載乘數 (1.0 = 滿血, 0.5 = 預算砍半)
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
                
                # 🌟 更新：同步讀取真實的股數與停利階段
                portfolio[ticker].append({
                    '進場價': float(row['進場價']),
                    '方向': row['方向'],
                    '進場時間': row['進場時間'].strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(row['進場時間']) else "未知",
                    '投入資金': float(row['投入資金']) if '投入資金' in row else 0.0,
                    
                    '停利階段': int(row.get('停利階段', 0)) if pd.notnull(row.get('停利階段', 0)) else 0,
                    '進場股數': int(row.get('進場股數', 2000)) if pd.notnull(row.get('進場股數', 2000)) else 2000,
                    
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
       # ==========================================
        # 🌟 [優先級 3：分級風控 MDD 熔斷監控機制]
        # ==========================================
        global PEAK_EQUITY, IS_FROZEN, CURRENT_MDD_TIER
        
        current_cash = get_available_cash()
        total_invested = sum(sum(p['投入資金'] for p in pos_list) for pos_list in portfolio.values())
        total_equity = current_cash + total_invested
        
        if total_equity > PEAK_EQUITY:
            PEAK_EQUITY = total_equity
            
        mdd = (PEAK_EQUITY - total_equity) / PEAK_EQUITY if PEAK_EQUITY > 0 else 0
        
        # 🛡️ 啟動三級防護網
        if mdd >= 0.20:
            if not IS_FROZEN:
                print(f"\n🚨🚨 [系統熔斷警報] 總資金回撤達 {mdd*100:.1f}%！超過極限 20%！🚨🚨")
                print(f"🛑 系統已強制切換為【只出不進】的絕對保護模式！")
                IS_FROZEN = True
            CURRENT_MDD_TIER = 0.0
            
        elif mdd >= 0.15:
            IS_FROZEN = False
            if CURRENT_MDD_TIER != 0.2:
                print(f"\n⚠️ [二級防護] 資金回撤達 {mdd*100:.1f}% ➔ 啟動重度防禦，新進部位強制縮水 80%！")
                CURRENT_MDD_TIER = 0.2
                
        elif mdd >= 0.10:
            IS_FROZEN = False
            if CURRENT_MDD_TIER != 0.5:
                print(f"\n🛡️ [一級防護] 資金回撤達 {mdd*100:.1f}% ➔ 啟動輕度防禦，新進部位強制砍半 (50%)！")
                CURRENT_MDD_TIER = 0.5
                
        else:
            if IS_FROZEN or CURRENT_MDD_TIER < 1.0:
                print(f"\n🟢 [警報解除] 資金回升，回撤縮小至 {mdd*100:.1f}%，恢復 100% 滿血資金動能。")
                IS_FROZEN = False
            CURRENT_MDD_TIER = 1.0
        
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
                # 🌟 1. 從字典中提取資料 (包含剛算好的期望值)
                win_rate = float(result.get("系統勝率(%)", 0))
                total_prof = float(result.get("累計報酬率(%)", 0))
                ev_score = float(result.get("期望值", 0)) # 🌟 [新增] 提取期望值
                status_tag = result.get("今日系統燈號", "無訊號")
                log_time = datetime.now()

                try:
                    with pyodbc.connect(DB_CONN_STR) as conn:
                        cursor = conn.cursor()
                        # 🌟 2. 修正 INSERT 指令，加入 [期望值] 欄位與對應的問號
                        cursor.execute('''
                            INSERT INTO strategy_performance 
                            ([Ticker SYMBOL], [紀錄時間], [系統勝率(%)], [累計報酬率(%)], [今日燈號], [期望值])
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (ticker, log_time, round(win_rate, 3), round(total_prof, 3), status_tag, round(ev_score, 3)))
                        conn.commit()
                except Exception as e:
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
    global portfolio, CURRENT_EQUITY, IS_FROZEN, CURRENT_MDD_TIER  # 🌟 補上 CURRENT_MDD_TIER 
    if ticker not in portfolio:
        portfolio[ticker] = []
        
    positions = portfolio[ticker]
    has_position = len(positions) > 0
    
    win_rate = result_dict.get("系統勝率(%)", 0)
    total_prof = result_dict.get("累計報酬率(%)", 0)
    latest_row = ticker_df.iloc[-1] 
    
    MAX_BATCHES = 3 
    today_str = datetime.now().strftime("%Y-%m-%d")
    bought_today = any(p.get('進場時間', '').startswith(today_str) for p in positions)
    
    # ==========================================
    # --- 狀況 A：進場 / 分批加碼 (選項一：市價追擊模式) ---
    # ==========================================
    if ("買訊" in status or "賣訊" in status):
        
        # 🌟 攔截機制：如果系統熔斷，無情拒絕任何新資金進場！
        if IS_FROZEN:
            print(f"❄️ {ticker} 出現 {status}，但系統熔斷保護中，拒絕進場！")
        else:
            trade_dir = '做多(Long)' if "買" in status else '放空(Short)'
            is_reverse_signal = has_position and positions[0]['方向'] != trade_dir
        
        # 加上冷卻機制：今天沒買過才能買
        if not is_reverse_signal and len(positions) < MAX_BATCHES and not bought_today:
            
            # ==========================================
            # 🌟 [優先級 2：EV 期望值動態資金分配 (Position Sizing)]
            # ==========================================
            ev_score = float(result_dict.get("期望值", 0))
            
            # 如果期望值為負，代表雖然有技術面訊號，但長期統計會賠錢，直接拒絕！
            if ev_score <= 0:
                print(f"❄️ {ticker} 期望值為負 (EV: {ev_score:.3f}%) ➔ 長期勝算過低，系統放棄進場！")
            
            else:
                # 1. 計算單筆交易標準預算 (利用 config.py 中的設定)
                base_budget = PARAMS['TOTAL_BUDGET'] / PARAMS['MAX_POSITIONS']
                
                # 2. 依據期望值 (EV) 決定投資權重
                if ev_score >= 2.0:
                    target_budget = base_budget * 1.5  # 極高勝算：動用 1.5 倍資金重壓
                    print(f"🚀 {ticker} 極高勝算 (EV: {ev_score:.2f}%) ➔ 啟動 1.5 倍資金重壓 (${target_budget:,.0f})")
                elif ev_score >= 1.0:
                    target_budget = base_budget * 1.0  # 標準勝算：動用標準資金
                    print(f"🔥 {ticker} 標準勝算 (EV: {ev_score:.2f}%) ➔ 啟動標準資金 (${target_budget:,.0f})")
                else:
                    target_budget = base_budget * 0.5  # 邊緣勝算：只動用 50% 資金試單
                    print(f"👀 {ticker} 邊緣勝算 (EV: {ev_score:.2f}%) ➔ 啟動半碼資金試單 (${target_budget:,.0f})")

                # 🌟 [重點新增] 套用大盤 MDD 防護網降載乘數
                if CURRENT_MDD_TIER < 1.0:
                    target_budget = target_budget * CURRENT_MDD_TIER
                    print(f"🛡️ [風控降載] 系統防禦狀態啟動，預算縮減為原計畫的 {CURRENT_MDD_TIER*100:.0f}% ➔ 最終核准: ${target_budget:,.0f}")
                    
                # 3. 計算能買多少股 (需預留手續費空間)
                fee_mult = (1 + (PARAMS['FEE_RATE'] * PARAMS['FEE_DISCOUNT']))
                raw_shares = int(target_budget / (current_price * fee_mult))
                
                # 4. 台股優化邏輯：買得起整張就買整張，買不起就買零股
                if raw_shares >= 1000:
                    TRADE_SHARES = int(raw_shares / 1000) * 1000 # 捨去零頭，買 1000 的倍數
                else:
                    TRADE_SHARES = max(1, raw_shares) # 高價股買零股，最少買 1 股
                    
                total_buy_cost = current_price * TRADE_SHARES * fee_mult
                print(f"⚙️ 系統精算：市價 {current_price} 元 ➔ 分配購買 {TRADE_SHARES} 股")

                # 5. 檢查可用現金是否足夠
                available_cash = get_available_cash()
                if available_cash >= total_buy_cost:
                    entry_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    try:
                        with pyodbc.connect(DB_CONN_STR) as conn:
                            cursor = conn.cursor()
                            # 寫入 [停利階段] 與 [進場股數]
                            cursor.execute('''
                                INSERT INTO active_positions ([Ticker SYMBOL], [方向], [進場時間], [進場價], [投入資金], [停利階段], [進場股數])
                                VALUES (?, ?, ?, ?, ?, 0, ?)
                            ''', (ticker, trade_dir, entry_time, round(current_price, 2), round(total_buy_cost, 0), TRADE_SHARES))
                            conn.commit()
                            
                        update_account_cash(-total_buy_cost)
                        
                        positions.append({
                            '進場價': current_price, '方向': trade_dir, '投入資金': total_buy_cost,
                            '進場時間': entry_time, 
                            '進場股數': TRADE_SHARES, 
                            '停利階段': 0,
                            '進場分數': int(latest_row.get('Buy_Score', 0) if "買" in status else latest_row.get('Sell_Score', 0)),
                            '進場趨勢多頭': (latest_row['Close'] > latest_row.get('BBI', 0))
                        })
                        print(f"⚡ [扣款成功] {ticker} 買入 {TRADE_SHARES} 股 | 支出: ${total_buy_cost:,.0f}")
                    except Exception as e:
                        print(f"⚠️ 進場失敗: {e}")
                else:
                    print(f"❌ [餘額不足] {ticker} 無法進場")

    # ==========================================
    # --- 狀況 B：部位控管與平倉 (含分批停利) ---
    # ==========================================
    if len(positions) > 0:
        is_long = positions[0]['方向'] == '做多(Long)'
        total_invested = sum(p['投入資金'] for p in positions)
        avg_cost = sum(p['進場價'] * p['投入資金'] for p in positions) / total_invested
        
        raw_p = (current_price - avg_cost) / avg_cost if is_long else (avg_cost - current_price) / avg_cost
        net_p = (raw_p * 100) - (FEE_SLIPPAGE * 100 * 2)
        
        vol = (latest_row['BB_std'] * 1.5) / latest_row['Close']
        sl_line = max(PARAMS['SL_MIN_PCT'], min(vol, PARAMS['SL_MAX_PCT'])) * 100
        # ✅ 確認目前持倉方向與趨勢方向「一致」時，才啟動大波段停利
        trend_is_with_me = (is_long and positions[0]['進場趨勢多頭']) or (not is_long and not positions[0]['進場趨勢多頭'])

        tp_line = (PARAMS['TP_TREND_PCT']*100) if (trend_is_with_me and latest_row['ADX14'] > PARAMS['ADX_TREND_THRESHOLD']) else (PARAMS['TP_BASE_PCT']*100)
        if positions[0]['進場分數'] >= 8: tp_line = 999.0
            
        # 🌟 [功能 2] 定義第一階段停利線 (原目標的一半)
        tp_stage_1 = tp_line * 0.5 
        current_tp_stage = positions[0].get('停利階段', 0)

        exit_msg = ""
        is_partial = False

        # ==========================================
        # 🌟 升級版：細膩化反轉出場邏輯與「讓利潤奔跑」機制
        # ==========================================
        if net_p <= -sl_line: 
            exit_msg = f"🛑 停損 (-{sl_line:.1f}%)"
        elif net_p >= tp_line: 
            exit_msg = f"🎯 最終停利 (+{tp_line:.1f}%)"
            
        # 🌟 [優先級 1 升級] 動態第一階段停利 (避免錯殺大波段)
        elif net_p >= tp_stage_1 and current_tp_stage == 0:
            # 檢查目前趨勢是否強勁 (ADX > 閥值，且方向正確)
            trend_is_with_me = (is_long and positions[0]['進場趨勢多頭']) or (not is_long and not positions[0]['進場趨勢多頭'])
            adx_is_strong = latest_row['ADX14'] > PARAMS['ADX_TREND_THRESHOLD']
            
            if trend_is_with_me and adx_is_strong:
                # 🌊 趨勢極強！不賣出任何股數，直接把階段標記為 1 (避開後續重複檢查)
                positions[0]['停利階段'] = 1
                try:
                    with pyodbc.connect(DB_CONN_STR) as conn:
                        conn.cursor().execute("UPDATE active_positions SET [停利階段] = 1 WHERE [Ticker SYMBOL] = ?", (ticker,))
                        conn.commit()
                except Exception as e:
                    pass
                print(f"🌊 {ticker} 獲利達標第一階段 (+{tp_stage_1:.1f}%)，但 ADX 顯示趨勢極強 ➔ 取消減碼，死抱全倉讓利潤奔跑！")
            else:
                # 🌤️ 趨勢普通或盤整：乖乖執行減碼 50% 入袋為安
                exit_msg = f"💰 達標第一階段 (+{tp_stage_1:.1f}%) ➔ 趨勢偏弱，減碼 50% 入袋為安"
                is_partial = True
                
        # 👇 反轉訊號的差異化處理 (維持不變)
        elif (is_long and "賣訊" in status) or (not is_long and "買訊" in status):
            if "弱" in status and current_tp_stage == 0:
                exit_msg = f"🔄 弱勢反轉 ({status}) ➔ 先減碼 50% 觀察"
                is_partial = True
            else:
                exit_msg = f"🔄 強勢反轉 ({status}) ➔ 全數結案撤退"
                is_partial = False
            
        if exit_msg:
            exit_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                with pyodbc.connect(DB_CONN_STR) as conn:
                    cursor = conn.cursor()
                    current_bank_cash = get_available_cash()
                    total_cash_back = 0
                    
                    for batch in positions:
                        # 讀取當初買了多少股
                        shares_to_sell = batch.get('進場股數', 2000)
                        invested_portion = batch['投入資金']
                        
                        # 🌟 採用 GPT 防護機制：避免除以 2 變成 0 股
                        if is_partial:
                            shares_to_sell = max(1, int(shares_to_sell / 2))
                            invested_portion = invested_portion / 2

                        exit_fee_rate = PARAMS['FEE_RATE'] * PARAMS['FEE_DISCOUNT']
                        
                        # 🌟 區分多空現金流算法
                        if is_long:
                            # 多單出場：扣手續費與交易稅
                            sell_net_mult = 1 - exit_fee_rate - PARAMS['TAX_RATE']
                            total_exit_proceeds = current_price * shares_to_sell * sell_net_mult
                            net_profit_cash = total_exit_proceeds - invested_portion
                            
                            # 這筆單實際拿回金庫的錢
                            cash_returned_this_batch = total_exit_proceeds 
                        else:
                            # 空單出場：只扣手續費
                            buy_back_cost = current_price * shares_to_sell * (1 + exit_fee_rate)
                            net_profit_cash = invested_portion - buy_back_cost
                            
                            # 這筆單實際拿回金庫的錢 = 保證金 + 淨利
                            cash_returned_this_batch = invested_portion + net_profit_cash
                        
                        # 🌟 統一計算報酬率與更新金庫
                        profit_pct = (net_profit_cash / invested_portion) * 100
                        total_cash_back += cash_returned_this_batch
                        current_bank_cash += cash_returned_this_batch 
                        
                        # 寫入歷史明細表
                        cursor.execute('''
                            INSERT INTO trade_history 
                            ([Ticker SYMBOL], [方向], [進場時間], [出場時間], [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金])
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (ticker, batch['方向'], batch['進場時間'], exit_time, 
                              round(batch['進場價'], 2), round(current_price, 2), 
                              round(profit_pct, 3), round(net_profit_cash, 0), round(current_bank_cash, 0)))

                        if is_partial:
                            batch['進場股數'] -= shares_to_sell
                            batch['投入資金'] -= invested_portion
                            batch['停利階段'] = 1

                    update_account_cash(total_cash_back)
                    CURRENT_EQUITY = current_bank_cash

                    if is_partial:
                        cursor.execute('''
                            UPDATE active_positions 
                            SET [投入資金] = [投入資金] / 2, [進場股數] = [進場股數] / 2, [停利階段] = 1 
                            WHERE [Ticker SYMBOL] = ?
                        ''', (ticker,))
                        print(f"[{exit_time}] {exit_msg} | {ticker} 領回: ${total_cash_back:,.0f} | 剩餘部位留倉")
                    else:
                        cursor.execute('DELETE FROM active_positions WHERE [Ticker SYMBOL] = ?', (ticker,))
                        portfolio[ticker] = [] 
                        print(f"[{exit_time}] {exit_msg} | {ticker} 全數結案！領回: ${total_cash_back:,.0f} | 餘額: ${CURRENT_EQUITY:,.0f}")
                        
                        draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof, expected_value=result_dict.get("期望值", 0))

                    conn.commit()
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