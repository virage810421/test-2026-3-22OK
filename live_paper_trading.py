import time
import pandas as pd
import pyodbc  
from datetime import datetime
import yfinance as yf
from advanced_chart import draw_chart
from screening import inspect_stock, add_chip_data, apply_slippage, calculate_pnl
from config import PARAMS, WATCH_LIST
from performance import get_strategy_ev  # ✨ Layer 2 匯入績效大腦
from param_storage import load_all_params
from sector_classifier import get_stock_sector
from kline_cache import get_smart_klines
# 啟動時自動讀取 AI 最新優化的 JSON 成果
AUTO_PARAMS = load_all_params()

# 修改查表邏輯，優先使用剛出爐的 AI 參數 (串接最新自動感知器)
def get_params_for_stock(ticker):
    # 🌟 直接呼叫我們寫好的 sector_classifier，全自動判斷產業！
    sector = get_stock_sector(ticker) 
    
    if sector in AUTO_PARAMS:
        return AUTO_PARAMS[sector]
        
    # 如果還沒有自動訓練出該產業的參數，直接用 config 裡的 PARAMS 預設值兜底
    return PARAMS
# ==========================================
# 💼 虛擬帳戶、機台與資料庫設定
# ==========================================
portfolio = {}       # 現在裡面會存放「清單 (List)」，支援分批加碼
trade_history = []   

DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)


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
    portfolio = {ticker: [] for ticker in WATCH_LIST} 
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            df = pd.read_sql("SELECT * FROM active_positions", conn)
            for _, row in df.iterrows():
                ticker = row['Ticker SYMBOL']
                if ticker not in portfolio:
                    portfolio[ticker] = []
                
                # 🌟 更新：同步讀取真實的股數、停利階段，以及「機構級歸因欄位」
                portfolio[ticker].append({
                    '進場價': float(row['進場價']),
                    '方向': row['方向'],
                    '進場時間': row['進場時間'].strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(row['進場時間']) else "未知",
                    '投入資金': float(row['投入資金']) if '投入資金' in row else 0.0,
                    '停利階段': int(row.get('停利階段', 0)) if pd.notnull(row.get('停利階段', 0)) else 0,
                    '進場股數': int(row.get('進場股數', 2000)) if pd.notnull(row.get('進場股數', 2000)) else 2000,
                    
                    # ✨ 新增：歸因記憶恢復
                    '市場狀態': row.get('市場狀態', '未知'),
                    '進場陣型': row.get('進場陣型', '傳統訊號'),
                    '期望值': float(row.get('期望值', 0.0)) if pd.notnull(row.get('期望值')) else 0.0,
                    '預期停損(%)': float(row.get('預期停損(%)', 0.0)) if pd.notnull(row.get('預期停損(%)')) else 0.0,
                    '預期停利(%)': float(row.get('預期停利(%)', 0.0)) if pd.notnull(row.get('預期停利(%)')) else 0.0,
                    '風報比(RR)': float(row.get('風報比(RR)', 0.0)) if pd.notnull(row.get('風報比(RR)')) else 0.0,
                    '風險金額': float(row.get('風險金額', 0.0)) if pd.notnull(row.get('風險金額')) else 0.0,
                    
                    '進場趨勢多頭': True # 保留相容性
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
        
        # 🛡️ 啟動三級防護網 (讀取 PARAMS 參數)
        if mdd >= PARAMS['MDD_LIMIT']:
            if not IS_FROZEN:
                print(f"\n🚨🚨 [系統熔斷警報] 總資金回撤達 {mdd*100:.1f}%！超過極限 {PARAMS['MDD_LIMIT']*100}%！🚨🚨")
                print(f"🛑 系統已強制切換為【只出不進】的絕對保護模式！")
                IS_FROZEN = True
            CURRENT_MDD_TIER = 0.0
            
        elif mdd >= PARAMS['MDD_LEVEL_2']:
            IS_FROZEN = False
            if CURRENT_MDD_TIER != PARAMS['MDD_MULTIPLIER_2']:
                print(f"\n⚠️ [二級防護] 資金回撤達 {mdd*100:.1f}% ➔ 啟動重度防禦，新進部位強制縮水 {PARAMS['MDD_MULTIPLIER_2']*100:.0f}%！")
                CURRENT_MDD_TIER = PARAMS['MDD_MULTIPLIER_2']
                
        elif mdd >= PARAMS['MDD_LEVEL_1']:
            IS_FROZEN = False
            if CURRENT_MDD_TIER != PARAMS['MDD_MULTIPLIER_1']:
                print(f"\n🛡️ [一級防護] 資金回撤達 {mdd*100:.1f}% ➔ 啟動輕度防禦，新進部位降載至 {PARAMS['MDD_MULTIPLIER_1']*100:.0f}%！")
                CURRENT_MDD_TIER = PARAMS['MDD_MULTIPLIER_1']
                
        else:
            if IS_FROZEN or CURRENT_MDD_TIER < 1.0:
                print(f"\n🟢 [警報解除] 資金回升，回撤縮小至 {mdd*100:.1f}%，恢復 100% 滿血資金動能。")
                IS_FROZEN = False
            CURRENT_MDD_TIER = 1.0
        
        print(f"\n[{current_time}] 📡 啟動定時海選雷達，掃描 {len(WATCH_LIST)} 檔標的...")

        try:
            # 🌟 實戰增量更新：只會抓取「今天」的最新跳動資料，其餘全用本地存檔！
            ticker_dfs = get_smart_klines(WATCH_LIST)
        except Exception as e:
            print(f"⚠️ K線智慧調閱失敗: {e}"); time.sleep(60); continue

        for ticker in WATCH_LIST:
            time.sleep(1)

            if ticker not in ticker_dfs: continue
            ticker_df = ticker_dfs[ticker].copy() # 直接拿拼好的資料

            # ✨ 疫苗 1：嚴格剃除沒有收盤價的幽靈 K 線，防止 NaN 病毒感染
            ticker_df.dropna(subset=['Close'], inplace=True)
            ticker_df.ffill(inplace=True) 
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

            # 🌟 自動掛載：優先使用昨晚 AI 剛訓練好的產業參數，沒有的話再用預設值
            current_stock_params = get_params_for_stock(ticker)
            
            # 將專屬參數傳遞給大腦 (保留原本的 preloaded_df，並加入 p=current_stock_params)
            result = inspect_stock(ticker, preloaded_df=ticker_df, p=current_stock_params)
    
            if result and "計算後資料" in result:
                # 🌟 1. 從字典中提取資料 (包含剛算好的期望值)
                win_rate = float(result.get("系統勝率(%)", 0))
                total_prof = float(result.get("累計報酬率(%)", 0))
                ev_score = float(result.get("期望值", 0))
                

                # 🛡️ 終極防護：阻斷 NaN 病毒寫入 SQL 導致崩潰
                if pd.isna(win_rate): win_rate = 0.0
                if pd.isna(total_prof): total_prof = 0.0
                if pd.isna(ev_score): ev_score = 0.0
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
                
                # ✨ 新增：X 光透視診斷 (讓系統不再安靜)
                latest_row = computed_df.iloc[-1]
                regime = latest_row.get('Regime', '未知')
                golden_tag = latest_row.get('Golden_Type', '無')
                
                if "觀望中" in status:
                    # 第一關：根本沒有觸發陣型與扣板機
                    print(f"  🔍 {ticker} | 環境: {regime.ljust(4)} | 陣型: 未成型 | 結論: 不符合進場結構")
                else:
                    # 第二關：有陣型，交給 handle_paper_trade 去做 EV 與 RR 檢查
                    # (它會在裡面印出因為 EV 或 RR 拒絕的訊息)
                    print(f"  🎯 {ticker} | 環境: {regime.ljust(4)} | 陣型: {golden_tag.ljust(8)} | 結論: 觸發初審，進入精算程序...")
                
                # 🌟 修復：把該股票專屬的產業參數，一併傳給負責下單計算的手
                handle_paper_trade(ticker, current_price, status, computed_df, result, current_stock_params)
                
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 掃描完成。進入冷卻等待 {PARAMS['SCAN_INTERVAL']} 秒...")
        time.sleep(PARAMS['SCAN_INTERVAL'])

# ==========================================
# 🎯 交易模組：支援分批加碼與平均成本計算
# ==========================================

def handle_paper_trade(ticker, current_price, status, ticker_df, result_dict, sys_params):
    global portfolio, CURRENT_EQUITY, IS_FROZEN, CURRENT_MDD_TIER
    if ticker not in portfolio:
        portfolio[ticker] = []
        
    positions = portfolio[ticker]
    has_position = len(positions) > 0
    
    win_rate = result_dict.get("系統勝率(%)", 0)
    total_prof = result_dict.get("累計報酬率(%)", 0)
    latest_row = ticker_df.iloc[-1] 
    
    # ✨ 終極微結構防護：計算昨日收盤價，判定是否碰觸 10% 漲跌停
    prev_close = ticker_df['Close'].iloc[-2] if len(ticker_df) >= 2 else current_price
    is_limit_up = current_price >= prev_close * 1.095  # 漲幅達 9.5% 以上視為漲停邊緣
    is_limit_down = current_price <= prev_close * 0.905 # 跌幅達 9.5% 以上視為跌停邊緣 
    
    MAX_BATCHES = sys_params['MAX_BATCHES']
    today_str = datetime.now().strftime("%Y-%m-%d")
    bought_today = any(p.get('進場時間', '').startswith(today_str) for p in positions)
    
   
    # ==========================================
    # --- 狀況 A：進場 / 分批加碼 (選項一：市價追擊模式) ---
    # ==========================================
    is_long_signal = "LONG" in status or "買訊" in status
    is_short_signal = "SHORT" in status or "賣訊" in status

    if is_long_signal or is_short_signal:
        # ✨ [新增] 微結構防護：漲停買不到，跌停空不到
        if is_long_signal and is_limit_up:
            print(f"🧱 {ticker} 亮出作多陣型，但目前【漲停鎖死】，市場無賣單，物理拒絕進場！")
            return
        if is_short_signal and is_limit_down:
            print(f"🧱 {ticker} 亮出放空陣型，但目前【跌停鎖死】，禁止借券放空，物理拒絕進場！")
            return

        # 🌟 多空進場實體攔截
        if is_long_signal and not PARAMS.get('ALLOW_LONG', True):
            print(f"🚫 {ticker} 產生多方訊號，但系統已關閉【做多開關】，放棄進場。")
            return 
            
        if is_short_signal and not PARAMS.get('ALLOW_SHORT', True):
            print(f"🚫 {ticker} 產生空方訊號，但系統已關閉【放空開關】，放棄進場。")
            return
        
        # 🌟 攔截機制：如果系統熔斷，無情拒絕任何新資金進場！
        if IS_FROZEN:
            print(f"❄️ {ticker} 出現 {status}，但系統熔斷保護中，拒絕進場！")
        else:
            trade_dir = '做多(Long)' if is_long_signal else '放空(Short)'
            is_reverse_signal = has_position and positions[0]['方向'] != trade_dir
        
        # 加上冷卻機制：今天沒買過才能買
        if not is_reverse_signal and len(positions) < MAX_BATCHES and not bought_today:
            
            # ==========================================
            # 🧠 終極 AI 精算師：Risk Parity + 期望值 (EV) + MDD 降載
            # ==========================================
            setup_tag = status.split(' ')[1] if len(status.split(' ')) > 1 else "傳統訊號"
            current_regime = latest_row.get('Regime', '未知')
            
            # ✨ Layer 2 升級：直接向 performance.py 查詢該陣型的真實期望值！(取代舊版假數據)
            ev_score = get_strategy_ev(setup_tag, current_regime)
            
            # ✨ 1. 預先試算風報比 (RR 濾網)
            setup_tag = status.split(' ')[1] if len(status.split(' ')) > 1 else "傳統訊號"
            current_regime = latest_row.get('Regime', '未知')
            
            volatility_pct = (latest_row['BB_std'] * 1.5) / current_price
            MAX_BATCHES = sys_params['MAX_BATCHES']  # 🌟 改用 sys_params
    # ... 中間省略 ...
            if pd.isna(volatility_pct): volatility_pct = sys_params['SL_MAX_PCT'] # 🌟 改
            entry_sl_pct = max(sys_params['SL_MIN_PCT'], min(volatility_pct, sys_params['SL_MAX_PCT'])) # 🌟 改
            
            trend_is_bull = (latest_row['Close'] > latest_row.get('BBI', 0))
            trend_is_with_me = (trade_dir == '做多(Long)' and trend_is_bull) or (trade_dir == '放空(Short)' and not trend_is_bull)
            adx_is_strong = latest_row.get('ADX14', 0) > sys_params.get('ADX_TREND_THRESHOLD', 20) # 🌟 改
            entry_tp_pct = sys_params['TP_TREND_PCT'] if (trend_is_with_me and adx_is_strong) else sys_params['TP_BASE_PCT'] # 🌟 改
            
            rr_ratio = entry_tp_pct / entry_sl_pct if entry_sl_pct > 0 else 0

            # 🚨 雙重品質把關：EV 必須大於 0，且風報比(RR) 必須及格
            if ev_score <= 0:
                print(f"❄️ {ticker} 期望值為負 (EV: {ev_score:.3f}%) ➔ 長期勝算過低，系統放棄進場！")
            elif rr_ratio < sys_params.get('MIN_RR_RATIO', 1.5): # 🌟 改
                print(f"⚖️ {ticker} 訊號觸發，但風報比過低 (RR: {rr_ratio:.2f} < 1.5) ➔ 潛在獲利不值得冒險，放棄進場！")
            else:
                # 2. 決定基礎「風險承受額度」(Base Risk) 
                base_risk = CURRENT_EQUITY * 0.01 
                
                # ✨ 3. 動態信心權重 (倉位管理：讓分數降級為資金控管工具)
                if "TREND" in setup_tag or "點火" in setup_tag or "倒貨" in setup_tag:
                    conviction_mult = 1.2
                    print(f"🚀 {ticker} 高度信心陣型 ({setup_tag}) ➔ 風險額度放大至 1.2 倍")
                elif "REVERSAL" in setup_tag or "抄底" in setup_tag or "摸頭" in setup_tag:
                    conviction_mult = 0.7
                    print(f"👀 {ticker} 逆勢陣型 ({setup_tag}) ➔ 風險額度降載至 0.7 倍試單")
                else:
                    conviction_mult = 1.0
                    
                target_risk = base_risk * conviction_mult
                
                # 套用大盤 MDD 防護網降載乘數
                if CURRENT_MDD_TIER < 1.0:
                    target_risk = target_risk * CURRENT_MDD_TIER
                    print(f"🛡️ [大盤防禦] 系統遭遇回撤，風險承受度強制縮減為 {CURRENT_MDD_TIER*100:.0f}%")
                
                # 反推能買多少股，並加上流動性過濾 (不得超過 20 日均量的 5%)
                raw_shares = target_risk / (current_price * entry_sl_pct)
                max_liquidity_shares = (latest_row.get('Vol_MA20', 1000) * 1000) * 0.05
                raw_shares = min(raw_shares, max_liquidity_shares)
                
                if raw_shares >= 1000:
                    TRADE_SHARES = int(raw_shares // 1000) * 1000
                else:
                    TRADE_SHARES = max(1, int(raw_shares))
                    
                fee_mult = (1 + (PARAMS['FEE_RATE'] * PARAMS['FEE_DISCOUNT']))
                total_buy_cost = current_price * TRADE_SHARES * fee_mult
                
                max_affordable_cost = CURRENT_EQUITY * 0.33 
                if total_buy_cost > max_affordable_cost:
                    TRADE_SHARES = int(max_affordable_cost / (current_price * fee_mult))
                    total_buy_cost = current_price * TRADE_SHARES * fee_mult
                
                print(f"⚙️ 結構式風控結案：RR {rr_ratio:.2f} | 停損距 {entry_sl_pct*100:.1f}% | 實質風險 ${target_risk:,.0f} ➔ 核准購買 {TRADE_SHARES} 股")

                # 5. 檢查可用現金並寫入 SQL
                available_cash = get_available_cash()
                if available_cash >= total_buy_cost:
                    entry_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    try:
                        with pyodbc.connect(DB_CONN_STR) as conn:
                            cursor = conn.cursor()
                            # ✨ 擴充 SQL 寫入：包含 7 個歸因欄位
                            cursor.execute('''
                                INSERT INTO active_positions (
                                    [Ticker SYMBOL], [方向], [進場時間], [進場價], [投入資金], [停利階段], [進場股數],
                                    [市場狀態], [進場陣型], [期望值], [預期停損(%)], [預期停利(%)], [風報比(RR)], [風險金額]
                                )
                                VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                ticker, trade_dir, entry_time, round(current_price, 2), round(total_buy_cost, 0), TRADE_SHARES,
                                current_regime, setup_tag, round(ev_score, 3), round(entry_sl_pct*100, 2), round(entry_tp_pct*100, 2), round(rr_ratio, 2), round(target_risk, 0)
                            ))
                            conn.commit()
                            
                            
                        update_account_cash(-total_buy_cost)
                        
                        # 🌟 擷取這筆交易的「陣型標籤」(從大腦傳來的 status 字串中切出來)
                        setup_tag = status.split(' ')[1] if len(status.split(' ')) > 1 else "傳統訊號"
                        
                        positions.append({
                            '進場價': current_price, '方向': trade_dir, '投入資金': total_buy_cost,
                            '進場時間': entry_time, '進場股數': TRADE_SHARES, '停利階段': 0,
                            '進場趨勢多頭': trend_is_bull,
                            # ✨ 存入快取記憶體
                            '陣型標籤': setup_tag, '市場狀態': current_regime, 
                            '期望值': ev_score, '預期停損(%)': entry_sl_pct, '預期停利(%)': entry_tp_pct, 
                            '風報比(RR)': rr_ratio, '風險金額': target_risk
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
        
        
        # ==========================================
        # 🛡️ 實戰級：根據「陣型 (Setup)」實施客製化停利策略
        # ==========================================
        volatility_pct = (latest_row['BB_std'] * 1.5) / current_price
        setup_tag = positions[0].get('陣型標籤', '傳統訊號') # 讀取進場時的陣型
        
        trend_is_with_me = (is_long and positions[0]['進場趨勢多頭']) or (not is_long and not positions[0]['進場趨勢多頭'])
        adx_is_strong = latest_row['ADX14'] > PARAMS['ADX_TREND_THRESHOLD']

        # 🌟 核心：依據不同 Setup，給予不同的防守與停利目標
        # 🌟 核心：依據不同 Setup，給予不同的防守與停利目標
        if "REVERSAL" in setup_tag or "抄底" in setup_tag or "摸頭" in setup_tag:
            # 【均值回歸型】目標是搶反彈，快進快出！
            DYNAMIC_SL = sys_params['SL_MIN_PCT'] # 🌟 改
            DYNAMIC_TP = 0.08  # 只要賺 8% 就滿足，不貪心
            ignore_tp = False  # 絕對不貪心，打到目標價強制作結
            
        elif "TREND" in setup_tag or "點火" in setup_tag or "倒貨" in setup_tag:
            # 【趨勢突破型】目標是吃大波段，讓利潤奔跑！
            DYNAMIC_SL = max(sys_params['SL_MIN_PCT'], min(volatility_pct, sys_params['SL_MAX_PCT'])) # 🌟 改
            DYNAMIC_TP = sys_params['TP_TREND_PCT'] # 🌟 改
            ignore_tp = True   # 🌟 無視傳統目標價，完全交給 Trailing Stop 死咬趨勢！
            
        else:
            # 【潛伏型或傳統 3 分制】中規中矩的作法
            DYNAMIC_SL = max(sys_params['SL_MIN_PCT'], min(volatility_pct, sys_params['SL_MAX_PCT'])) 
            DYNAMIC_TP = sys_params['TP_TREND_PCT'] if (trend_is_with_me and adx_is_strong) else sys_params['TP_BASE_PCT'] 
            # 🌟 [修復防當機] 改用 .get 防呆，或直接依賴陣型標籤
            ignore_tp = positions[0].get('進場分數', 0) >= 3

        # 🌟 Pandas 魔法：直接利用 ticker_df 切片，找出進場後的極端價格！
        try:
            # 將字串轉換為 pandas 日期格式，並對齊到 00:00:00
            entry_dt = pd.to_datetime(positions[0]['進場時間']).normalize()
            # 擷取進場日到今天的全部 K 線
            post_entry_df = ticker_df.loc[entry_dt:]
            
            if is_long:
                max_reached_price = max(post_entry_df['High'].max(), current_price, avg_cost)
            else:
                max_reached_price = min(post_entry_df['Low'].min(), current_price, avg_cost)
        except Exception as e:
            # 萬一時間解析失敗的備案
            max_reached_price = current_price
            
        # 計算防守線與目標價
        if is_long:
            trailing_stop_price = max_reached_price * (1 - DYNAMIC_SL)
            # 防線只進不退：取「原始停損」與「移動停損」的最高點
            final_stop_price = max(avg_cost * (1 - DYNAMIC_SL), trailing_stop_price)
            tp_price = avg_cost * (1 + DYNAMIC_TP)
            tp_stage_1_price = avg_cost * (1 + (DYNAMIC_TP * 0.5))
        else:
            trailing_stop_price = max_reached_price * (1 + DYNAMIC_SL)
            final_stop_price = min(avg_cost * (1 + DYNAMIC_SL), trailing_stop_price)
            tp_price = avg_cost * (1 - DYNAMIC_TP)
            tp_stage_1_price = avg_cost * (1 - (DYNAMIC_TP * 0.5))

        current_tp_stage = positions[0].get('停利階段', 0)
        exit_msg = ""
        is_partial = False

        # ==========================================
        # 🌟 觸發判定與部位平倉邏輯
        # ==========================================
        is_stop_loss = (current_price <= final_stop_price) if is_long else (current_price >= final_stop_price)
        is_take_profit = ((current_price >= tp_price) if is_long else (current_price <= tp_price)) and not ignore_tp
        is_stage_1 = (current_price >= tp_stage_1_price) if is_long else (current_price <= tp_stage_1_price)

        # ✨ 微結構防護：想跑但跑不掉的殘酷現實
        if (is_stop_loss or is_take_profit or is_stage_1) and (is_long and is_limit_down):
            print(f"⛓️ {ticker} 觸發出場，但目前【跌停鎖死】無人接刀，強制留倉承受風險！")
            return # 強制中斷出場程序
        if (is_stop_loss or is_take_profit or is_stage_1) and (not is_long and is_limit_up):
            print(f"⛓️ {ticker} 空單觸發回補，但目前【漲停鎖死】無法買回，強制留倉承受風險！")
            return # 強制中斷出場程序

        if is_stop_loss:
            real_loss_pct = ((current_price - avg_cost) / avg_cost) * 100 if is_long else ((avg_cost - current_price) / avg_cost) * 100
            # 判斷是初始停損還是移動停利出場
            if (is_long and final_stop_price > avg_cost) or (not is_long and final_stop_price < avg_cost):
                exit_msg = f"🛡️ 移動鎖利觸發 (鎖住獲利 {real_loss_pct:.3f}%)"
            else:
                exit_msg = f"🛑 停損出場 ({real_loss_pct:.3f}%)"
                
        elif is_take_profit:
            real_win_pct = ((current_price - avg_cost) / avg_cost) * 100 if is_long else ((avg_cost - current_price) / avg_cost) * 100
            exit_msg = f"🎯 達標傳統停利 (+{real_win_pct:.3f}%)"
            
        elif is_stage_1 and current_tp_stage == 0:
            if trend_is_with_me and adx_is_strong:
                positions[0]['停利階段'] = 1
                try:
                    with pyodbc.connect(DB_CONN_STR) as conn:
                        conn.cursor().execute("UPDATE active_positions SET [停利階段] = 1 WHERE [Ticker SYMBOL] = ?", (ticker,))
                        conn.commit()
                except: pass
                print(f"🌊 {ticker} 達標第一階段，趨勢極強 ➔ 死抱全倉讓利潤奔跑！")
            else:
                real_win_pct = ((current_price - avg_cost) / avg_cost) * 100 if is_long else ((avg_cost - current_price) / avg_cost) * 100
                exit_msg = f"💰 達標第一階段 (+{real_win_pct:.3f}%) ➔ 趨勢偏弱，減碼 50% 入袋為安"
                is_partial = True
                
        # 👇 反轉訊號的差異化處理
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
                        
                        # 🌟 採用 GPT 防護機制：避免除以 2 變成 0 股，並加入全平倉判定
                        if is_partial:
                            shares_to_sell = max(1, int(shares_to_sell / 2))
                            # 🛡️ [防禦] 如果算出來要賣的股數等於或大於持有股數，強制轉為全部平倉
                            if shares_to_sell >= batch.get('進場股數', 2000):
                                is_partial = False
                                shares_to_sell = batch.get('進場股數', 2000)
                            else:
                                invested_portion = invested_portion / 2

                        exit_fee_rate = PARAMS['FEE_RATE'] * PARAMS['FEE_DISCOUNT']
                        
                        # 1. 執行出場滑價計算
                        trade_dir_int = 1 if is_long else -1
                        actual_exit_price = apply_slippage(current_price, -trade_dir_int, PARAMS['MARKET_SLIPPAGE'])
                        
                        # 2. 呼叫機構級計算機 (精算手續費與稅金)
                        pnl, invested = calculate_pnl(
                            direction=trade_dir_int,
                            entry_price=batch['進場價'],
                            exit_price=actual_exit_price,
                            shares=shares_to_sell,
                            fee_rate=exit_fee_rate,
                            tax_rate=PARAMS['TAX_RATE']
                        )
                        
                        # 3. 統一變數名稱，對接後續的 SQL 寫入
                        net_profit_cash = pnl
                        profit_pct = (pnl / invested) * 100
                        
                        # 🌟 [修復：會計帳面漏水] 改用 invested_portion，將當初的進場手續費精準補回帳戶
                        cash_returned_this_batch = invested_portion + pnl
                        
                        total_cash_back += cash_returned_this_batch
                        current_bank_cash += cash_returned_this_batch 
                        
                        # ✨ 寫入歷史明細表 (擴充機構級歸因)
                        cursor.execute('''
                            INSERT INTO trade_history 
                            ([Ticker SYMBOL], [方向], [進場時間], [出場時間], [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金],
                             [市場狀態], [進場陣型], [期望值], [預期停損(%)], [預期停利(%)], [風報比(RR)], [風險金額])
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            ticker, batch['方向'], batch['進場時間'], exit_time, 
                            round(batch['進場價'], 2), round(current_price, 2), 
                            round(profit_pct, 3), round(net_profit_cash, 0), round(current_bank_cash, 0),
                            batch.get('市場狀態', '未知'), batch.get('陣型標籤', '傳統訊號'),
                            round(batch.get('期望值', 0.0), 3), round(batch.get('預期停損(%)', 0.0)*100, 2), 
                            round(batch.get('預期停利(%)', 0.0)*100, 2), round(batch.get('風報比(RR)', 0.0), 2), round(batch.get('風險金額', 0.0), 0)
                        ))
                        if is_partial:
                            batch['進場股數'] -= shares_to_sell
                            batch['投入資金'] -= invested_portion
                            batch['停利階段'] = 1

                    # 更新銀行總可用現金
                    update_account_cash(total_cash_back)
                    CURRENT_EQUITY = current_bank_cash

                    if is_partial:
                        # 🌟 [修復：碎股脫節] 直接傳入 Python 算好的剩餘數字，不讓 SQL 自己除以 2
                        remaining_funds = batch['投入資金']
                        remaining_shares = batch['進場股數']
                        cursor.execute('''
                            UPDATE active_positions 
                            SET [投入資金] = ?, [進場股數] = ?, [停利階段] = 1 
                            WHERE [Ticker SYMBOL] = ?
                        ''', (remaining_funds, remaining_shares, ticker))
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