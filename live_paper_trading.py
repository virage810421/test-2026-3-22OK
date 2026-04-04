import warnings
# 🌟 將警告消音器放在最前面
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', message=".*pandas only supports SQLAlchemy.*")
import pandas as pd
import pyodbc  
from datetime import datetime
import requests
# 🌟 補上 add_chip_data 模組的匯入
from screening import smart_download, apply_slippage, calculate_pnl, inspect_stock, add_chip_data
from config import PARAMS
from strategies import get_active_strategy


# ==========================================
# ⚙️ 系統環境設定 (總開關)
# ==========================================
# 🌟 設為 True：測試模式，不會發送真實 LINE 訊息 (防打擾、省配額)
# 🌟 設為 False：實戰模式，會真實傳送到您的手機
IS_TEST_MODE = True

DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

# ==========================================
# 📡 戰情通訊官：LINE Bot 推播模組
# ==========================================
def send_line_bot_msg(msg):
    if IS_TEST_MODE:
        print("\n🔇 [測試模式已開啟] 攔截 LINE 發送請求。")
        print("-" * 30)
        print(f"📝 原本會發送至手機的內容為：\n{msg}")
        print("-" * 30)
        return  

    token = "/VY7IKhPqqJ1W6v04s9f8vi8hDbo5W1daaCD2LD3sIki1rWq3wF41uF6mwbKf4UsERhRK68MGn2K1nKRIffEzSZ8OTQgm0VWQ1g3CLTzNB9RZrAJ7py8MtPuzPi5/I9GnkEM69CeX983gFCQPBRx1AdB04t89/1O/w1cDnyilFU="
    user_id = "U5dd01cba7ab960a7bd1a1b6efc43411b" 

    url = 'https://api.line.me/v2/bot/message/push'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    data = {
        "to": user_id,
        "messages": [{"type": "text", "text": msg}]
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            print("📲 LINE 戰情報告已成功發送至您的手機！")
        else:
            print(f"⚠️ LINE 官方拒絕了發送！狀態碼: {response.status_code}")
            print(f"🔍 錯誤詳情: {response.text}")
    except Exception as e:
        print(f"⚠️ LINE 網路發送發生例外錯誤: {e}")

# ==========================================
# 💰 帳戶管理模組
# ==========================================
def get_available_cash(cursor):
    try:
        cursor.execute("SELECT [可用現金] FROM account_info WHERE [帳戶名稱] = '我的實戰帳戶'")
        row = cursor.fetchone()
        if row: return float(row[0])
    except: pass
    
    try:
        cursor.execute("INSERT INTO account_info ([帳戶名稱], [可用現金], [最後更新時間]) VALUES ('我的實戰帳戶', 5000000, ?)", (datetime.now(),))
        cursor.connection.commit()
    except: pass
    return 5000000.0

def run_eod_broker():
    print("\n" + "="*60)
    print("🤖 [自動下單中心] 啟動盤後結算與執行程序...")
    print("="*60)
    
    try:
        conn = pyodbc.connect(DB_CONN_STR)
        cursor = conn.cursor()
    except Exception as e:
        print(f"🛑 無法連線 SQL 資料庫: {e}")
        return

    current_cash = get_available_cash(cursor)
    try: active_df = pd.read_sql("SELECT * FROM active_positions", conn)
    except: active_df = pd.DataFrame()
        
    stock_value = 0.0
    daily_log = [] 

    # ==========================================
    # 🛡️ 階段一：掃描庫存 (動態停損/停利)
    # ==========================================
    if not active_df.empty:
        for _, pos in active_df.iterrows():
            ticker = pos['Ticker SYMBOL']
            direction = pos['方向']
            entry_price = float(pos['進場價'])
            shares = int(pos['進場股數'])
            setup_tag = pos.get('進場陣型', '傳統訊號')
            
            raw_tp = pos.get('停利階段', 0)
            tp_stage = int(raw_tp) if pd.notna(raw_tp) else 0
            
            is_long = ('Long' in direction or '多' in direction)

            df = smart_download(ticker, period="3mo")
            if df.empty: continue
            
            # 🌟 核心修復 1：必須補上籌碼資料，才能給 inspect_stock 裡面的 AI 晶片判斷！
            df = add_chip_data(df, ticker)
            
            result = inspect_stock(ticker, preloaded_df=df, p=PARAMS)
            if not result or '計算後資料' not in result: continue
            computed_df = result['計算後資料']
            latest_row = computed_df.iloc[-1]
            curr_price = latest_row['Close']
            
            volatility_pct = (latest_row.get('BB_std', 0) * 1.5) / curr_price if curr_price > 0 else 0.05
            trend_is_with_me = (is_long and latest_row['Close'] > latest_row.get('BBI', 0)) or (not is_long and latest_row['Close'] < latest_row.get('BBI', 0))
            adx_is_strong = latest_row.get('ADX14', 0) > PARAMS.get('ADX_TREND_THRESHOLD', 20)
            
            active_strategy = get_active_strategy(setup_tag)
            DYNAMIC_SL, DYNAMIC_TP, ignore_tp = active_strategy.get_exit_rules(
                PARAMS, volatility_pct, trend_is_with_me, adx_is_strong, pos.get('進場分數', 0)
            )

            entry_dt = pd.to_datetime(pos['進場時間']).normalize()
            post_entry_df = df.loc[entry_dt:] if entry_dt in df.index else df
            
            if is_long:
                max_price = post_entry_df['High'].max() if not post_entry_df.empty else curr_price
                final_stop = max(entry_price * (1 - DYNAMIC_SL), max_price * (1 - DYNAMIC_SL))
                tp_stage_1 = entry_price * (1 + (DYNAMIC_TP * 0.5))
                tp_final = entry_price * (1 + DYNAMIC_TP)
                is_stop, is_tp_half, is_tp_full = (curr_price <= final_stop), (curr_price >= tp_stage_1 and tp_stage == 0), (curr_price >= tp_final and not ignore_tp)
            else:
                min_price = post_entry_df['Low'].min() if not post_entry_df.empty else curr_price
                final_stop = min(entry_price * (1 + DYNAMIC_SL), min_price * (1 + DYNAMIC_SL))
                tp_stage_1 = entry_price * (1 - (DYNAMIC_TP * 0.5))
                tp_final = entry_price * (1 - DYNAMIC_TP)
                is_stop, is_tp_half, is_tp_full = (curr_price >= final_stop), (curr_price <= tp_stage_1 and tp_stage == 0), (curr_price <= tp_final and not ignore_tp)

            exit_msg, sell_shares = "", 0
            
            if is_stop:
                exit_msg, sell_shares = "🛑 觸發防守線", shares
            elif is_tp_full:
                exit_msg, sell_shares = "🎯 達標最終停利", shares
            elif is_tp_half:
                if trend_is_with_me and adx_is_strong:
                    cursor.execute("UPDATE active_positions SET [停利階段] = 1 WHERE [Ticker SYMBOL] = ?", (ticker,))
                    daily_log.append(f"🌊 {ticker} 達第一階段，趨勢強抱緊！")
                else:
                    exit_msg, sell_shares = "💰 達第一階段，減碼50%", max(1, shares // 2)

            if sell_shares > 0:
                trade_dir_int = 1 if is_long else -1
                actual_exit_price = apply_slippage(curr_price, -trade_dir_int, PARAMS['MARKET_SLIPPAGE'])
                pnl, invested = calculate_pnl(trade_dir_int, entry_price, actual_exit_price, sell_shares, PARAMS['FEE_RATE']*PARAMS['FEE_DISCOUNT'], PARAMS['TAX_RATE'])
                
                current_cash += (invested + pnl)
                
                if sell_shares == shares:
                    cursor.execute("DELETE FROM active_positions WHERE [Ticker SYMBOL] = ? AND [進場時間] = ?", (ticker, pos['進場時間']))
                else:
                    cursor.execute("UPDATE active_positions SET [進場股數] = ?, [投入資金] = ?, [停利階段] = 1 WHERE [Ticker SYMBOL] = ?", (shares - sell_shares, pos['投入資金'] - invested, ticker))
                    stock_value += curr_price * (shares - sell_shares)

                # 🌟 致命核彈 Bug 修復 2：正確寫入「歷史戰績表 (trade_history)」，而不是錯誤寫回庫存表
                try:
                    cursor.execute('''
                        INSERT INTO trade_history (
                            [策略名稱], [Ticker SYMBOL], [方向], [進場時間], [出場時間],
                            [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金]
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        "實戰自動結算", ticker, direction, pos['進場時間'], datetime.now(),
                        entry_price, actual_exit_price, (pnl/invested)*100, pnl, current_cash
                    ))
                except Exception as e:
                    print(f"⚠️ {ticker} 寫入歷史戰績表失敗: {e}")
                
                daily_log.append(f"{exit_msg} {ticker}: 損益 ${pnl:,.0f}")
            else:
                stock_value += curr_price * shares

    # ==========================================
    # ⚔️ 階段二：執行建倉任務
    # ==========================================
    try: decisions = pd.read_csv("daily_decision_desk.csv")
    except: decisions = pd.DataFrame()
        
    total_nav = current_cash + stock_value if (current_cash + stock_value) > 0 else 1000000
    
    if not decisions.empty:
        for _, row in decisions.iterrows():
            ticker = row.get('Ticker', 'Unknown') 
            kelly_pct = float(row.get('Kelly_Pos', 0))
            trade_direction = row.get('Direction', '做多(Long)') 
            
            if kelly_pct <= 0: continue
            
            cursor.execute("SELECT COUNT(*) FROM active_positions WHERE [Ticker SYMBOL] = ?", (ticker,))
            if cursor.fetchone()[0] > 0: continue
                
            df = smart_download(ticker, period="5d")
            if df.empty: continue
            curr_price = df['Close'].iloc[-1]
            
            shares = int((total_nav * kelly_pct) / curr_price)
            shares = int(shares // 1000) * 1000 if shares >= 1000 else shares
            if shares < 1: continue
            
            total_cost = curr_price * shares * (1 + PARAMS['FEE_RATE']*PARAMS['FEE_DISCOUNT'])
            
            can_afford = current_cash >= total_cost or PARAMS.get('IGNORE_CASH_LIMIT', False)
            
            if can_afford:
                if not PARAMS.get('IGNORE_CASH_LIMIT', False):
                    current_cash -= total_cost
                
                stock_value += curr_price * shares
                
                cursor.execute('''
                    INSERT INTO active_positions (
                        [Ticker SYMBOL], [方向], [進場時間], [進場價], [投入資金], 
                        [進場股數], [停利階段], [進場陣型], [期望值], [風險金額]
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                ''', (
                    ticker, trade_direction, datetime.now(), curr_price, total_cost, 
                    shares, row.get('Structure', 'AI訊號'), 
                    row.get('EV', 0.0), total_cost * 0.05
                ))
                
                action_icon = "🟢 做多" if "Long" in trade_direction else "🔴 放空"
                daily_log.append(f"{action_icon} {ticker}: {shares}股 (花費 ${total_cost:,.0f})")
                
    # ==========================================
    # 📊 階段三：結算與發送 LINE 戰報
    # ==========================================
    cursor.execute("UPDATE account_info SET [可用現金] = ?, [最後更新時間] = ? WHERE [帳戶名稱] = '我的實戰帳戶'", (current_cash, datetime.now()))
    conn.commit()
    conn.close()
    
    net_worth = current_cash + stock_value
    
    report_text = f"📊 [HFA 系統戰報] {datetime.now().strftime('%Y-%m-%d')}\n"
    report_text += f"💰 總淨值: ${net_worth:,.0f}\n"
    report_text += f"💵 現金: ${current_cash:,.0f} | 📦 股票: ${stock_value:,.0f}\n"
    report_text += "-"*20 + "\n"
    if daily_log:
        report_text += "\n".join(daily_log)
    else:
        report_text += "💤 今日無任何交易動作，持股續抱。"

    print(report_text)
    send_line_bot_msg(report_text) 

if __name__ == "__main__":
    run_eod_broker()