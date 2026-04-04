import pandas as pd
import pyodbc
from datetime import datetime, timedelta
from FinMind.data import DataLoader
import time
from config import WATCH_LIST  # 🌟 匯入中央名單

def update_daily_chips():
    # 🌟 假日自動檢查鎖
    now = datetime.now()
    if now.weekday() >= 5: # 5 是週六, 6 是週日
        print(f"[{now.strftime('%H:%M:%S')}] ☕ 今天是週末（非交易日），自動收集車休息中...")
        return 
        
    print(f"[{datetime.now().strftime('%H:%M:%S')}] > 啟動每日法人籌碼資料收集車...")
    
    # 🌟 核心：讀取中央名單，並自動拔除 ".TW" 讓 FinMind 能夠辨識
    pure_watch_list = [ticker.replace(".TW", "") for ticker in WATCH_LIST]

    # 設定要抓取的天數
    days_to_fetch = 5
    start_dt = (datetime.now() - timedelta(days=days_to_fetch)).strftime("%Y-%m-%d")
    
    
    dl = DataLoader()
    
    # 🌟 完整還原的資料庫連線字串 (完美閉合)
    DB_CONN_STR = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=localhost;'  
        r'DATABASE=股票online;'
        r'Trusted_Connection=yes;'
    )

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            
            for stock_id in pure_watch_list:
                print(f"📡 正在下載 {stock_id} 的籌碼資料...")
                
                try:
                    # 從 FinMind 下載資料
                    chip_df = dl.taiwan_stock_institutional_investors(stock_id=stock_id, start_date=start_dt)
                    if chip_df is None or chip_df.empty:
                        continue
                        
                    # 計算淨買賣超
                    chip_df['Net'] = chip_df['buy'] - chip_df['sell']
                    
                    # 依日期分組，抓出三大法人 (對齊英文名稱)
                    foreign = chip_df[chip_df['name'] == 'Foreign_Investor'].groupby('date')['Net'].sum()
                    trust = chip_df[chip_df['name'] == 'Investment_Trust'].groupby('date')['Net'].sum()
                    dealers = chip_df[chip_df['name'].isin(['Dealer_self', 'Dealer_Hedging'])].groupby('date')['Net'].sum()
                    
                    # 整理成一張乾淨的表
                    dates = chip_df['date'].unique()
                    for date_str in dates:
                        f_net = float(foreign.get(date_str, 0))
                        t_net = float(trust.get(date_str, 0))
                        d_net = float(dealers.get(date_str, 0))
                        total_net = f_net + t_net + d_net
                        
                        ticker_symbol = f"{stock_id}.TW"
                        
                        # 寫入 SQL (使用 IF NOT EXISTS 避免重複寫入報錯)
                        cursor.execute('''
                            IF NOT EXISTS (SELECT 1 FROM daily_chip_data WHERE [日期] = ? AND [Ticker SYMBOL] = ?)
                            BEGIN
                                INSERT INTO daily_chip_data 
                                ([日期], [Ticker SYMBOL], [外資買賣超], [投信買賣超], [自營商買賣超], [三大法人合計])
                                VALUES (?, ?, ?, ?, ?, ?)
                            END
                        ''', (date_str, ticker_symbol, date_str, ticker_symbol, f_net, t_net, d_net, total_net))
                    
                    conn.commit()
                    print(f"✅ {stock_id} 寫入完成！")
                    
                except Exception as e:
                    print(f"⚠️ {stock_id} 處理失敗: {e}")
                
                # 稍微等待，避免被 FinMind 鎖 IP
                time.sleep(1)

        print("🎉 所有籌碼資料更新完畢！去資料庫看看吧！")

    except pyodbc.Error as e:
        print(f"❌ SQL Server 連線失敗: {e}")

# ==========================================
# 2️⃣ 每月任務：抓取月營收 (FinMind API)
# ==========================================
def update_monthly_revenue():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 📦 啟動【月營收】收集車(抓取近5年)...")
    pure_watch_list = [ticker.replace(".TW", "") for ticker in WATCH_LIST]
    # 🌟 正確位置：在這裡改成 1825 天 (5年)
    start_dt = (datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d") 
    dl = DataLoader()
    
    DB_CONN_STR = (r'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=股票online;Trusted_Connection=yes;')

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            for stock_id in pure_watch_list:
                try:
                    df = dl.taiwan_stock_month_revenue(stock_id=stock_id, start_date=start_dt)
                    if df is None or df.empty: continue
                    
                    for _, row in df.iterrows():
                        ym = f"{row['revenue_year']}-{str(row['revenue_month']).zfill(2)}"
                        yoy = row.get('RevenueYearOnYearRatio', 0) 
                        ticker = f"{stock_id}.TW"
                        
                        cursor.execute('''
                            IF EXISTS (SELECT 1 FROM fundamental_data WHERE [Ticker SYMBOL]=? AND [資料年月]=?)
                                UPDATE fundamental_data SET [單月營收年增率(%)]=?, [更新時間]=GETDATE() WHERE [Ticker SYMBOL]=? AND [資料年月]=?
                            ELSE
                                INSERT INTO fundamental_data ([Ticker SYMBOL], [資料年月], [單月營收年增率(%)]) VALUES (?, ?, ?)
                        ''', (ticker, ym, yoy, ticker, ym, ticker, ym, yoy))
                        
                    conn.commit()
                    print(f"✅ {stock_id} 月營收更新完成！")
                except Exception as e:
                    print(f"⚠️ {stock_id} 月營收處理失敗: {e}")
                time.sleep(1)
    except pyodbc.Error as e:
        print(f"❌ SQL Server 連線失敗: {e}")

# ==========================================
# 3️⃣ 每季任務：抓取季財報 (FinMind API 終極擴充版)
# ==========================================
def update_quarterly_financials():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 啟動【季財報】收集車(抓取近5年)...")
    pure_watch_list = [ticker.replace(".TW", "") for ticker in WATCH_LIST]
    start_dt = (datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d") 
    dl = DataLoader()
    DB_CONN_STR = (r'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=股票online;Trusted_Connection=yes;')

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            for stock_id in pure_watch_list:
                try:
                    df = dl.taiwan_stock_financial_statement(stock_id=stock_id, start_date=start_dt)
                    if df is None or df.empty: continue
                    
                    dates = df['date'].unique()
                    for d in dates:
                        q_month = pd.to_datetime(d).month
                        q_str = f"{pd.to_datetime(d).year}-Q{(q_month-1)//3 + 1}"
                        ticker = f"{stock_id}.TW"
                        
                        sub_df = df[df['date'] == d]
                        
                        def get_val(type_name):
                            res = sub_df[sub_df['type'] == type_name]['value']
                            return float(res.iloc[0]) if not res.empty else 0.0

                        # 基本財報數據
                        eps = get_val('EPS')
                        gross_margin = get_val('GrossProfitMargin')
                        op_margin = get_val('OperatingIncomeMargin')
                        roe = get_val('ROE')
                        net_margin = get_val('NetIncomeMargin')
                        cash_flow = get_val('CashFlowsFromOperatingActivities') 
                        
                        # 🌟 新增計算：負債比率 = (總負債 / 總資產)
                        assets = get_val('TotalAssets')
                        liabilities = get_val('TotalLiabilities')
                        debt_ratio = (liabilities / assets * 100) if assets > 0 else 0.0
                        
                        # 🌟 新增計算：本業獲利比 = (營業利益 / 稅前淨利)
                        op_income = get_val('OperatingIncome')
                        pre_tax = get_val('IncomeBeforeTax')
                        core_profit_ratio = (op_income / pre_tax * 100) if pre_tax > 0 else 100.0

                        cursor.execute('''
                            IF EXISTS (SELECT 1 FROM fundamental_data WHERE [Ticker SYMBOL]=? AND [資料年月]=?)
                                UPDATE fundamental_data 
                                SET [單季EPS]=?, [毛利率(%)]=?, [營業利益率(%)]=?, [ROE(%)]=?, [稅後淨利率(%)]=?, [營業現金流]=?, [負債比率(%)]=?, [本業獲利比(%)]=?, [更新時間]=GETDATE() 
                                WHERE [Ticker SYMBOL]=? AND [資料年月]=?
                            ELSE
                                INSERT INTO fundamental_data 
                                ([Ticker SYMBOL], [資料年月], [單季EPS], [毛利率(%)], [營業利益率(%)], [ROE(%)], [稅後淨利率(%)], [營業現金流], [負債比率(%)], [本業獲利比(%)]) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (ticker, q_str, eps, gross_margin, op_margin, roe, net_margin, cash_flow, debt_ratio, core_profit_ratio, ticker, q_str, 
                              ticker, q_str, eps, gross_margin, op_margin, roe, net_margin, cash_flow, debt_ratio, core_profit_ratio))
                        
                    conn.commit()
                    print(f"✅ {stock_id} 季財報更新完成！")
                except Exception as e:
                    print(f"⚠️ {stock_id} 季財報處理失敗: {e}")
                time.sleep(1)
    except pyodbc.Error as e:
        print(f"❌ SQL Server 連線失敗: {e}")

# ==========================================
# ⚙️ 總控制台：時間分流閥 (主發動機)
# ==========================================
if __name__ == "__main__":
    now = datetime.now()
    
    print("========================================")
    print("🚀 綜合數據物流中心啟動中...")
    print(f"📅 今日日期：{now.strftime('%Y-%m-%d')} | 星期{now.weekday() + 1}")
    print("========================================")

    # 🚦 閥門 A：每日籌碼 (避開六日)
    update_daily_chips()

    # 🚦 閥門 B：月營收 (每月 11 號到 15 號之間執行)
    #if 11 <= now.day <= 15:

    #---⏳開啟就可以手動添加----   
    if True:  
        update_monthly_revenue()
    else:
        print("⏳ 未到月營收更新期 (每月 11~15 號)，跳過檢查。")

    # 🚦 閥門 C：季財報 (每年 3, 5, 8, 11 月的 15 號到 20 號之間執行)
    # 注意：5月發布Q1, 8月發布Q2, 11月發布Q3, 隔年3月發布年報(Q4)
    if now.month in [3, 5, 8, 11] and 15 <= now.day <= 20:
        update_quarterly_financials()
    else:
        print("⏳ 未到季財報更新期 (3,5,8,11月中旬)，跳過檢查。")

    print("\n🎉 今日所有物流任務排程檢驗完畢！")