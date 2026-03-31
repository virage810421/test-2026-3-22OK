import pandas as pd
import pyodbc
from datetime import datetime, timedelta
from FinMind.data import DataLoader
import time

def update_daily_chips():
    # 🌟 新增：假日自動檢查鎖
    now = datetime.now()
    if now.weekday() >= 5: # 5 是週六, 6 是週日
        print(f"[{now.strftime('%H:%M:%S')}] ☕ 今天是週末（非交易日），自動收集車休息中...")
        return # 直接結束程式，不浪費 API 額度
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚚 啟動每日法人籌碼資料收集車...")
    
    # 你的觀察清單
    watch_list = [
        "2330", "2454", "2317", "2303", "2308",
        "2382", "3231", "6669", "2357", "3034",
        "2603", "2609", "2615",
        "2881", "2882", "2891",
        "1519", "1513", "2618", "2002"
    ]

    # 設定要抓取的天數 (如果是第一次跑，可以抓過去 5 天補齊資料)
    days_to_fetch = 5
    start_dt = (datetime.now() - timedelta(days=days_to_fetch)).strftime("%Y-%m-%d")
    
    dl = DataLoader()
    
    DB_CONN_STR = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=localhost;'  
        r'DATABASE=股票online;'
        r'Trusted_Connection=yes;'
    )

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            
            for stock_id in watch_list:
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

if __name__ == "__main__":
    update_daily_chips()