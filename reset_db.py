import pyodbc
from datetime import datetime

# 資料庫連線字串
DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

def reset_trading_system(initial_cash=500000000):
    """
    ⚠️ 警告：這將永久刪除所有交易紀錄與持倉，請謹慎使用。
    """
    confirm = input(f"🚀 確定要重置所有資料並將資金設為 {initial_cash:,.0f} 嗎？(y/n): ")
    if confirm.lower() != 'y':
        print("🛑 已取消重置作業。")
        return

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()

            print("🧹 正在清理資料表...")
            
            # 1. 清空所有交易相關表單
            tables_to_clean = [
                "active_positions",      # 目前持倉
                "trade_history",         # 實戰/模擬歷史
                "backtest_history",      # 大腦回測明細
                "strategy_performance"   # 策略評分紀錄
            ]
            
            for table in tables_to_clean:
                cursor.execute(f"DELETE FROM {table}")
                print(f"✅ {table} 已清空")

            # 2. 重置銀行資金
            # 先檢查 account_info 是否有資料，沒有就 Insert，有就 Update
            cursor.execute("SELECT COUNT(*) FROM account_info WHERE [帳戶名稱] = '我的實戰帳戶'")
            if cursor.fetchone()[0] == 0:
                cursor.execute('''
                    INSERT INTO account_info ([帳戶名稱], [可用現金], [最後更新時間])
                    VALUES (?, ?, ?)
                ''', ('我的實戰帳戶', initial_cash, datetime.now()))
            else:
                cursor.execute('''
                    UPDATE account_info 
                    SET [可用現金] = ?, [最後更新時間] = ? 
                    WHERE [帳戶名稱] = '我的實戰帳戶'
                ''', (initial_cash, datetime.now()))
            
            print(f"💰 銀行帳戶已重置為: ${initial_cash:,.0f}")
            
            conn.commit()
            print("\n✨ 資料庫大掃除完成！現在你可以重新啟動程式了。")

    except Exception as e:
        print(f"❌ 重置過程中發生錯誤: {e}")

if __name__ == "__main__":
    reset_trading_system(500000000) # 這裡輸入你想設定的初始金額