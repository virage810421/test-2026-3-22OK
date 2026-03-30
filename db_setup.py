import pyodbc
import sys

def setup_tsql_database():
    # ==========================================
    # 🛡️ 安全保險鎖：確認是否執行核彈級重置
    # ==========================================
    print("========================================================")
    print("💣 【資料庫核彈級重置系統】")
    print("⚠️ 警告：這將永久刪除所有交易紀錄、持倉、回測與籌碼資料！")
    print("========================================================")
    
    confirm = input("🚀 確定要執行「全庫刪除並重建乾淨空表」嗎？(y/n): ")
    if confirm.lower() != 'y':
        print("🛑 已安全取消作業，您的資料毫髮無傷。")
        return

    print("\n🔧 準備連線至 SQL Server 進行全庫建置與檢查...")
    
    master_conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=localhost;'  
        r'DATABASE=master;' 
        r'Trusted_Connection=yes;'
    )

    conn = None
    try:
        conn = pyodbc.connect(master_conn_str, autocommit=True)
        cursor = conn.cursor()

        # 1. 檢查並建立 Database
        cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '股票online')
            BEGIN
                CREATE DATABASE 股票online;
            END
        ''')
        print("✅ 資料庫 [股票online] 確認/建立完成！")

        conn.close()

        # 2. 切換至目標資料庫
        target_conn_str = master_conn_str.replace('DATABASE=master;', 'DATABASE=股票online;')
        conn = pyodbc.connect(target_conn_str)
        cursor = conn.cursor()

        # ==========================================
        # 💣 核彈級清理：強制刪除所有舊資料表
        # ==========================================
        print("\n💣 啟動強制重置模式，正在刪除舊有資料表...")
        tables_to_drop = [
            'trade_history',
            'active_positions',
            'daily_chip_data',
            'strategy_performance',
            'backtest_history',
            'account_info'
        ]
        
        for table in tables_to_drop:
            cursor.execute(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}")
        print("🗑️ 舊資料表已全數刪除完畢！\n")


        # ==========================================
        # 🏗️ 開始建立全新資料表
        # ==========================================
        print("🏗️ 開始建立 6 大核心資料表 (全新乾淨版)...")

        # --- 表單 1：歷史交易總帳 (trade_history) ---
        cursor.execute('''
            CREATE TABLE trade_history (
                [Ticker SYMBOL] VARCHAR(20),
                [方向] NVARCHAR(10),
                [進場時間] DATETIME,
                [出場時間] DATETIME,
                [進場價] FLOAT,
                [出場價] FLOAT,
                [報酬率(%)] DECIMAL(10,3),
                [淨損益金額] FLOAT,
                [結餘本金] FLOAT
            )
        ''')
        print("   👉 建立 [trade_history] 成功")

        # --- 表單 2：目前持倉工作區 (active_positions) ---
        cursor.execute('''
            CREATE TABLE active_positions (
                [Ticker SYMBOL] VARCHAR(20),
                [方向] NVARCHAR(10),
                [進場時間] DATETIME,
                [進場價] FLOAT,
                [投入資金] FLOAT,
                [停利階段] INT,
                [進場股數] INT
            )
        ''')
        print("   👉 建立 [active_positions] 成功")

        # --- 表單 3：每日法人籌碼庫 (daily_chip_data) ---
        cursor.execute('''
            CREATE TABLE daily_chip_data (
                [日期] DATE,
                [Ticker SYMBOL] VARCHAR(20),
                [外資買賣超] FLOAT,
                [投信買賣超] FLOAT,
                [自營商買賣超] FLOAT,
                [三大法人合計] FLOAT,
                PRIMARY KEY ([日期], [Ticker SYMBOL])
            )
        ''')
        print("   👉 建立 [daily_chip_data] 成功")

        # --- 表單 4：歷史勝率與報酬追蹤表 (strategy_performance) ---
        cursor.execute('''
            CREATE TABLE strategy_performance (
                [Ticker SYMBOL] VARCHAR(20),
                [紀錄時間] DATETIME,
                [系統勝率(%)] DECIMAL(10,3),
                [累計報酬率(%)] DECIMAL(10,3),
                [今日燈號] NVARCHAR(50),
                [期望值] DECIMAL(10,3)
            )
        ''')
        print("   👉 建立 [strategy_performance] 成功")

        # --- 表單 5：大腦回測明細 (backtest_history) ---
        cursor.execute('''
            CREATE TABLE backtest_history (
                [策略名稱] NVARCHAR(50),
                [Ticker SYMBOL] VARCHAR(20),
                [方向] NVARCHAR(10),
                [進場時間] DATETIME,
                [出場時間] DATETIME,
                [進場價] FLOAT,
                [出場價] FLOAT,
                [報酬率(%)] DECIMAL(10,3),
                [淨損益金額] FLOAT,
                [結餘本金] FLOAT
            )
        ''')
        print("   👉 建立 [backtest_history] 成功")

        # --- 表單 6：帳戶資訊 (account_info) ---
        cursor.execute('''
            CREATE TABLE account_info (
                [帳戶名稱] NVARCHAR(50) PRIMARY KEY,
                [可用現金] FLOAT,
                [最後更新時間] DATETIME
            )
        ''')
        print("   👉 建立 [account_info] 成功")

        conn.commit()
        print("\n✅ 系統基礎建設已全部就緒！(舊資料已徹底清除)")

    except Exception as e:
        print(f"\n❌ 發生未知的錯誤: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_tsql_database()