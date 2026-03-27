import pyodbc

def setup_tsql_database():
    print("🔧 準備連線至 SQL Server 進行全庫建置與檢查...")
    
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

        print("🏗️ 開始檢查 4 大核心資料表...")

        # --- 表單 1：歷史交易總帳 (trade_history) ---
        cursor.execute('''
            IF OBJECT_ID('trade_history', 'U') IS NULL
            BEGIN
                CREATE TABLE trade_history (
                    [Ticker SYMBOL] VARCHAR(20),
                    [方向] NVARCHAR(10),
                    [進場時間] DATETIME,
                    [出場時間] DATETIME,
                    [進場價] FLOAT,
                    [出場價] FLOAT,
                    [報酬率(%)] DECIMAL(10,3)
                )
            END
        ''')
        print("   👉 檢查/建立 [trade_history] 成功")
        
        # --- 表單 2：目前持倉工作區 (active_positions) 新版 ---
        cursor.execute('''
            IF OBJECT_ID('active_positions', 'U') IS NULL
            BEGIN
                CREATE TABLE active_positions (
                    [Trade_ID] INT IDENTITY(1,1) PRIMARY KEY,
                    [Ticker SYMBOL] VARCHAR(20),
                    [方向] NVARCHAR(10),
                    [進場時間] DATETIME,
                    [進場價] FLOAT,
                    [投入資金] FLOAT
                )
            END
        ''')
        print("   👉 檢查/建立 [active_positions] 成功")

        # --- 表單 3：每日法人籌碼庫 (daily_chip_data) ---
        cursor.execute('''
            IF OBJECT_ID('daily_chip_data', 'U') IS NULL
            BEGIN
                CREATE TABLE daily_chip_data (
                    [日期] DATE,
                    [Ticker SYMBOL] VARCHAR(20),
                    [外資買賣超] FLOAT,
                    [投信買賣超] FLOAT,
                    [自營商買賣超] FLOAT,
                    [三大法人合計] FLOAT,
                    PRIMARY KEY ([日期], [Ticker SYMBOL])
                )
            END
        ''')
        print("   👉 檢查/建立 [daily_chip_data] 成功")

        # --- 表單 4：歷史勝率與報酬追蹤表 (strategy_performance) ---
        cursor.execute('''
            IF OBJECT_ID('strategy_performance', 'U') IS NULL
            BEGIN
                CREATE TABLE strategy_performance (
                    [Log_ID] INT IDENTITY(1,1) PRIMARY KEY,
                    [紀錄時間] DATETIME,
                    [Ticker SYMBOL] VARCHAR(20),
                    [系統勝率(%)] DECIMAL(10,3),
                    [累計報酬率(%)] DECIMAL(10,3),
                    [今日燈號] NVARCHAR(50)
                )
            END
        ''')
        print("   👉 檢查/建立 [strategy_performance] 成功")

        conn.commit()
        print("✅ 系統基礎建設已全部就緒！(重複執行也不會破壞資料)")

    except Exception as e:
        print(f"❌ 發生未知的錯誤: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_tsql_database()