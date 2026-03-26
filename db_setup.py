import pyodbc

def create_tsql_database():
    print("🔧 準備連線至 SQL Server...")
    
    # ==========================================
    # 🔌 第一階段：連線至 master 建立空地 (Database)
    # ==========================================
    master_conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=localhost;'  
        r'DATABASE=master;'  # 👈 關鍵 1：先連線到系統預設空地
        r'Trusted_Connection=yes;'
    )

    conn = None
    try:
        # 👈 關鍵 2：建立 DB 必須開啟 autocommit=True
        conn = pyodbc.connect(master_conn_str, autocommit=True)
        cursor = conn.cursor()

        # 檢查並建立資料庫
        cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '股票online')
            BEGIN
                CREATE DATABASE 股票online;
            END
        ''')
        print("✅ 資料庫 [股票online] 確認/建立完成！")
        
        # 斷開 master，準備切換
        conn.close()

        # ==========================================
        # 🔌 第二階段：切換至專屬資料庫蓋房子 (Tables)
        # ==========================================
        target_conn_str = master_conn_str.replace('DATABASE=master;', 'DATABASE=股票online;')
        conn = pyodbc.connect(target_conn_str)
        cursor = conn.cursor()

        # 建立：歷史交易總帳 (trade_history)
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
                    [報酬率(%)] DECIMAL(10,3) -- 👈 關鍵 3：在底層嚴格鎖死 3 位小數
                )
            END
        ''')
        
        # 建立：目前持倉工作區 (active_positions)
        cursor.execute('''
            IF OBJECT_ID('active_positions', 'U') IS NULL
            BEGIN
                CREATE TABLE active_positions (
                    [Ticker SYMBOL] VARCHAR(20) PRIMARY KEY,
                    [方向] NVARCHAR(10),
                    [進場時間] DATETIME,
                    [進場價] FLOAT
                )
            END
        ''')

        conn.commit()
        print("✅ 交易資料庫與表單 (trade_history, active_positions) 建立完成！")

    except pyodbc.Error as e:
        print(f"❌ SQL Server 執行失敗: {e}")
    except Exception as e:
        print(f"❌ 發生未知的錯誤: {e}")
    finally:
        # 確保無論如何都會安全關閉連線
        if conn:
            conn.close()

if __name__ == "__main__":
    create_tsql_database()