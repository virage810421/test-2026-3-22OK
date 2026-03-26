import pyodbc

def create_tsql_database():
    print("🔧 準備連線至 SQL Server...")
    
    # ==========================================
    # 🔌 設定 SQL Server 連線字串
    # ==========================================
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=localhost;'  # 例如: localhost 或 LAPTOP-XYZ\SQLEXPRESS
        r'DATABASE=股票online;'
        r'Trusted_Connection=yes;'
    )

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 💡 這裡是你未來打地基 (建立 Table) 的地方
        # 使用 IF NOT EXISTS 防呆：如果表已經存在，就不會重複建立
        
        # 建立：歷史交易總帳 (trade_history)
        cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='trade_history' AND xtype='U')
            CREATE TABLE trade_history (
                [Ticker SYMBOL] VARCHAR(20),
                [方向] NVARCHAR(10),
                [進場時間] DATETIME,
                [出場時間] DATETIME,
                [進場價] FLOAT,
                [出場價] FLOAT,
                [報酬率(%)] FLOAT
            )
        ''')
        
        # 建立：目前持倉工作區 (active_positions)
        cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='active_positions' AND xtype='U')
            CREATE TABLE active_positions (
                [Ticker SYMBOL] VARCHAR(20) PRIMARY KEY,
                [方向] NVARCHAR(10),
                [進場時間] DATETIME,
                [進場價] FLOAT
            )
        ''')

        conn.commit()
        print("✅ 交易資料庫與表單 (trade_history, active_positions) 建立完成！")

    except Exception as e:
        print(f"❌ 連線或建立表單失敗: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    create_tsql_database()