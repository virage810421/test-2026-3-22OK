import pyodbc

def update_tsql_database():
    print("🔧 準備連線至 SQL Server 進行資料表升級...")
    
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=localhost;'  
        r'DATABASE=股票online;' 
        r'Trusted_Connection=yes;'
    )

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        print("🗑️ 步驟 1: 刪除舊的 active_positions 表單...")
        # 刪除舊的表單 (警告：這會清空你目前的未平倉紀錄！)
        cursor.execute("IF OBJECT_ID('active_positions', 'U') IS NOT NULL DROP TABLE active_positions;")
        conn.commit()

        print("🏗️ 步驟 2: 建立支援「分批進場」的新 active_positions 表單...")
        # 建立新的表單
        cursor.execute('''
            CREATE TABLE active_positions (
                -- 🌟 關鍵修改 1：新增 Trade_ID 作為自動遞增的流水號主鍵
                [Trade_ID] INT IDENTITY(1,1) PRIMARY KEY, 
                
                -- Ticker 不再是 Primary Key，允許重複！
                [Ticker SYMBOL] VARCHAR(20), 
                [方向] NVARCHAR(10),
                [進場時間] DATETIME,
                [進場價] FLOAT,
                
                -- 🌟 關鍵修改 2：新增資金欄位，方便未來計算平均成本與加碼
                [投入資金] FLOAT 
            )
        ''')
        conn.commit()
        print("✅ 升級完成！資料庫現在允許同一檔股票多次進場了！")

    except Exception as e:
        print(f"❌ 發生錯誤: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    update_tsql_database()