import pyodbc
import sys

def setup_tsql_database():
    print("========================================================")
    print("💣 【資料庫核彈級重置：升級機構級歸因日誌】")
    print("⚠️ 警告：這將永久刪除所有舊資料表，並建立包含全新風控欄位的新表！")
    print("========================================================")
    
    confirm = input("🚀 確定要執行「全庫刪除並升級新架構」嗎？(y/n): ")
    if confirm.lower() != 'y':
        print("🛑 已安全取消作業。")
        return

    # 🌟 新增：詢問是否刪除籌碼資料
    clear_chips = input("❓ 是否要一併刪除【法人籌碼庫 (daily_chip_data)】？(選 n 可保留已下載的歷史籌碼) (y/n): ")
    # 🌟 再新增：詢問是否刪除基本面資料
    clear_funds = input("❓ 是否要一併刪除【基本面財報庫 (fundamental_data)】？(選 n 可保留已下載的財報) (y/n): ")
    print("\n🔧 準備連線至 SQL Server 進行建置...")
    
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

        cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '股票online')
            BEGIN
                CREATE DATABASE 股票online;
            END
        ''')

        target_conn_str = master_conn_str.replace('DATABASE=master;', 'DATABASE=股票online;')
        conn = pyodbc.connect(target_conn_str)
        cursor = conn.cursor()

        # ==========================================
        # 💣 清理舊表
        # ==========================================
        tables_to_drop = [
            'trade_history', 'active_positions', 
            'strategy_performance', 'backtest_history', 'account_info'
        ]
        
        # 💡 根據使用者的選擇，決定是否加入刪除名單
        if clear_chips.lower() == 'y':
            tables_to_drop.append('daily_chip_data')
        if clear_funds.lower() == 'y':  # 🌟 這裡判斷基本面
            tables_to_drop.append('fundamental_data')
            
        for table in tables_to_drop:
            cursor.execute(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}")
            
        print("🗑️ 交易與帳戶舊資料表已刪除！(未選擇 Y 的歷史數據庫已為您安全保留)\n")

        # ==========================================
        # 🏗️ 建立全新資料表 (包含 ✨ 歸因擴充欄位)
        # ==========================================
        print("🏗️ 開始建立 6 大核心資料表 (機構級擴充版)...")

        # --- 表單 1：歷史交易總帳 (實戰版) ---
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
                [結餘本金] FLOAT,
                -- ✨ 機構級歸因欄位 ✨
                [市場狀態] NVARCHAR(50),
                [進場陣型] NVARCHAR(50),
                [期望值] DECIMAL(10,3),
                [預期停損(%)] DECIMAL(10,3),
                [預期停利(%)] DECIMAL(10,3),
                [風報比(RR)] DECIMAL(10,3),
                [風險金額] FLOAT
            )
        ''')
        print("   👉 建立 [trade_history] 成功 (已擴充 7 個歸因欄位)")

        # --- 表單 2：目前持倉工作區 ---
        # 為了能在平倉時寫入日誌，持倉表也要記住當初進場的理由
        cursor.execute('''
            CREATE TABLE active_positions (
                [Ticker SYMBOL] VARCHAR(20),
                [方向] NVARCHAR(10),
                [進場時間] DATETIME,
                [進場價] FLOAT,
                [投入資金] FLOAT,
                [停利階段] INT,
                [進場股數] INT,
                -- ✨ 持倉快取記憶 ✨
                [市場狀態] NVARCHAR(50),
                [進場陣型] NVARCHAR(50),
                [期望值] DECIMAL(10,3),
                [預期停損(%)] DECIMAL(10,3),
                [預期停利(%)] DECIMAL(10,3),
                [風報比(RR)] DECIMAL(10,3),
                [風險金額] FLOAT
            )
        ''')
        print("   👉 建立 [active_positions] 成功")

        # --- 表單 3：每日法人籌碼庫 ---
        # 💡 加入防護：如果使用者選擇保留舊表 (表已存在)，這裡不會報錯覆蓋
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
        print("   👉 建立/確認 [daily_chip_data] 成功")

        # --- 表單 4：策略績效追蹤表 ---
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

        # --- 表單 5：大腦回測明細 ---
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
                [結餘本金] FLOAT,
                -- ✨ 機構級歸因欄位 ✨
                [市場狀態] NVARCHAR(50),
                [進場陣型] NVARCHAR(50),
                [期望值] DECIMAL(10,3),
                [預期停損(%)] DECIMAL(10,3),
                [預期停利(%)] DECIMAL(10,3),
                [風報比(RR)] DECIMAL(10,3),
                [風險金額] FLOAT
            )
        ''')
        print("   👉 建立 [backtest_history] 成功 (已擴充 7 個歸因欄位)")

        # --- 表單 6：帳戶資訊 ---
        cursor.execute('''
            CREATE TABLE account_info (
                [帳戶名稱] NVARCHAR(50) PRIMARY KEY,
                [可用現金] FLOAT,
                [最後更新時間] DATETIME
            )
        ''')
        
        # 自動給予預設資金
        cursor.execute("INSERT INTO account_info ([帳戶名稱], [可用現金], [最後更新時間]) VALUES ('我的實戰帳戶', 50000000, GETDATE())")
        print("   👉 建立 [account_info] 成功 (已注入預設資金 500,000,000)")
        # --- 表單 7：基本面資料庫 ---
        cursor.execute('''
            IF OBJECT_ID('fundamental_data', 'U') IS NULL
            BEGIN
                CREATE TABLE fundamental_data (
                    [Ticker SYMBOL] VARCHAR(20),
                    [資料年月] VARCHAR(10),  
                    [單月營收年增率(%)] DECIMAL(10,2),
                    [毛利率(%)] DECIMAL(10,2),
                    [營業利益率(%)] DECIMAL(10,2),
                    [單季EPS] DECIMAL(10,2),
                    [ROE(%)] DECIMAL(10,2),
                    [營業現金流] FLOAT,
                    [殖利率(%)] DECIMAL(10,2),
                    [更新時間] DATETIME DEFAULT GETDATE(),
                    PRIMARY KEY ([Ticker SYMBOL], [資料年月])
                )
            END
        ''')
        print("   👉 建立/確認 [fundamental_data] 成功 (量化雙引擎核心)")

        conn.commit()

    except Exception as e:
        print(f"\n❌ 發生未知的錯誤: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_tsql_database()