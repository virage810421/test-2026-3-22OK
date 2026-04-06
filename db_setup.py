import pyodbc

MASTER_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=master;"
    r"Trusted_Connection=yes;"
)

TARGET_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)


def setup_tsql_database():
    print("========================================================")
    print("💣 【資料庫核彈級重置：升級機構級雙引擎架構】")
    print("⚠️ 警告：這將永久刪除指定舊資料表，並建立全新資料表！")
    print("========================================================")

    confirm = input("🚀 確定要執行「全庫刪除並升級新架構」嗎？(y/n): ").strip().lower()
    if confirm != "y":
        print("🛑 已安全取消作業。")
        return

    clear_chips = input("❓ 是否要一併刪除【法人籌碼庫 (daily_chip_data)】？(選 n 可保留歷史籌碼) (y/n): ").strip().lower()
    clear_funds = input("❓ 是否要一併刪除【基本面財報庫 (fundamentals_clean)】？(選 n 可保留歷史財報) (y/n): ").strip().lower()
    clear_rev_simple = input("❓ 是否要一併刪除【最新月營收簡表 (monthly_revenue_simple)】？(選 n 可保留) (y/n): ").strip().lower()
    clear_rev_industry = input("❓ 是否要一併刪除【產業月營收表 (stock_revenue_industry_tw)】？(選 n 可保留) (y/n): ").strip().lower()

    print("\n🔧 準備連線至 SQL Server 進行建置...")

    conn = None
    try:
        # 先確保資料庫存在
        with pyodbc.connect(MASTER_CONN_STR, autocommit=True) as master_conn:
            cursor = master_conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'股票online')
                BEGIN
                    CREATE DATABASE 股票online;
                END
            """)

        conn = pyodbc.connect(TARGET_CONN_STR)
        cursor = conn.cursor()

        # =========================================================
        # 💣 清理舊表
        # =========================================================
        tables_to_drop = [
            "trade_history",
            "active_positions",
            "strategy_performance",
            "backtest_history",
            "account_info",
        ]

        if clear_chips == "y":
            tables_to_drop.append("daily_chip_data")
        if clear_funds == "y":
            tables_to_drop.append("fundamentals_clean")
        if clear_rev_simple == "y":
            tables_to_drop.append("monthly_revenue_simple")
        if clear_rev_industry == "y":
            tables_to_drop.append("stock_revenue_industry_tw")

        for table in tables_to_drop:
            cursor.execute(f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}")

        print("🗑️ 指定之舊資料表已刪除完畢！(未選擇 Y 的歷史資料表已保留)\n")

        # =========================================================
        # 🏗️ 建立全新資料表
        # =========================================================
        print("🏗️ 開始建立核心資料表...")

        # --- 表 1：歷史交易總帳 ---
        cursor.execute("""
            CREATE TABLE dbo.trade_history (
                [策略名稱] NVARCHAR(50) NULL,
                [Ticker SYMBOL] VARCHAR(20) NOT NULL,
                [方向] NVARCHAR(10) NULL,
                [進場時間] DATETIME NULL,
                [出場時間] DATETIME NULL,
                [進場價] FLOAT NULL,
                [出場價] FLOAT NULL,
                [報酬率(%)] DECIMAL(10,3) NULL,
                [淨損益金額] FLOAT NULL,
                [結餘本金] FLOAT NULL,
                [市場狀態] NVARCHAR(50) NULL,
                [進場陣型] NVARCHAR(50) NULL,
                [期望值] DECIMAL(10,3) NULL,
                [預期停損(%)] DECIMAL(10,3) NULL,
                [預期停利(%)] DECIMAL(10,3) NULL,
                [風報比(RR)] DECIMAL(10,3) NULL,
                [風險金額] FLOAT NULL
            )
        """)

        # --- 表 2：目前持倉工作區 ---
        cursor.execute("""
            CREATE TABLE dbo.active_positions (
                [Ticker SYMBOL] VARCHAR(20) NOT NULL,
                [方向] NVARCHAR(10) NULL,
                [進場時間] DATETIME NULL,
                [進場價] FLOAT NULL,
                [投入資金] FLOAT NULL,
                [停利階段] INT NULL,
                [進場股數] INT NULL,
                [市場狀態] NVARCHAR(50) NULL,
                [進場陣型] NVARCHAR(50) NULL,
                [期望值] DECIMAL(10,3) NULL,
                [預期停損(%)] DECIMAL(10,3) NULL,
                [預期停利(%)] DECIMAL(10,3) NULL,
                [風報比(RR)] DECIMAL(10,3) NULL,
                [風險金額] FLOAT NULL
            )
        """)

        # --- 表 3：每日法人籌碼庫 ---
        cursor.execute("""
            IF OBJECT_ID(N'dbo.daily_chip_data', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.daily_chip_data (
                    [日期] DATE NOT NULL,
                    [Ticker SYMBOL] VARCHAR(20) NOT NULL,
                    [外資買賣超] FLOAT NULL,
                    [投信買賣超] FLOAT NULL,
                    [自營商買賣超] FLOAT NULL,
                    [三大法人合計] FLOAT NULL,
                    CONSTRAINT PK_daily_chip_data PRIMARY KEY ([日期], [Ticker SYMBOL])
                )
            END
        """)

        # --- 表 4：策略績效追蹤表 ---
        cursor.execute("""
            CREATE TABLE dbo.strategy_performance (
                [Ticker SYMBOL] VARCHAR(20) NULL,
                [紀錄時間] DATETIME NULL,
                [系統勝率(%)] DECIMAL(10,3) NULL,
                [累計報酬率(%)] DECIMAL(10,3) NULL,
                [今日燈號] NVARCHAR(50) NULL,
                [期望值] DECIMAL(10,3) NULL
            )
        """)

        # --- 表 5：大腦回測明細 ---
        cursor.execute("""
            CREATE TABLE dbo.backtest_history (
                [策略名稱] NVARCHAR(50) NULL,
                [Ticker SYMBOL] VARCHAR(20) NULL,
                [方向] NVARCHAR(10) NULL,
                [進場時間] DATETIME NULL,
                [出場時間] DATETIME NULL,
                [進場價] FLOAT NULL,
                [出場價] FLOAT NULL,
                [報酬率(%)] DECIMAL(10,3) NULL,
                [淨損益金額] FLOAT NULL,
                [結餘本金] FLOAT NULL,
                [市場狀態] NVARCHAR(50) NULL,
                [進場陣型] NVARCHAR(50) NULL,
                [期望值] DECIMAL(10,3) NULL,
                [預期停損(%)] DECIMAL(10,3) NULL,
                [預期停利(%)] DECIMAL(10,3) NULL,
                [風報比(RR)] DECIMAL(10,3) NULL,
                [風險金額] FLOAT NULL
            )
        """)

        # --- 表 6：帳戶資訊 ---
        cursor.execute("""
            CREATE TABLE dbo.account_info (
                [帳戶名稱] NVARCHAR(50) PRIMARY KEY,
                [可用現金] FLOAT NULL,
                [最後更新時間] DATETIME NULL
            )
        """)

        cursor.execute("""
            IF NOT EXISTS (
                SELECT 1 FROM dbo.account_info WHERE [帳戶名稱] = N'我的實戰帳戶'
            )
            BEGIN
                INSERT INTO dbo.account_info ([帳戶名稱], [可用現金], [最後更新時間])
                VALUES (N'我的實戰帳戶', 50000000, GETDATE())
            END
        """)

        # --- 表 7：基本面資料庫 ---
        cursor.execute("""
            IF OBJECT_ID(N'dbo.fundamentals_clean', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.fundamentals_clean (
                    [Ticker SYMBOL] VARCHAR(20) NOT NULL,
                    [資料年月日] DATE NOT NULL,
                    [毛利率(%)] DECIMAL(10,3) NULL,
                    [營業利益率(%)] DECIMAL(10,3) NULL,
                    [單季EPS] DECIMAL(10,3) NULL,
                    [ROE(%)] DECIMAL(10,3) NULL,
                    [稅後淨利率(%)] DECIMAL(10,3) NULL,
                    [營業現金流] FLOAT NULL,
                    [預估殖利率(%)] DECIMAL(10,3) NULL,
                    [負債比率(%)] DECIMAL(10,3) NULL,
                    [本業獲利比(%)] DECIMAL(10,3) NULL,
                    [更新時間] DATETIME DEFAULT GETDATE(),
                    CONSTRAINT PK_fundamentals_clean PRIMARY KEY ([Ticker SYMBOL], [資料年月日])
                )
            END
        """)

        # --- 表 8：最新月營收簡表 ---
        cursor.execute("""
            IF OBJECT_ID(N'dbo.monthly_revenue_simple', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.monthly_revenue_simple (
                    [Ticker SYMBOL] VARCHAR(20) NOT NULL,
                    [公司名稱] NVARCHAR(100) NULL,
                    [產業類別] NVARCHAR(100) NULL,
                    [資料年月日] DATE NOT NULL,
                    [單月營收年增率(%)] DECIMAL(18,3) NULL,
                    CONSTRAINT PK_monthly_revenue_simple PRIMARY KEY ([Ticker SYMBOL], [資料年月日])
                )
            END
        """)

        # --- 表 9：產業月營收表 ---
        cursor.execute("""
            IF OBJECT_ID(N'dbo.stock_revenue_industry_tw', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.stock_revenue_industry_tw (
                    [Ticker SYMBOL] NVARCHAR(20) NOT NULL,
                    [公司名稱] NVARCHAR(100) NULL,
                    [產業類別] NVARCHAR(100) NULL,
                    [產業類別名稱] NVARCHAR(100) NULL,
                    [資料年月日] DATE NOT NULL,
                    [單月營收年增率(%)] DECIMAL(18,3) NULL,
                    CONSTRAINT PK_stock_revenue_industry_tw PRIMARY KEY ([Ticker SYMBOL], [資料年月日])
                )
            END
        """)

        conn.commit()
        print("\n✅ 資料庫擴充升級完畢！底層風控容器與基本面貨架已全部準備就緒。")

    except Exception as e:
        print(f"\n❌ 發生未知的錯誤: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    setup_tsql_database()
