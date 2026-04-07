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


def ensure_table(cursor, table_name, create_sql):
    cursor.execute(f"""
    IF OBJECT_ID(N'dbo.{table_name}', N'U') IS NULL
    BEGIN
        {create_sql}
    END
    """)


def ensure_column(cursor, table_name, col_name, col_sql_type):
    cursor.execute(
        f"IF COL_LENGTH('dbo.{table_name}', N'{col_name}') IS NULL "
        f"ALTER TABLE dbo.{table_name} ADD [{col_name}] {col_sql_type}"
    )


def ensure_core_tables(cursor):
    # =========================================================
    # 表 1：歷史交易總帳
    # =========================================================
    ensure_table(cursor, "trade_history", """
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

    trade_history_columns = {
        "策略名稱": "NVARCHAR(50) NULL",
        "Ticker SYMBOL": "VARCHAR(20) NOT NULL DEFAULT ''",
        "方向": "NVARCHAR(10) NULL",
        "進場時間": "DATETIME NULL",
        "出場時間": "DATETIME NULL",
        "進場價": "FLOAT NULL",
        "出場價": "FLOAT NULL",
        "報酬率(%)": "DECIMAL(10,3) NULL",
        "淨損益金額": "FLOAT NULL",
        "結餘本金": "FLOAT NULL",
        "市場狀態": "NVARCHAR(50) NULL",
        "進場陣型": "NVARCHAR(50) NULL",
        "期望值": "DECIMAL(10,3) NULL",
        "預期停損(%)": "DECIMAL(10,3) NULL",
        "預期停利(%)": "DECIMAL(10,3) NULL",
        "風報比(RR)": "DECIMAL(10,3) NULL",
        "風險金額": "FLOAT NULL",
    }
    for col, typ in trade_history_columns.items():
        ensure_column(cursor, "trade_history", col, typ)

    # =========================================================
    # 表 2：目前持倉工作區
    # =========================================================
    ensure_table(cursor, "active_positions", """
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

    active_positions_columns = {
        "Ticker SYMBOL": "VARCHAR(20) NOT NULL DEFAULT ''",
        "方向": "NVARCHAR(10) NULL",
        "進場時間": "DATETIME NULL",
        "進場價": "FLOAT NULL",
        "投入資金": "FLOAT NULL",
        "停利階段": "INT NULL",
        "進場股數": "INT NULL",
        "市場狀態": "NVARCHAR(50) NULL",
        "進場陣型": "NVARCHAR(50) NULL",
        "期望值": "DECIMAL(10,3) NULL",
        "預期停損(%)": "DECIMAL(10,3) NULL",
        "預期停利(%)": "DECIMAL(10,3) NULL",
        "風報比(RR)": "DECIMAL(10,3) NULL",
        "風險金額": "FLOAT NULL",
    }
    for col, typ in active_positions_columns.items():
        ensure_column(cursor, "active_positions", col, typ)

    # =========================================================
    # 表 3：每日法人籌碼庫
    # =========================================================
    ensure_table(cursor, "daily_chip_data", """
        CREATE TABLE dbo.daily_chip_data (
            [日期] DATE NOT NULL,
            [Ticker SYMBOL] NVARCHAR(20) NOT NULL,
            [外資買賣超] FLOAT NULL,
            [投信買賣超] FLOAT NULL,
            [自營商買賣超] FLOAT NULL,
            [三大法人合計] FLOAT NULL,
            [資料來源] NVARCHAR(20) NULL,
            [更新時間] DATETIME NULL,
            CONSTRAINT PK_daily_chip_data PRIMARY KEY ([日期], [Ticker SYMBOL])
        )
    """)

    chip_columns = {
        "日期": "DATE NULL",
        "Ticker SYMBOL": "NVARCHAR(20) NULL",
        "外資買賣超": "FLOAT NULL",
        "投信買賣超": "FLOAT NULL",
        "自營商買賣超": "FLOAT NULL",
        "三大法人合計": "FLOAT NULL",
        "資料來源": "NVARCHAR(20) NULL",
        "更新時間": "DATETIME NULL",
    }
    for col, typ in chip_columns.items():
        ensure_column(cursor, "daily_chip_data", col, typ)

    # =========================================================
    # 表 4：季財報資料庫
    # =========================================================
    ensure_table(cursor, "fundamentals_clean", """
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
            [更新時間] DATETIME NULL
        )
    """)

    fundamentals_columns = {
        "Ticker SYMBOL": "VARCHAR(20) NOT NULL DEFAULT ''",
        "資料年月日": "DATE NULL",
        "毛利率(%)": "DECIMAL(10,3) NULL",
        "營業利益率(%)": "DECIMAL(10,3) NULL",
        "單季EPS": "DECIMAL(10,3) NULL",
        "ROE(%)": "DECIMAL(10,3) NULL",
        "稅後淨利率(%)": "DECIMAL(10,3) NULL",
        "營業現金流": "FLOAT NULL",
        "預估殖利率(%)": "DECIMAL(10,3) NULL",
        "負債比率(%)": "DECIMAL(10,3) NULL",
        "本業獲利比(%)": "DECIMAL(10,3) NULL",
        "更新時間": "DATETIME NULL",
    }
    for col, typ in fundamentals_columns.items():
        ensure_column(cursor, "fundamentals_clean", col, typ)

    # =========================================================
    # 表 5：月營收資料庫
    # =========================================================
    ensure_table(cursor, "monthly_revenue_simple", """
        CREATE TABLE dbo.monthly_revenue_simple (
            [Ticker SYMBOL] NVARCHAR(20) NOT NULL,
            [公司名稱] NVARCHAR(100) NULL,
            [產業類別] NVARCHAR(20) NULL,
            [產業類別名稱] NVARCHAR(100) NULL,
            [資料年月日] DATE NOT NULL,
            [單月營收年增率(%)] DECIMAL(18,3) NULL,
            [更新時間] DATETIME NULL
        )
    """)

    revenue_columns = {
        "Ticker SYMBOL": "NVARCHAR(20) NOT NULL DEFAULT ''",
        "公司名稱": "NVARCHAR(100) NULL",
        "產業類別": "NVARCHAR(20) NULL",
        "產業類別名稱": "NVARCHAR(100) NULL",
        "資料年月日": "DATE NULL",
        "單月營收年增率(%)": "DECIMAL(18,3) NULL",
        "更新時間": "DATETIME NULL",
    }
    for col, typ in revenue_columns.items():
        ensure_column(cursor, "monthly_revenue_simple", col, typ)

    # =========================================================
    # 表 6：帳戶資訊
    # =========================================================
    ensure_table(cursor, "account_info", """
        CREATE TABLE dbo.account_info (
            [帳戶名稱] NVARCHAR(50) NOT NULL,
            [可用現金] FLOAT NULL,
            [最後更新時間] DATETIME NULL
        )
    """)

    account_columns = {
        "帳戶名稱": "NVARCHAR(50) NOT NULL DEFAULT ''",
        "可用現金": "FLOAT NULL",
        "最後更新時間": "DATETIME NULL",
    }
    for col, typ in account_columns.items():
        ensure_column(cursor, "account_info", col, typ)


def setup_tsql_database():
    print("========================================================")
    print("💣 【資料庫核彈級重置 / 補欄位升級雙模式】")
    print("⚠️ 可選擇全新建立，或僅補齊缺少欄位。")
    print("========================================================")

    mode = input("請選模式：1=全庫重置重建 | 2=只補欄位升級 (1/2): ").strip()

    conn = None
    try:
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

        if mode == "1":
            confirm = input("🚀 確定要執行「全庫刪除並升級新架構」嗎？(y/n): ").strip().lower()
            if confirm != "y":
                print("🛑 已安全取消作業。")
                return

            tables_to_drop = [
                "trade_history",
                "active_positions",
                "strategy_performance",
                "backtest_history",
                "account_info",
                "daily_chip_data",
                "fundamentals_clean",
                "monthly_revenue_simple",
            ]

            for table in tables_to_drop:
                cursor.execute(f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table}")

            print("🗑️ 舊資料表已刪除，準備重建...")

        print("🔧 開始檢查 / 建立核心資料表與欄位...")
        ensure_core_tables(cursor)
        conn.commit()

        print("✅ 資料表與欄位檢查完成！")
        print("   - 缺少的表已建立")
        print("   - 缺少的欄位已補齊")
        print("   - 之後各支程式可直接 INSERT / UPDATE")

    except Exception as e:
        print(f"❌ db_setup 發生錯誤：{e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    setup_tsql_database()
