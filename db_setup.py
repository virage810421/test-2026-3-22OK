import os
import sys
import pyodbc
import re

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")

def _assert_safe_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(str(name)):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return str(name)

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


# =========================================================
# 工具函式
# =========================================================
def safe_print(msg):
    text = str(msg)
    try:
        print(text)
    except UnicodeEncodeError:
        enc = getattr(sys.stdout, 'encoding', None) or 'utf-8'
        print(text.encode(enc, errors='replace').decode(enc, errors='replace'))


def log(msg):
    safe_print(msg)


def get_arg_value(flag_name: str, default=None):
    for i, arg in enumerate(sys.argv):
        if arg == flag_name and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def has_flag(flag_name: str) -> bool:
    return flag_name in sys.argv


def env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y", "on")


def ensure_table(cursor, table_name, create_sql):
    table_name = _assert_safe_identifier(table_name)
    cursor.execute(f"""
    IF OBJECT_ID(N'dbo.{table_name}', N'U') IS NULL
    BEGIN
        {create_sql}
    END
    """)


def ensure_column(cursor, table_name, col_name, col_sql_type):
    table_name = _assert_safe_identifier(table_name)
    cursor.execute(
        f"IF COL_LENGTH('dbo.{table_name}', N'{col_name}') IS NULL "
        f"ALTER TABLE dbo.{table_name} ADD [{col_name}] {col_sql_type}"
    )


# =========================================================
# 核心 schema 管理
# =========================================================
def ensure_core_tables(cursor):
    # -----------------------------------------------------
    # trade_history
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # active_positions
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # daily_chip_data
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # fundamentals_clean
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # monthly_revenue_simple
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # account_info
    # -----------------------------------------------------
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


# =========================================================
# 操作模式
# =========================================================
def ensure_database_exists():
    with pyodbc.connect(MASTER_CONN_STR, autocommit=True) as master_conn:
        cursor = master_conn.cursor()
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'股票online')
            BEGIN
                CREATE DATABASE 股票online;
            END
        """)


def drop_known_tables(cursor):
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


def parse_mode():
    """
    支援 3 種方式指定模式：
    1. 命令列：
       python db_setup.py --mode upgrade
       python db_setup.py --mode reset --yes
    2. 環境變數：
       DB_SETUP_MODE=upgrade
       DB_SETUP_MODE=reset
       DB_SETUP_CONFIRM_RESET=true
    3. 無參數預設：
       upgrade（安全模式）
    """
    cli_mode = get_arg_value("--mode")
    env_mode = os.getenv("DB_SETUP_MODE")
    mode = (cli_mode or env_mode or "upgrade").strip().lower()

    if mode not in ("upgrade", "reset"):
        raise ValueError("mode 只能是 upgrade 或 reset")

    auto_yes = has_flag("--yes") or env_bool("DB_SETUP_CONFIRM_RESET", False)
    return mode, auto_yes


def setup_tsql_database():
    log("========================================================")
    log("🛠️ db_setup 自動化版本啟動")
    log("模式支援：upgrade / reset")
    log("預設：upgrade（安全補欄模式）")
    log("========================================================")

    mode, auto_yes = parse_mode()
    conn = None

    try:
        ensure_database_exists()

        conn = pyodbc.connect(TARGET_CONN_STR)
        cursor = conn.cursor()

        if mode == "reset":
            if not auto_yes:
                raise RuntimeError(
                    "reset 模式需要明確確認。請加 --yes 或設定 DB_SETUP_CONFIRM_RESET=true"
                )
            log("🚨 reset 模式啟動：準備刪除既有核心資料表...")
            drop_known_tables(cursor)
            log("🗑️ 舊資料表已刪除")

        log("🔧 開始檢查 / 建立核心資料表與欄位...")
        ensure_core_tables(cursor)
        conn.commit()

        log("✅ 資料表與欄位檢查完成")
        log(f"   模式：{mode}")
        if mode == "upgrade":
            log("   動作：保留資料 + 補齊缺表 / 缺欄")
        else:
            log("   動作：重建核心表 + 補齊欄位")

    except Exception as e:
        log(f"❌ db_setup 發生錯誤：{e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    setup_tsql_database()
