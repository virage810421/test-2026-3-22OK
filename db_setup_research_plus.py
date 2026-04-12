# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import pyodbc

TARGET_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)


def safe_print(msg):
    text = str(msg)
    try:
        print(text)
    except UnicodeEncodeError:
        enc = getattr(sys.stdout, 'encoding', None) or 'utf-8'
        print(text.encode(enc, errors='replace').decode(enc, errors='replace'))


def ensure_table(cursor, table_name, create_sql):
    cursor.execute(f"""
    IF OBJECT_ID(N'dbo.{table_name}', N'U') IS NULL
    BEGIN
        {create_sql}
    END
    """)


def _safe_exec(cursor, sql: str):
    try:
        cursor.execute(sql)
    except Exception:
        pass


def _rename_column_if_exists(cursor, table_name: str, old_col: str, new_col: str):
    escaped_old = str(old_col).replace("'", "''")
    escaped_new = str(new_col).replace("'", "''")
    escaped_table = str(table_name).replace("'", "''")
    sql = f"""
    IF OBJECT_ID(N'dbo.{escaped_table}', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.{escaped_table}', N'{escaped_old}') IS NOT NULL
       AND COL_LENGTH(N'dbo.{escaped_table}', N'{escaped_new}') IS NULL
    BEGIN
        EXEC sp_rename N'dbo.{escaped_table}.[{old_col}]', N'{new_col}', 'COLUMN';
    END
    """
    _safe_exec(cursor, sql)


def ensure_training_universe_daily_chinese_columns(cursor):
    rename_pairs = [
        ('Ticker SYMBOL', '股票代號'),
        ('Liquidity_Score', '流動性分數'),
        ('Chip_Score', '籌碼分數'),
        ('Fundamental_Score', '基本面分數'),
        ('Revenue_Momentum_Score', '營收動能分數'),
        ('Risk_Penalty', '風險扣分'),
        ('Tradability_Flag', '可交易旗標'),
        ('Training_Eligible_Flag', '可訓練旗標'),
        ('Training_Tier', '訓練分層'),
        ('Exclude_Reason', '排除原因'),
        ('Universe_Score', '訓練母池總分'),
        ('ADV20', '二十日平均成交額'),
        ('ATR_Pct', 'ATR百分比'),
        ('ROE(%)', '股東權益報酬率(%)'),
        ('SectorBucket', '產業分桶'),
    ]
    for old_col, new_col in rename_pairs:
        _rename_column_if_exists(cursor, 'training_universe_daily', old_col, new_col)


def ensure_research_plus_tables(cursor):
    ensure_table(cursor, 'feature_cross_section_snapshot', """
        CREATE TABLE dbo.feature_cross_section_snapshot (
            [建立時間] DATETIME NOT NULL DEFAULT GETDATE(),
            [股票代號] NVARCHAR(32) NOT NULL,
            [完整代號] NVARCHAR(32) NULL,
            [產業名稱] NVARCHAR(64) NULL,
            [收盤價] DECIMAL(28,6) NULL,
            [二十日報酬] DECIMAL(28,10) NULL,
            [相對大盤二十日強弱] DECIMAL(28,10) NULL,
            [相對產業二十日強弱] DECIMAL(28,10) NULL,
            [ATR百分比] DECIMAL(28,10) NULL,
            [二十日實現波動率] DECIMAL(28,10) NULL,
            [成交額代理] DECIMAL(38,6) NULL,
            [二十日平均成交額代理] DECIMAL(38,6) NULL,
            [營收年增率] DECIMAL(28,10) NULL,
            [籌碼總比率] DECIMAL(28,10) NULL,
            [相對大盤分位數] DECIMAL(18,10) NULL,
            [相對產業分位數] DECIMAL(18,10) NULL,
            [營收年增率分位數] DECIMAL(18,10) NULL,
            [籌碼總比率分位數] DECIMAL(18,10) NULL,
            [成交額分位數] DECIMAL(18,10) NULL,
            [平均成交額分位數] DECIMAL(18,10) NULL,
            [ATR百分比分位數] DECIMAL(18,10) NULL,
            [實現波動率分位數] DECIMAL(18,10) NULL,
            CONSTRAINT PK_feature_cross_section_snapshot PRIMARY KEY ([建立時間], [股票代號])
        )
    """)
    ensure_table(cursor, 'feature_event_calendar', """
        CREATE TABLE dbo.feature_event_calendar (
            [股票代號] NVARCHAR(32) NOT NULL,
            [事件日期] DATE NOT NULL,
            [事件類型] NVARCHAR(32) NOT NULL,
            [來源檔名] NVARCHAR(260) NULL,
            [建立時間] DATETIME NOT NULL DEFAULT GETDATE(),
            CONSTRAINT PK_feature_event_calendar PRIMARY KEY ([股票代號], [事件日期], [事件類型])
        )
    """)
    ensure_table(cursor, 'live_feature_mount', """
        CREATE TABLE dbo.live_feature_mount (
            [掛載時間] DATETIME NOT NULL,
            [股票代號] NVARCHAR(32) NOT NULL,
            [特徵名稱] NVARCHAR(128) NOT NULL,
            [特徵值] DECIMAL(38,10) NULL,
            [來源] NVARCHAR(64) NULL,
            CONSTRAINT PK_live_feature_mount PRIMARY KEY ([掛載時間], [股票代號], [特徵名稱])
        )
    """)
    ensure_table(cursor, 'training_feature_registry', """
        CREATE TABLE dbo.training_feature_registry (
            [建立時間] DATETIME NOT NULL DEFAULT GETDATE(),
            [特徵名稱] NVARCHAR(128) NOT NULL,
            [特徵桶] NVARCHAR(64) NULL,
            [是否Percentile驅動] BIT NULL,
            [是否事件窗特徵] BIT NULL,
            [是否實戰啟用] BIT NULL,
            [是否訓練啟用] BIT NULL,
            CONSTRAINT PK_training_feature_registry PRIMARY KEY ([建立時間], [特徵名稱])
        )
    """)
    ensure_table(cursor, 'task_completion_registry', """
        CREATE TABLE dbo.task_completion_registry (
            [任務名稱] NVARCHAR(128) NOT NULL,
            [任務分類] NVARCHAR(64) NULL,
            [完成狀態] NVARCHAR(32) NULL,
            [說明] NVARCHAR(MAX) NULL,
            [更新時間] DATETIME NOT NULL DEFAULT GETDATE(),
            CONSTRAINT PK_task_completion_registry PRIMARY KEY ([任務名稱])
        )
    """)
    ensure_table(cursor, 'integration_run_registry', """
        CREATE TABLE dbo.integration_run_registry (
            [執行時間] DATETIME NOT NULL,
            [模組名稱] NVARCHAR(128) NOT NULL,
            [狀態] NVARCHAR(32) NULL,
            [輸出路徑] NVARCHAR(260) NULL,
            [備註] NVARCHAR(MAX) NULL,
            CONSTRAINT PK_integration_run_registry PRIMARY KEY ([執行時間], [模組名稱])
        )
    """)
    ensure_table(cursor, 'system_guard_log', """
        CREATE TABLE dbo.system_guard_log (
            [記錄時間] DATETIME NOT NULL,
            [健康狀態] NVARCHAR(32) NULL,
            [警告數] INT NULL,
            [錯誤數] INT NULL,
            [內容] NVARCHAR(MAX) NULL,
            CONSTRAINT PK_system_guard_log PRIMARY KEY ([記錄時間])
        )
    """)

    ensure_table(cursor, 'stock_master', """
        CREATE TABLE dbo.stock_master (
            [Ticker SYMBOL] NVARCHAR(32) NOT NULL,
            [公司名稱] NVARCHAR(128) NULL,
            [市場別] NVARCHAR(16) NULL,
            [產業類別] NVARCHAR(64) NULL,
            [產業類別名稱] NVARCHAR(64) NULL,
            [是否停牌] BIT NULL,
            [是否下市] BIT NULL,
            [是否ETF] BIT NULL,
            [是否普通股] BIT NULL,
            [SectorBucket] NVARCHAR(32) NULL,
            [來源] NVARCHAR(64) NULL,
            [更新時間] DATETIME NOT NULL DEFAULT GETDATE(),
            CONSTRAINT PK_stock_master PRIMARY KEY ([Ticker SYMBOL])
        )
    """)
    ensure_table(cursor, 'company_quality_snapshot', """
        CREATE TABLE dbo.company_quality_snapshot (
            [Ticker SYMBOL] NVARCHAR(32) NOT NULL,
            [資料日期] DATE NOT NULL,
            [單月營收年增率(%)] DECIMAL(18,6) NULL,
            [毛利率(%)] DECIMAL(18,6) NULL,
            [營業利益率(%)] DECIMAL(18,6) NULL,
            [單季EPS] DECIMAL(18,6) NULL,
            [ROE(%)] DECIMAL(18,6) NULL,
            [稅後淨利率(%)] DECIMAL(18,6) NULL,
            [負債比率(%)] DECIMAL(18,6) NULL,
            [本業獲利比(%)] DECIMAL(18,6) NULL,
            [預估殖利率(%)] DECIMAL(18,6) NULL,
            [Revenue_Growth_Score] DECIMAL(18,6) NULL,
            [Profitability_Score] DECIMAL(18,6) NULL,
            [BalanceSheet_Score] DECIMAL(18,6) NULL,
            [Dividend_Score] DECIMAL(18,6) NULL,
            [Quality_Total_Score] DECIMAL(18,6) NULL,
            [資料來源] NVARCHAR(64) NULL,
            CONSTRAINT PK_company_quality_snapshot PRIMARY KEY ([Ticker SYMBOL], [資料日期])
        )
    """)
    ensure_table(cursor, 'revenue_momentum_snapshot', """
        CREATE TABLE dbo.revenue_momentum_snapshot (
            [Ticker SYMBOL] NVARCHAR(32) NOT NULL,
            [資料年月] NVARCHAR(16) NOT NULL,
            [單月營收年增率(%)] DECIMAL(18,6) NULL,
            [三月平均年增(%)] DECIMAL(18,6) NULL,
            [六月平均年增(%)] DECIMAL(18,6) NULL,
            [營收加速度] DECIMAL(18,6) NULL,
            [是否連續三月正成長] BIT NULL,
            [營收動能分數] DECIMAL(18,6) NULL,
            CONSTRAINT PK_revenue_momentum_snapshot PRIMARY KEY ([Ticker SYMBOL], [資料年月])
        )
    """)
    ensure_table(cursor, 'price_liquidity_daily', """
        CREATE TABLE dbo.price_liquidity_daily (
            [Ticker SYMBOL] NVARCHAR(32) NOT NULL,
            [資料日期] DATE NOT NULL,
            [Close] DECIMAL(18,6) NULL,
            [Volume] DECIMAL(38,6) NULL,
            [Amount] DECIMAL(38,6) NULL,
            [ADV20] DECIMAL(38,6) NULL,
            [Turnover_Ratio] DECIMAL(18,6) NULL,
            [ATR_Pct] DECIMAL(18,6) NULL,
            [近20日缺資料天數] INT NULL,
            [是否異常波動] BIT NULL,
            [是否連續無量] BIT NULL,
            [Liquidity_Score] DECIMAL(18,6) NULL,
            CONSTRAINT PK_price_liquidity_daily PRIMARY KEY ([Ticker SYMBOL], [資料日期])
        )
    """)
    ensure_table(cursor, 'chip_factors_daily', """
        CREATE TABLE dbo.chip_factors_daily (
            [Ticker SYMBOL] NVARCHAR(32) NOT NULL,
            [資料日期] DATE NOT NULL,
            [外資買賣超] DECIMAL(38,6) NULL,
            [投信買賣超] DECIMAL(38,6) NULL,
            [自營商買賣超] DECIMAL(38,6) NULL,
            [三大法人合計] DECIMAL(38,6) NULL,
            [籌碼集中度] DECIMAL(18,6) NULL,
            [大戶散戶差] DECIMAL(18,6) NULL,
            [Chip_Score] DECIMAL(18,6) NULL,
            CONSTRAINT PK_chip_factors_daily PRIMARY KEY ([Ticker SYMBOL], [資料日期])
        )
    """)
    ensure_table(cursor, 'training_universe_daily', """
        CREATE TABLE dbo.training_universe_daily (
            [股票代號] NVARCHAR(32) NOT NULL,
            [資料日期] DATE NOT NULL,
            [產業類別] NVARCHAR(32) NULL,
            [產業類別名稱] NVARCHAR(64) NULL,
            [流動性分數] DECIMAL(18,6) NULL,
            [籌碼分數] DECIMAL(18,6) NULL,
            [基本面分數] DECIMAL(18,6) NULL,
            [營收動能分數] DECIMAL(18,6) NULL,
            [風險扣分] DECIMAL(18,6) NULL,
            [可交易旗標] BIT NULL,
            [可訓練旗標] BIT NULL,
            [訓練分層] NVARCHAR(32) NULL,
            [排除原因] NVARCHAR(512) NULL,
            [訓練母池總分] DECIMAL(18,6) NULL,
            [資料完整率] DECIMAL(18,6) NULL,
            [二十日平均成交額] DECIMAL(38,6) NULL,
            [ATR百分比] DECIMAL(18,6) NULL,
            [股東權益報酬率(%)] DECIMAL(18,6) NULL,
            [負債比率(%)] DECIMAL(18,6) NULL,
            [單月營收年增率(%)] DECIMAL(18,6) NULL,
            [產業分桶] NVARCHAR(32) NULL,
            CONSTRAINT PK_training_universe_daily PRIMARY KEY ([股票代號], [資料日期])
        )
    """)

    ensure_table(cursor, 'risk_gateway_log', """
        CREATE TABLE dbo.risk_gateway_log (
            [id] BIGINT IDENTITY(1,1) NOT NULL,
            [記錄時間] DATETIME NOT NULL,
            [股票代號] NVARCHAR(32) NULL,
            [是否通過] BIT NULL,
            [阻擋原因] NVARCHAR(MAX) NULL,
            [策略名稱] NVARCHAR(128) NULL,
            CONSTRAINT PK_risk_gateway_log PRIMARY KEY ([id])
        )
    """)

    # 既有表也一併升級精度，避免 float -> numeric 溢位
    alter_sqls = [
        "IF OBJECT_ID(N'dbo.live_feature_mount', N'U') IS NOT NULL ALTER TABLE dbo.live_feature_mount ALTER COLUMN [特徵值] DECIMAL(38,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [收盤價] DECIMAL(28,6) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [二十日報酬] DECIMAL(28,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [相對大盤二十日強弱] DECIMAL(28,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [相對產業二十日強弱] DECIMAL(28,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [ATR百分比] DECIMAL(28,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [二十日實現波動率] DECIMAL(28,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [成交額代理] DECIMAL(38,6) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [二十日平均成交額代理] DECIMAL(38,6) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [營收年增率] DECIMAL(28,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [籌碼總比率] DECIMAL(28,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [相對大盤分位數] DECIMAL(18,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [相對產業分位數] DECIMAL(18,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [營收年增率分位數] DECIMAL(18,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [籌碼總比率分位數] DECIMAL(18,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [成交額分位數] DECIMAL(18,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [平均成交額分位數] DECIMAL(18,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [ATR百分比分位數] DECIMAL(18,10) NULL",
        "IF OBJECT_ID(N'dbo.feature_cross_section_snapshot', N'U') IS NOT NULL ALTER TABLE dbo.feature_cross_section_snapshot ALTER COLUMN [實現波動率分位數] DECIMAL(18,10) NULL",
    ]
    for sql in alter_sqls:
        _safe_exec(cursor, sql)

    ensure_training_universe_daily_chinese_columns(cursor)


def main():
    conn = pyodbc.connect(TARGET_CONN_STR)
    cur = conn.cursor()
    ensure_research_plus_tables(cur)
    conn.commit()
    conn.close()
    safe_print('✅ research plus tables ready')


if __name__ == '__main__':
    main()
