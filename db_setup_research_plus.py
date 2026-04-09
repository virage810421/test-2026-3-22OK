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


def main():
    conn = pyodbc.connect(TARGET_CONN_STR)
    cur = conn.cursor()
    ensure_research_plus_tables(cur)
    conn.commit()
    conn.close()
    safe_print('✅ research plus tables ready')


if __name__ == '__main__':
    main()
