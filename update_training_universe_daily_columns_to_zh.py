# -*- coding: utf-8 -*-
from __future__ import annotations

import pyodbc

TARGET_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)

RENAME_PAIRS = [
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


def rename_column_if_exists(cursor, old_col: str, new_col: str):
    old_safe = old_col.replace("'", "''")
    new_safe = new_col.replace("'", "''")
    sql = f"""
    IF OBJECT_ID(N'dbo.training_universe_daily', N'U') IS NOT NULL
       AND COL_LENGTH(N'dbo.training_universe_daily', N'{old_safe}') IS NOT NULL
       AND COL_LENGTH(N'dbo.training_universe_daily', N'{new_safe}') IS NULL
    BEGIN
        EXEC sp_rename N'dbo.training_universe_daily.[{old_col}]', N'{new_col}', 'COLUMN';
    END
    """
    cursor.execute(sql)


def main():
    conn = pyodbc.connect(TARGET_CONN_STR)
    cur = conn.cursor()
    for old_col, new_col in RENAME_PAIRS:
        try:
            rename_column_if_exists(cur, old_col, new_col)
        except Exception:
            pass
    conn.commit()
    conn.close()
    print('✅ training_universe_daily 欄位中文化完成')


if __name__ == '__main__':
    main()
