# -*- coding: utf-8 -*-
PATCH_NAME = 'db_setup_SQLAlchemy_參數傳遞修復'
CHANGED_FILES = ['fts_db_engine.py']
SUMMARY = [
    '修正 SQLAlchemy 2.x 下 qmark 參數傳遞，改用 exec_driver_sql()',
    '修正 scalar()/execute()/executemany() 與 SQL Server pyodbc 方言相容性',
    '避免 db_setup.py 在 table_exists() 階段因 tuple/list 參數型別而炸掉',
]
NEXT_STEPS = [
    'python db_setup.py --mode upgrade',
    'python monthly_revenue_simple.py',
    'python formal_trading_system_v83_official_main.py --bootstrap',
]
