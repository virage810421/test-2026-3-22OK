# -*- coding: utf-8 -*-
"""
v93 schema/view autosync patch manifest

更新檔內容：
1. fts_control_tower.py
   - bootstrap / daily / train 啟動時自動執行 MigrationRunner().upgrade()。
   - 自動同步 SQL Table 結構與中文欄位 View。
   - runtime 輸出 db_schema_view_autosync 狀態。

2. fts_db_migrations.py
   - 正式基本面主表固定為 dbo.fundamentals_clean。
   - 偵測舊名/誤建表：damendals_clean、damental_clean、fundamental_clean、fundamental_data、fundamentals_data。
   - 先把可相容欄位資料搬入 dbo.fundamentals_clean，再將舊名表改名為 zzz_legacy_* 備份，不直接刪資料。
   - migration 報告新增 canonical_table_consolidation。

3. fts_sql_chinese_column_views.py
   - 中文欄位 View 改為 V3 管理標記。
   - 不再為誤建/舊名/備份表建立中文 View，避免把非正式表誤當主線。
   - dbo.fundamentals_clean 會固定對應 dbo.基本面清洗資料_中文欄位。

使用方式：
- 覆蓋到專案根目錄。
- 執行：python formal_trading_system_v83_official_main.py --bootstrap
- 之後 daily/train 每次啟動都會自動同步，不必手動跑 View sync。
"""

PATCH_VERSION = "v93_schema_view_autosync_canonical_fundamentals"
UPDATED_FILES = [
    "fts_control_tower.py",
    "fts_db_migrations.py",
    "fts_sql_chinese_column_views.py",
]
