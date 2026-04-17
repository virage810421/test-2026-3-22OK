這包更新檔做了兩件事：

1. 第一次啟動（Bootstrap）在 GUI 內會先跑：
   python -u fts_db_migrations.py upgrade
   再跑：
   python -u formal_trading_system_v83_official_main.py --bootstrap

   也就是先確保建立 / 升級資料庫【股票Online】，再執行 bootstrap。

2. 專案內主要資料庫名稱預設從【股票online】統一改成【股票Online】。
   已更新核心設定與常見直寫連線字串檔案。

覆蓋方式：
- 將本 ZIP 內檔案覆蓋到專案根目錄
- 之後用 GUI 按「初始化 / 第一次啟動」即可

注意：
- 若你的 SQL Server 區分大小寫，舊資料庫【股票online】與新資料庫【股票Online】會被視為不同 DB。
- 若不想建立新庫，請先確認你的 SQL Server collation 是否不區分大小寫。
