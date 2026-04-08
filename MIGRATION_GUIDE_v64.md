# FTS v64 升級與覆蓋說明

## 先講結論
`fts_upgrade_100_pack.zip` / `fts_upgrade_100_v63_pack.zip` / 本次 `v64` 包，都不是「單一檔案貼進舊檔」的更新方式。
它們是 **整包專案快照**。

## 正確做法
1. 先把舊專案整份備份，例如 `test-2026-3-22OK_backup_before_v64/`
2. 解壓本次升級包到 **新資料夾**，例如 `test-2026-3-22OK_v64/`
3. 再把你舊專案中真正屬於你自己的資料或機密搬過來：
   - `.env` / secrets
   - SQL 連線設定
   - 券商憑證
   - 自訂 watch_list / training_pool / config 參數
   - 你的 ETL cache / CSV 備份
4. 用新資料夾執行 `formal_trading_system_v64.py`
5. 確認 runtime 報告都正常，再決定是否把舊專案切換成新版本

## 不建議
- 不建議只複製 `formal_trading_system_v64.py` 一支去覆蓋舊專案
- 不建議直接無備份覆蓋整個舊資料夾
- 不建議在尚未補齊憑證與價格快照時切到 LIVE

## 你需要自行確認的檔案
- `config.py`
- `fts_config.py`
- `db_setup.py`
- `daily_chip_etl.py`
- `monthly_revenue_simple.py`
- `yahoo_csv_to_sql.py`
- `ml_data_generator.py`
- `ml_trainer.py`
- `model_governance.py`

## 上線前最低要求
- 有 `daily_decision_desk.csv`
- 有 `last_price_snapshot.csv`
- 有模型 artifacts
- 有 broker credentials
- LIVE 模式需人工解鎖
