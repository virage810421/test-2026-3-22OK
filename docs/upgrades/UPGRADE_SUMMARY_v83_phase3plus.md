# UPGRADE SUMMARY v83 phase3plus

## 這次完整升級到位
- `fts_fundamentals_etl_mainline.py`
  - 改成主線內建 ETL
  - 不再必須依賴 `yahoo_csv_to_sql.py` 才能運作
  - 會自動建立 fundamentals CSV 模板與 runtime JSON
- `fts_broker_api_adapter.py`
  - 新增可插式真券商 adapter
  - 支援 config template / probe / REST endpoint mapping
- `fts_phase3_real_cutover_stage.py`
  - 從 contract-only 升成 adapter-ready
- `formal_trading_system_v83_official_main.py`
  - 主線改成直接收編上面兩項

## 三段完成度
- Phase1：完整升級（pre-live 範圍）
- Phase2：完整升級（假真券商範圍）
- Phase3：adapter-ready，開戶後填 config 即可做 live smoke test；未開戶前不宣告 100% 完整

## 自動產生檔案
### 會自動產生
- `daily_decision_desk.csv`（若缺）
- `data/ml_training_data.csv`（若缺）
- `data/market_financials_backup_fullspeed.csv`（若缺，先建模板）
- `data/executable_order_payloads.csv`（Phase1 bridge 執行後）
- `runtime/*.json` 階段報告
- `runtime/broker_adapter_config.template.json`
- `runtime/real_broker_credentials_template.json`

### 不會憑空產生真資料，需條件成立
- fundamentals 真財報列資料：需要 yfinance 可用，且開啟 smart sync
- SQL 寫入：需要 pyodbc + SQL Server 可連線
- 真券商 callback / 真成交資料：需要真券商 API 與帳號
