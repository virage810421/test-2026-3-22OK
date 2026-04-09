# v83 單一入口三模式最終整合版

這包將 v83 主控收口成單一入口三模式：

- `python formal_trading_system_v83_official_main.py --bootstrap`
- `python formal_trading_system_v83_official_main.py`
- `python formal_trading_system_v83_official_main.py --train`

並一併納入兩個已知 hotfix，避免主控進入日常模式時因版本落差中斷：

- `model_governance.py`：修正 `payloadd` typo
- `fts_feature_stack_audit.py` / `fts_feature_catalog.py`：補齊 `percentile_backed`、`event_calendar_precise` 相容欄位

## 模式說明

### bootstrap
第一次建置 / 新電腦初始化。會嘗試依序呼叫：
- `db_setup_research_plus.py`
- `run_full_market_percentile_snapshot.py`
- `run_precise_event_calendar_build.py`
- `run_sync_feature_snapshots_to_sql.py`

完成後會再自動跑一次 daily 主控與 completion audit。

### daily
預設模式。會執行：
- fundamentals ETL local sync
- training governance summary
- training stress audit
- backfill resilience audit
- feature stack audit
- cross-sectional percentile 狀態
- event calendar 狀態
- mainline linkage
- project completion audit
- file classification
- phase1/2/3 upgrade stages

### train
重訓模式。會執行：
- `ml_data_generator.py`
- `ml_trainer.py`
- `run_project_completion_audit.py`
- training governance summary
- training stress audit

## 使用方式
覆蓋到專案根目錄後直接使用三模式入口即可。
