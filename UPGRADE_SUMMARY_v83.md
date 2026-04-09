# UPGRADE SUMMARY v83

## 本次真正合併進主線
- yahoo_csv_to_sql.py → `fts_fundamentals_etl_mainline.py`
- model_governance.py → `fts_training_governance_mainline.py`
- ml_trainer.py → `fts_trainer_backend.py`

## 新主線入口
- `formal_trading_system_v83_official_main.py`

## 三階段完成度
- Phase1：完整升級（就 pre-live / 無真券商範圍）
- Phase2：完整升級（就 mock / 假真券商範圍）
- Phase3：未完整升級，僅完成 contract / cutover skeleton，待真開戶

## 相容策略
- 保留 `formal_trading_system_v82_three_stage_upgrade.py` 作 wrapper
- 保留 `yahoo_csv_to_sql.py` / `model_governance.py` / `ml_trainer.py` 作舊入口相容
