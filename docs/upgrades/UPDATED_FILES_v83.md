# UPDATED FILES v83

## 新增 / 改名主線
- formal_trading_system_v83_official_main.py
- formal_trading_system_v82_three_stage_upgrade.py  （現在是 wrapper，轉呼叫 v83）

## 新增主線吸收模組
- fts_fundamentals_etl_mainline.py
- fts_training_governance_mainline.py
- fts_trainer_backend.py

## 已更新的相容與治理檔
- fts_training_orchestrator.py
- fts_file_classification.py
- fts_config.py
- ml_trainer.py
- model_governance.py

## 三段完成度
- Phase1：完整升級（pre-live 範圍）
- Phase2：完整升級（mock broker 範圍）
- Phase3：未完整升級（已完成 contract / cutover skeleton，待真開戶）

## 執行入口
- python formal_trading_system_v83_official_main.py
