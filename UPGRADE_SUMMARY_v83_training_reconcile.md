# v83 訓練治理 / 對帳回填強化包

這包針對 Gemini 提到、且經過實際檢視後值得修的兩條深水區補強：

A. Training Governance / ML Trainer
- train-only feature selection
- purged walk-forward
- out-of-time holdout
- overfit gap 與 feature/sample ratio 檢查
- training stress audit
- model governance live health typo 修復 + training integrity gate

B. Reconciliation / True Backfill
- reconciliation 多 key 對帳 / 去重
- corporate action suspect detection
- repair actions suggestions
- startup repair plan 強化
- fundamentals true backfill 增加 stale / dedupe 檢查
- backfill resilience audit

更新檔：
- formal_trading_system_v83_official_main.py
- model_governance.py
- fts_trainer_backend.py
- fts_training_governance_mainline.py
- fts_reconciliation_engine.py
- fts_state_repair.py
- fts_fundamentals_true_backfill.py
- fts_training_stress_audit.py
- fts_backfill_resilience_audit.py
- run_training_stress_audit.py
- run_backfill_resilience_audit.py
