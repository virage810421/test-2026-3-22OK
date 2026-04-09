# File Classification (2026-04-09 03:18:29)

## 一定要留
- formal_trading_system_v83_official_main.py：主線 or 核心 contract / 執行 / 風控 / ETL
- formal_trading_system_v82_three_stage_upgrade.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_fundamentals_etl_mainline.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_training_governance_mainline.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_trainer_backend.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_phase1_upgrade.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_phase2_mock_broker_stage.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_phase3_real_cutover_stage.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_broker_factory.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_broker_paper.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_broker_real_stub.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_decision_execution_bridge.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_live_safety.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_reconciliation_engine.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_preopen_checklist.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_intraday_incident_guard.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_eod_closebook.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_callback_event_schema.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_callback_event_store.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_order_state_machine.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_broker_requirements_contract.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_real_broker_adapter_blueprint.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_live_release_gate.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_live_cutover_plan.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_training_orchestrator.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_training_prod_readiness.py：主線 or 核心 contract / 執行 / 風控 / ETL
- fts_trainer_promotion_policy.py：主線 or 核心 contract / 執行 / 風控 / ETL
- ml_data_generator.py：主線 or 核心 contract / 執行 / 風控 / ETL
- daily_chip_etl.py：主線 or 核心 contract / 執行 / 風控 / ETL
- monthly_revenue_simple.py：主線 or 核心 contract / 執行 / 風控 / ETL
- advanced_chart.py：主線 or 核心 contract / 執行 / 風控 / ETL

## 可合併
- fts_bridge_replacement_plan.py：偏治理/報表/規劃，可以再收斂
- fts_legacy_bridge_map.py：偏治理/報表/規劃，可以再收斂
- fts_module_fate_map.py：偏治理/報表/規劃，可以再收斂
- fts_unused_candidates.py：偏治理/報表/規劃，可以再收斂
- fts_upgrade_plan.py：偏治理/報表/規劃，可以再收斂
- fts_target95_plan.py：偏治理/報表/規劃，可以再收斂
- fts_target95_push.py：偏治理/報表/規劃，可以再收斂
- fts_target95_scorecard.py：偏治理/報表/規劃，可以再收斂
- fts_upgrade_truth_report.py：偏治理/報表/規劃，可以再收斂
- fts_completion_gap_report.py：偏治理/報表/規劃，可以再收斂
- fts_progress.py：偏治理/報表/規劃，可以再收斂
- fts_progress_full_report.py：偏治理/報表/規劃，可以再收斂
- fts_gate_summary.py：偏治理/報表/規劃，可以再收斂
- fts_console_brief.py：偏治理/報表/規劃，可以再收斂
- fts_report.py：偏治理/報表/規劃，可以再收斂
- fts_dashboard.py：偏治理/報表/規劃，可以再收斂

## 先別動
- launcher.py：仍可能被 legacy 主線或資料流依賴
- risk_gateway.py：仍可能被 legacy 主線或資料流依賴
- execution_engine.py：仍可能被 legacy 主線或資料流依賴
- paper_broker.py：仍可能被 legacy 主線或資料流依賴
- portfolio_risk.py：仍可能被 legacy 主線或資料流依賴
- system_guard.py：仍可能被 legacy 主線或資料流依賴
- live_paper_trading.py：仍可能被 legacy 主線或資料流依賴
- master_pipeline.py：仍可能被 legacy 主線或資料流依賴

## 已合併進主線，但先保留相容入口
- yahoo_csv_to_sql.py：已被 fts_fundamentals_etl_mainline.py 吸收；先保留相容入口
- model_governance.py：已被 fts_training_governance_mainline.py 吸收；先保留治理函式
- ml_trainer.py：已被 fts_trainer_backend.py 吸收；先保留舊執行入口

## 可刪除（先跑完 v83 smoke test）
- formal_trading_system_v79.py：v83 冒煙測試通過後可封存或刪除
- formal_trading_system_v80_prebroker_sealed.py：v83 冒煙測試通過後可封存或刪除
- formal_trading_system_v81_mainline_merged.py：v83 冒煙測試通過後可封存或刪除
- fts_live_adapter_stub.py：v83 冒煙測試通過後可封存或刪除