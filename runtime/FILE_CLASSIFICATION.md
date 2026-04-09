# File Classification (2026-04-09 23:20:13)

## 一定要留
- formal_trading_system_v83_official_main.py：主線 or 核心 service / guard / engine
- formal_trading_system_v82_three_stage_upgrade.py：主線 or 核心 service / guard / engine
- fts_fundamentals_etl_mainline.py：主線 or 核心 service / guard / engine
- fts_training_governance_mainline.py：主線 or 核心 service / guard / engine
- fts_trainer_backend.py：主線 or 核心 service / guard / engine
- fts_phase1_upgrade.py：主線 or 核心 service / guard / engine
- fts_phase2_mock_broker_stage.py：主線 or 核心 service / guard / engine
- fts_phase3_real_cutover_stage.py：主線 or 核心 service / guard / engine
- fts_decision_execution_bridge.py：主線 or 核心 service / guard / engine
- fts_live_safety.py：主線 or 核心 service / guard / engine
- fts_reconciliation_engine.py：主線 or 核心 service / guard / engine
- fts_preopen_checklist.py：主線 or 核心 service / guard / engine
- fts_intraday_incident_guard.py：主線 or 核心 service / guard / engine
- fts_eod_closebook.py：主線 or 核心 service / guard / engine
- fts_market_data_service.py：主線 or 核心 service / guard / engine
- fts_feature_service.py：主線 or 核心 service / guard / engine
- fts_chip_enrichment_service.py：主線 or 核心 service / guard / engine
- fts_screening_engine.py：主線 or 核心 service / guard / engine
- fts_sector_service.py：主線 or 核心 service / guard / engine
- fts_system_guard_service.py：主線 or 核心 service / guard / engine
- fts_risk_gateway.py：主線 or 核心 service / guard / engine
- fts_watchlist_service.py：主線 or 核心 service / guard / engine
- fts_market_climate_service.py：主線 or 核心 service / guard / engine
- fts_decision_desk_builder.py：主線 or 核心 service / guard / engine
- fts_signal_gate.py：主線 or 核心 service / guard / engine
- fts_portfolio_gate.py：主線 or 核心 service / guard / engine
- fts_position_state_service.py：主線 or 核心 service / guard / engine
- fts_ab_wave_upgrade.py：主線 or 核心 service / guard / engine
- fts_ab_diff_audit.py：主線 or 核心 service / guard / engine
- ml_data_generator.py：主線 or 核心 service / guard / engine
- daily_chip_etl.py：主線 or 核心 service / guard / engine
- monthly_revenue_simple.py：主線 or 核心 service / guard / engine
- advanced_chart.py：主線 or 核心 service / guard / engine

## 只補差異
- yahoo_csv_to_sql.py：已收編，現在只補差異
- daily_chip_etl.py：已收編，現在只補差異
- monthly_revenue_simple.py：已收編，現在只補差異
- ml_data_generator.py：已收編，現在只補差異
- advanced_chart.py：已收編，現在只補差異
- config.py：已收編，現在只補差異

## 先別動
- launcher.py：仍可作零件來源，先別整支搬
- execution_engine.py：仍可作零件來源，先別整支搬
- paper_broker.py：仍可作零件來源，先別整支搬
- portfolio_risk.py：仍可作零件來源，先別整支搬
- master_pipeline.py：仍可作零件來源，先別整支搬
- live_paper_trading.py：仍可作零件來源，先別整支搬

## 已收編，但先保留相容入口
- yahoo_csv_to_sql.py：已被 fts_fundamentals_etl_mainline.py 收編；只補差異
- model_governance.py：已被 fts_training_governance_mainline.py 收編；保留治理核心
- ml_trainer.py：已被 fts_trainer_backend.py 收編；保留舊執行入口
- daily_chip_etl.py：已被 fts_etl_daily_chip_service.py 收編；保留舊門牌
- monthly_revenue_simple.py：已被 fts_etl_monthly_revenue_service.py 收編；保留舊門牌
- ml_data_generator.py：已被 fts_training_data_builder.py 收編；保留舊門牌
- advanced_chart.py：已被 fts_chart_service.py 收編；保留舊門牌