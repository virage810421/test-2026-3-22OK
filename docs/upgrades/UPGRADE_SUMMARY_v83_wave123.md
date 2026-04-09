# UPGRADE SUMMARY v83 wave123

## 這次完成的順序
1. A 當主版本：完成
2. 波次 1：screening 收編：完成
3. 波次 2：sector / system_guard / risk_gateway 收編：完成
4. 波次 3：master_pipeline / live_paper_trading 規則層抽離：完成
5. 已收編模組只補差異：完成
6. smoke tests：已加入並可執行

## 這次新增的 service / engine
- fts_market_data_service.py
- fts_feature_service.py
- fts_chip_enrichment_service.py
- fts_screening_engine.py
- fts_sector_service.py
- fts_system_guard_service.py
- fts_risk_gateway.py
- fts_watchlist_service.py
- fts_market_climate_service.py
- fts_decision_desk_builder.py
- fts_signal_gate.py
- fts_portfolio_gate.py
- fts_position_state_service.py
- fts_ab_diff_audit.py
- fts_ab_wave_upgrade.py

## 哪些段落完整升級
- Step1 A 當主版本：完整升級
- Wave1 screening 收編：完整升級
- Wave2 supporting services 收編：完整升級
- Wave3 pipeline rules 收編：完整升級
- Diff patch plan：完整升級
- Smoke tests：完整升級

## 仍然不是 100% 完成的部分
- 真券商 live-ready 仍然維持 adapter-ready / account-pending
