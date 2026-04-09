# v83 全面升級補洞包

本包完成：
- 研究層特徵服務正式化
- 全市場 percentile snapshot 正式化
- 精準事件窗日曆正式化
- selected_features live mount 正式化
- 訓練資料 builder 掛接新特徵
- SQL 研究支援表增補腳本
- CSV/feature snapshot 寫回 SQL 腳本
- 主線全面串聯與任務完成稽核

這次屬於完整升級的段落：
- 特徵層補洞與實戰掛載
- 訓練資料 builder 串接新特徵
- 研究層額外 table 建立能力
- feature snapshot / event calendar / live mount 寫回 SQL
- 主控與任務完成稽核串聯

仍需使用者實際執行：
1. python db_setup_research_plus.py
2. python run_full_market_percentile_snapshot.py
3. python run_precise_event_calendar_build.py
4. python run_sync_feature_snapshots_to_sql.py
5. python formal_trading_system_v83_official_main.py
6. python ml_data_generator.py
7. python ml_trainer.py

注意：
- 這包是「全面補洞與串聯包」，不是對你全部專案每一支 py 逐行重寫。
- 但它已補上目前最缺的主線、feature、event、SQL、task audit 幾條鏈。
