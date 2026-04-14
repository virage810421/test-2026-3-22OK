# -*- coding: utf-8 -*-
"""本檔只是套用說明，不參與交易流程。"""

PACKAGE_NAME = "清掉舊門牌_保留功能本體_可套用版"
REMOVED_OLD_DOORS = [
    "advanced_chart.py",
    "daily_chip_etl.py",
    "monthly_revenue_simple.py",
    "yahoo_csv_to_sql.py",
    "ml_data_generator.py",
    "ml_trainer.py",
    "formal_trading_system.py",
    "kline_cache.py",
]
FUNCTION_BODY_OWNERS = {
    "advanced_chart.py": "fts_chart_service.py",
    "daily_chip_etl.py": "fts_etl_daily_chip_service.py",
    "monthly_revenue_simple.py": "fts_etl_monthly_revenue_service.py",
    "yahoo_csv_to_sql.py": "fts_fundamentals_etl_mainline.py",
    "ml_data_generator.py": "fts_training_data_builder.py",
    "ml_trainer.py": "fts_trainer_backend.py",
    "formal_trading_system.py": "formal_trading_system_v83_official_main.py",
    "kline_cache.py": "fts_market_data_service.py",
}

APPLY_STEPS = [
    "解壓覆蓋到專案根目錄",
    "執行 python apply_clean_old_doors.py --apply 清掉既有資料夾殘留舊門牌",
    "執行 python run_project_healthcheck.py --deep 檢查",
]
