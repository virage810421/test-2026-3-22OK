# -*- coding: utf-8 -*-
PATCH_NAME = '架構收尾_legacy_detach_and_db_migration'
PATCH_SCOPE = [
    '斬斷核心主線對 legacy facade 的依賴',
    '新增 service-first internal API',
    '新增 DB adapter / migration / optional ORM facade',
    '重寫 db_setup.py 與 db_logger.py',
    '新增 legacy facade cleanup / detach guard',
]
CHANGED_FILES = [
    'fts_service_api.py', 'fts_legacy_detach_guard.py', 'fts_legacy_facade_cleanup.py',
    'fts_db_engine.py', 'fts_db_schema.py', 'fts_db_migrations.py', 'fts_db_orm.py',
    'db_setup.py', 'db_logger.py',
    'advanced_optimizer.py', 'optimizer.py', 'event_backtester.py', 'live_paper_trading.py',
    'fts_model_layer.py', 'fts_etl_daily_chip_service.py', 'fts_legacy_master_pipeline_impl.py',
    'advanced_chart.py', 'screening.py', 'strategies.py', 'master_pipeline.py', 'ml_data_generator.py', 'ml_trainer.py', 'yahoo_csv_to_sql.py',
    'fts_screening_detachment_audit.py',
]
