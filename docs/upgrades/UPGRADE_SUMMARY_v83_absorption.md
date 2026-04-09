# UPGRADE SUMMARY v83 absorption patch

## 這次完成

### 第一批：核心抽離（完整升級）
- daily_chip_etl.py → fts_etl_daily_chip_service.py
- monthly_revenue_simple.py → fts_etl_monthly_revenue_service.py
- ml_data_generator.py → fts_training_data_builder.py
- 原舊檔全部改成 wrapper

### 第二批：圖表服務整理（完整升級）
- advanced_chart.py → fts_chart_service.py
- 原舊檔改成 wrapper

### 第三批：只改呼叫方式（完整升級）
- ml_trainer.py 保留舊入口，主線改走 fts_trainer_backend.py
- model_governance.py 保留治理核心服務，由 fts_training_governance_mainline.py 統一調度

## 「保留舊門牌，但新大樓已經搬走」是什麼意思？
- 舊檔名還在，舊指令還能跑
- 真正工作邏輯已搬到新的 service/backend
- 目的：不中斷你原本使用方式，同時讓主線逐步去 legacy 化
