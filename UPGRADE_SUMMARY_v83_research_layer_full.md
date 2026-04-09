# v83 研究層完整強化包

這包完成的範圍：

1. 全市場 percentile 正式版
   - 新增 `fts_cross_sectional_percentile_service.py`
   - 輸出 `data/feature_cross_section_snapshot.csv`
   - 正式化 market/sector/revenue/chip/turnover/volatility percentile

2. 事件窗日曆精準化
   - 新增 `fts_event_calendar_service.py`
   - 輸出 `data/feature_event_calendar.csv`
   - 精準產出 revenue / earnings / dividend 視窗特徵

3. 特徵掛載落實
   - `selected_features.pkl` 真正掛到 live path
   - 輸出 `runtime/live_feature_mount.json`
   - 輸出 `data/selected_live_feature_mounts.csv`

4. v83 主控接線
   - 正式接入 percentile + event calendar + feature stack audit

5. 研究層範圍完整升級
   - 特徵分桶
   - selected-feature 實戰控制
   - 全市場 percentile 正式版
   - 精準事件窗
   - live feature mount

注意：
- 這裡的「完整」是指 **研究/特徵層** 範圍，不包含真券商、即時行情、外部完整事件資料庫。
- 第一次上線前請先跑：
  - `python run_full_market_percentile_snapshot.py`
  - `python run_precise_event_calendar_build.py`
  - `python formal_trading_system_v83_official_main.py`
