v17 升級 patch
==============
這包是基於你目前 v16 / v16.1 狀態往上升。

這次重點：
1. 修 report 相容問題
   - fts_report.py 改成支援新舊呼叫方式
2. decision 診斷更完整
   - fts_compat.py 會輸出 normalized_decision_preview.csv
   - 並印出 rows / ticker / action / price 統計
3. SignalLoader 診斷更完整
   - 印出 valid / skip_no_ticker / skip_bad_action / skip_no_price / skip_zero_qty / actions
4. 新主入口
   - formal_trading_system_v17.py

覆蓋後建議直接跑：
python formal_trading_system_v17.py
