v20 升級 patch
==============
這版重點：
把你的上游任務正式註冊進主控，不只畫架構，還讓主控知道 ETL / AI / decision builder 是誰。

新增：
1. fts_task_registry.py
   - 註冊 ETL / AI / decision 任務
2. fts_orchestrator.py
   - 先做存在性檢查與 ready/missing 回報
   - 下一輪再接實際執行/排程
3. formal_trading_system_v20.py
   - 啟動時輸出 task_registry.json
   - 執行時檢查上游任務是否存在

這版的意義：
- 你的主架構不再只是概念
- 主控已經開始「辨識並檢查」上游任務
