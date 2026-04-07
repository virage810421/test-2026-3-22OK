v26 升級 patch
==============
這版重點：
把 heartbeat、retry queue、upstream 狀態、持倉與執行結果整合成健康儀表板。

新增：
1. fts_dashboard.py
   - 產出 runtime/health_dashboard.json
2. formal_trading_system_v26.py
   - 在主流程結束時輸出健康儀表板
3. 儀表板內容包含：
   - heartbeat
   - architecture_map
   - task_registry_summary
   - upstream_status
   - upstream_exec
   - retry_queue_summary
   - execution_readiness
   - execution_result
   - positions_summary
   - recent_task_logs

這版的意義：
- 你不只看零散 log
- 而是開始有一份每日總覽狀態
- 更接近真正可營運的正式交易平台
