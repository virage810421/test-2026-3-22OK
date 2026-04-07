v24 升級 patch
==============
這版重點：
retry queue 內的失敗任務，開始能在下次啟動時自動挑選補跑。

新增：
1. enable_auto_retry_on_boot
   - 啟動時自動檢查 retry queue
2. auto_retry_required_tasks
   - 是否自動補跑必要任務
3. retry_only_same_stage_enabled
   - 只有該 stage 開啟時才補跑
4. fts_orchestrator.py
   - 新增 execute_retry_items()
5. fts_retry_queue.py
   - 新增 list_retryable_items()
   - 新增 mark_success()

建議的安全做法：
- 先維持：
  enable_auto_retry_on_boot = True
  auto_retry_required_tasks = False
  auto_retry_failed_optional_tasks = False
- 先讓系統具備補跑骨架
- 之後再逐步放開自動補跑
