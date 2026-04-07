v25 升級 patch
==============
這版重點：
每次上游任務執行後，會把結果封存成 task log。

新增：
1. fts_task_logs.py
   - 將每個 task 的結果寫到 runtime/task_logs/*.json
2. fts_orchestrator.py
   - _execute_one() 會保存：
     - stdout
     - stderr
     - returncode
     - status
     - stage / task / script
3. formal_trading_system_v25.py
   - 延續 v24 自動補跑與主流程

這版的意義：
- 任務失敗時，不再只看主 log
- 你可以直接打開對應 task log 看 stdout/stderr
- 更接近正式營運需要的可追查性
