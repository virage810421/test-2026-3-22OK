v20.1 patch
===========
這次修：
1. ImportError: cannot import name 'ExecutionReadinessChecker'
   - 原因是你的 fts_signal.py 不是新版
   - 現在已補回 ExecutionReadinessChecker
2. formal_trading_system_v20.py
   - 改成對部分 v18/v19/v20 設定缺失更容錯
   - 用 getattr 讀某些新設定，避免舊 config 混裝直接炸掉

覆蓋：
- fts_signal.py
- formal_trading_system_v20.py

再重跑：
python formal_trading_system_v20.py
