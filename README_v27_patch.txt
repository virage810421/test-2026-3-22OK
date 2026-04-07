v27 升級 patch
==============
這版重點：
把 health dashboard 再濃縮成 daily ops summary 與 alerts。

新增：
1. fts_daily_ops.py
   - 產出 runtime/daily_ops_summary.json
   - 產出 runtime/alerts.json
2. 異常旗標規則目前包含：
   - heartbeat crash
   - retry queue pending_retry > 0
   - upstream failed > 0
   - zero signal
   - rejected orders > 0
3. formal_trading_system_v27.py
   - 主流程結束時輸出 summary 與 alerts

這版的意義：
- 你每天不一定要先看完整 dashboard
- 可以先看 daily ops summary / alerts
- 更接近營運值班視角
