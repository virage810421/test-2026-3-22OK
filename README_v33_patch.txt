v33 升級 patch
==============
這版重點：
在未來接真券商前，再多一層 broker approval gate。

新增：
1. fts_broker_approval.py
   - 產出 broker_approval_gate.json
   - mode / broker_type 非純 paper 時，提醒未來需要人工審批
2. formal_trading_system_v33.py
   - execution 前要同時通過：
     - launch gate
     - live safety gate
     - broker approval gate

這版的意義：
- 未來你就算開始碰 live 相關接入
- 系統也不會只有一層 gate
- 更接近真正實戰前的雙重/三重保護
