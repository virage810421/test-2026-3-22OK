v28 升級 patch
==============
這版重點：
把上游狀態、decision 品質、signals 與 retry queue 收斂成發車前驗證閘門。

新增：
1. fts_gatekeeper.py
   - 產出 runtime/launch_gate.json
   - 明確給出 go_for_execution = True / False
2. gate 規則目前包含：
   - 缺少必要上游任務
   - 必要上游任務失敗
   - required retry queue pending
   - normalized decision 為空
   - ticker/action/price 全缺
   - zero signal 先列 warning
3. formal_trading_system_v28.py
   - gate 不通過時直接跳過 execution
   - gate 通過才送單

這版的意義：
- 系統不再只會「看到資料就送單」
- 而是開始有正式的發車前驗證
- 更接近正式實戰交易系統的最後一段工程化
