v18 升級 patch
==============
這版不是再加交易條件，而是補「長期穩定跑」最重要的營運層。

新增：
1. runtime lock
   - 避免同一台機器同時跑兩個實例
2. heartbeat
   - runtime/heartbeat.json 會寫目前跑到哪個 stage
3. decision archive
   - 每次跑前先封存原始 decision input
4. audit trail
   - runtime/audit_events.jsonl 會記錄 boot / recovery / execution / crash 等事件
5. config snapshot
   - 每次啟動先把設定輸出成 json

主入口：
- formal_trading_system_v18.py

建議：
- 這版很適合往「排程每天自動跑」靠近
- 先保留 v17 備援，再切到 v18
