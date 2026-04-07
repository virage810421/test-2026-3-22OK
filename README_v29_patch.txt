v29 升級 patch
==============
這版重點是回答：
「升級這麼多版本，AI 訓練在哪裡？」

答案：
前面很多版都在補主控、執行、營運、調度與安全閘門，
AI 訓練一直有被註冊，但沒有被清楚掛回主控視角。

v29 補上：
1. fts_ai_pipeline.py
   - ai_pipeline_registry.json
   - ai_pipeline_status.json
   - ai_decision_bridge.json
2. formal_trading_system_v29.py
   - 啟動時輸出 AI pipeline 的註冊/狀態/橋接資訊
3. Progress
   - AI訓練層提升
   - 新增 AI訓練掛回/橋接

這版的意義：
- AI 訓練不再只是「你自己知道有 ml_trainer.py」
- 而是主控開始知道：
  - 訓練入口是誰
  - 訓練資產是否存在
  - 訓練與 decision / execution 的橋在哪裡
