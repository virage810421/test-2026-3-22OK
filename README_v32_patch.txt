v32 升級 patch
==============
這版重點：

1. 回答你的問題：
   - 研究/選股層很重要
   - 它不是不重要，而是它主要決定「賺得好不好」
   - 風控/恢復/驗證/實盤保護則更直接決定「會不會先因工程事故出大事」

2. 升級內容：
   - fts_live_safety.py
     - paper/live 實盤保護層
     - 產出 live_safety_gate.json
   - fts_research_registry.py
     - 正式把研究/選股層的重要性與角色掛回主控視角
   - formal_trading_system_v32.py
     - execution 前除了 launch gate，還要過 live safety gate

這版的意義：
- 研究/選股層被正式承認為核心上游
- 但 execution 前還多了一層實盤保護
- 更接近未來接真券商前的最後保險
