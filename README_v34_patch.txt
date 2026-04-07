v34 升級 patch
==============
這版重點：
把研究/選股層從「重要、已掛回」再升到「有正式品質閘門」。

新增：
1. fts_research_gate.py
   - 產出 research_quality_gate.json
   - 檢查：
     - research normalize 後是否為空
     - ticker/action/price 是否缺失
     - research output 是否轉不出有效 signals
2. formal_trading_system_v34.py
   - 在 readiness 後先跑 research gate
   - 再進入 model gate / launch gate / live safety / broker approval

這版的意義：
- 研究/選股層不再只是概念上重要
- 而是開始有正式的輸出品質檢查
- 更接近把 research layer 也工程化
