v19 升級 patch
==============
這版主要回答兩件事：

1. 你現在升級到哪裡？
   - 新主控/執行層已經很成熟
   - 但上游 ETL / AI / research 還沒有完全收進統一主架構
   - 所以還需要升級，但方向已經很清楚

2. 會不會偏離主架構？
   - v19 就是在修這件事
   - 新增 architecture_map，把你的原始 ETL/AI/研究輸出清楚映射到新主控
   - 目的不是取代你的主架構，而是把新主控掛回去

新增：
- fts_architecture_map.py
- formal_trading_system_v19.py
- Progress 更新

建議：
- v19 開始可把它視為「正式執行中樞」
- 你的舊 ETL / AI / research 還是上游來源，不是被否定
