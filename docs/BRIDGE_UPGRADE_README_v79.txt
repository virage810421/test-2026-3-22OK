橋接版說明
================

這版已直接橋接原本主幹檔，不是重做一套新系統。

已升級的核心邏輯：
1. master_pipeline.py
   - 啟動 watchlist 後，先跑 LocalHistoryBootstrap，再跑 PriceGapBridge。
2. screening.py
   - K線讀取順序改成：SQL -> data/kline_cache CSV -> 網路 -> 舊快取回退。
3. fts_local_history_bootstrap.py
   - 會正規化 ticker，並在整理本地 OHLCV 後同步產出 data/last_price_snapshot.csv。
4. fts_price_gap_bridge.py
   - 價格橋接順序改成：manual override -> last snapshot -> decision -> kline cache -> SQL -> 掃描 CSV -> 網路。
   - 會回寫 data/last_price_snapshot.csv。

建議執行順序：
1. python master_pipeline.py
2. 若要單獨補快取：python -c "from fts_local_history_bootstrap import LocalHistoryBootstrap; LocalHistoryBootstrap().build()"
3. 若要單獨補價格：python -c "from fts_price_gap_bridge import PriceGapBridge; PriceGapBridge().build(['2330.TW','2317.TW'])"

注意：
- 若 SQL 沒有 daily_price_data，screening.py 仍可正常退回到 CSV / 網路。
- 若本地沒有任何 OHLCV，bootstrap 仍會產生 request list 與 last snapshot 骨架。
