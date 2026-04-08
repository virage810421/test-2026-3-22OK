v11.1 patch
=================
問題原因：
formal_trading_system_v11.py 會呼叫：
    SignalLoader.load_from_normalized_df(normalized_df)

但你專案中的 fts_signal.py 仍是較舊版本，沒有這個方法。

修復方式：
1. 用這份 fts_signal.py 覆蓋專案中的 fts_signal.py
2. 重新執行 formal_trading_system_v11.py

這次錯誤不是 decision csv 壞掉，而是模組版本不一致。
