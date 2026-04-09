# v83 bootstrap 建表 hotfix

這包把 `formal_trading_system_v83_official_main.py` 的 `--bootstrap` 改成：

1. 先跑 `db_setup.py --mode upgrade`
2. 再跑 `db_setup_research_plus.py`
3. 再跑 percentile / event / sync 腳本
4. 並把每一步的成功/失敗與 stdout/stderr 尾段直接印在主畫面上

使用方式：

```powershell
python formal_trading_system_v83_official_main.py --bootstrap
```

若 `db_setup.py` 或 `db_setup_research_plus.py` 失敗，主畫面會直接看到 returncode 與 stderr 尾段。
