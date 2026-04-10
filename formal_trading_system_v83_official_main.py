# -*- coding: utf-8 -*-
from __future__ import annotations

"""Level-3 full control wrapper.

唯一正式入口改由 fts_control_tower.main 接管，
保留 formal_trading_system_v83_official_main.py 舊門牌，避免外部啟動命令中斷。
"""

from fts_control_tower import main

CONTROL_LEVEL = 'level_3'
CONTROL_TARGET = 'fts_control_tower.main'


if __name__ == '__main__':
    raise SystemExit(main())
