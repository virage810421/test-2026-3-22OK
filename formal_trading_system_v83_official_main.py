# -*- coding: utf-8 -*-
from __future__ import annotations

"""正式交易主控版 v83 單一入口。

此檔保留為目前正式啟動入口；功能主體由 fts_control_tower.main 接管。
舊門牌 wrapper 已在「清掉舊門牌 + 保留功能本體」版本中移除。
"""

from fts_control_tower import main

CONTROL_LEVEL = 'level_3'
CONTROL_TARGET = 'fts_control_tower.main'


if __name__ == '__main__':
    raise SystemExit(main())
