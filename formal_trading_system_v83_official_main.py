# -*- coding: utf-8 -*-
from __future__ import annotations

"""正式交易主控版 v83 單一入口。

此檔保留為正式入口，但在轉交 control tower 之前會先確保：
1. runtime/kill_switch_state.json 已自動落地
2. 單一入口三模式參數可直接透傳給 fts_control_tower.main
"""

from typing import Optional, List

from fts_kill_switch import KillSwitchManager
from fts_control_tower import main as control_tower_main

CONTROL_LEVEL = "level_3"
CONTROL_TARGET = "fts_control_tower.main"


def main(argv: Optional[List[str]] = None) -> int:
    try:
        KillSwitchManager().ensure_default_state()
    except Exception:
        pass
    return control_tower_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
