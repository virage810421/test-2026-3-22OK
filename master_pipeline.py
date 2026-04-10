# -*- coding: utf-8 -*-
from __future__ import annotations

"""Level-2 wrapper for legacy master_pipeline.

保留舊門牌 master_pipeline.py，
真正主線改由 fts_pipeline.run_level2_mainline 接管；
舊版完整研究/決策實作保存在 fts_legacy_master_pipeline_impl.py。
"""

from fts_pipeline import run_level2_mainline
from fts_utils import log

BRIDGE_LEVEL = 'level_2'
BRIDGE_TARGET = 'fts_pipeline.run_level2_mainline'
LEGACY_IMPL = 'fts_legacy_master_pipeline_impl.py'


def main(execute_legacy: bool = True) -> int:
    path, payload = run_level2_mainline(execute_legacy=execute_legacy)
    log(f'✅ master_pipeline 第二級主線完成：{path}')
    return 0 if payload.get('status') in {'mainline_ready', 'mainline_degraded'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
