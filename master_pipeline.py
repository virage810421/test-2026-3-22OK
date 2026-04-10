# -*- coding: utf-8 -*-
"""Level-2 compatibility wrapper for master_pipeline.

舊門牌 master_pipeline.py 保留；
真正的全主線整合入口改由 fts_pipeline.Level2MainlinePipeline 提供。
原始研究/決策主體已封存到 fts_legacy_master_pipeline_impl.py。
"""
from __future__ import annotations

from fts_pipeline import Level2MainlinePipeline, main as _fts_pipeline_main

BRIDGE_LEVEL = 'level_2'
BRIDGE_TARGET = 'fts_pipeline.Level2MainlinePipeline'
LEGACY_SOURCE = 'master_pipeline.py -> fts_legacy_master_pipeline_impl.py'


def run_level2_mainline():
    return Level2MainlinePipeline().run()


def main():
    return _fts_pipeline_main()


if __name__ == '__main__':
    raise SystemExit(main())
