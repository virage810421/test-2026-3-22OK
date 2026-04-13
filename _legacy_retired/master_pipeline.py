# -*- coding: utf-8 -*-
from __future__ import annotations

"""Legacy CLI facade for master_pipeline.

新主線請直接走 fts_pipeline.run_level2_mainline。
"""

import warnings

from fts_pipeline import run_level2_mainline
from fts_utils import log

LEGACY_FACADE = True
SERVICE_ENTRYPOINT = 'fts_pipeline.run_level2_mainline'
LEGACY_IMPL = 'fts_legacy_master_pipeline_impl.py'


def main(execute_legacy: bool = True) -> int:
    warnings.warn('master_pipeline.py 已退役為 legacy facade；新主線請直接呼叫 fts_pipeline.run_level2_mainline。', DeprecationWarning, stacklevel=2)
    path, payload = run_level2_mainline(execute_legacy=execute_legacy)
    log(f'✅ master_pipeline 主線完成：{path}')
    return 0 if payload.get('status') in {'mainline_ready', 'mainline_degraded'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
