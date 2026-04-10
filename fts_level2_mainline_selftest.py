# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib

MODULES = [
    'fts_pipeline',
    'master_pipeline',
    'yahoo_csv_to_sql',
]


def main() -> int:
    for name in MODULES:
        mod = importlib.import_module(name)
        print(f'OK => {name} :: {getattr(mod, "__file__", "")}')
    print('INFO => fts_legacy_master_pipeline_impl.py is runtime-loaded by Level-2 mainline to avoid eager heavy dependency import.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
