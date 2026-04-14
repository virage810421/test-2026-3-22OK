# -*- coding: utf-8 -*-
from __future__ import annotations

from fts_admin_cli import run_sync_feature_snapshots


def main(argv=None) -> int:
    return int(run_sync_feature_snapshots(argv) or 0)


if __name__ == '__main__':
    raise SystemExit(main())
