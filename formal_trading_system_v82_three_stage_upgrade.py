# -*- coding: utf-8 -*-
"""Compatibility wrapper.
v82 三階段入口已收編到 v83 正式交易主控版主線。
"""
from formal_trading_system_v83_official_main import FormalTradingSystemV83OfficialMain


def main() -> int:
    FormalTradingSystemV83OfficialMain().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
