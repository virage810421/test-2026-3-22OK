# -*- coding: utf-8 -*-
from fts_legacy_facade_cleanup import main

if __name__ == "__main__":
    import sys
    raise SystemExit(main(apply="--apply" in sys.argv))
