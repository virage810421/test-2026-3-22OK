# -*- coding: utf-8 -*-
"""Load non-destructive level-2 runtime variants from advanced_chart(1).zip."""
from __future__ import annotations

import importlib
from fts_level2_variant_registry import VARIANT_PACKAGE, VARIANT_MODULES


def load_variant(module_name: str):
    if module_name not in VARIANT_MODULES:
        raise KeyError(f"unknown level-2 variant: {module_name}")
    return importlib.import_module(f"{VARIANT_PACKAGE}.{module_name}")


def available_variants():
    return sorted(VARIANT_MODULES)
