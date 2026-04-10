# -*- coding: utf-8 -*-
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fts_upgrade_runtime import PATHS, now_str, write_json

NULLISH_TEXT = {
    '', 'na', 'n/a', 'nan', 'none', 'null', 'nat', '#n/a', '#na', 'inf', '-inf', 'infinity', '-infinity'
}
TRAINING_RUNTIME_PATH = PATHS.runtime_dir / 'training_data_quality_report.json'
ORDER_RUNTIME_PATH = PATHS.runtime_dir / 'order_contract_quality_report.json'


def _normalize_text(value: Any) -> str:
    return str(value or '').strip().lower()


def is_nullish(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    if isinstance(value, str) and _normalize_text(value) in NULLISH_TEXT:
        return True
    return False


def safe_float_or_none(value: Any, *, allow_zero: bool = True, precision: int = 6, min_value: float | None = None, max_value: float | None = None) -> float | None:
    if is_nullish(value):
        return None
    try:
        if isinstance(value, str):
            value = value.replace(',', '').replace('%', '').strip()
        num = float(value)
    except Exception:
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    if not allow_zero and abs(num) < 1e-12:
        return None
    if min_value is not None and num < min_value:
        return None
    if max_value is not None and num > max_value:
        return None
    return round(num, precision)


def safe_int_or_none(value: Any, *, allow_zero: bool = True, min_value: int | None = None, max_value: int | None = None) -> int | None:
    num = safe_float_or_none(value, allow_zero=allow_zero, precision=0)
    if num is None:
        return None
    out = int(round(num))
    if min_value is not None and out < min_value:
        return None
    if max_value is not None and out > max_value:
        return None
    return out


def sanitize_numeric_frame(df: pd.DataFrame, float_cols: list[str] | None = None, int_cols: list[str] | None = None) -> pd.DataFrame:
    out = df.copy()
    float_cols = float_cols or []
    int_cols = int_cols or []
    for col in float_cols:
        if col in out.columns:
            out[col] = out[col].apply(lambda x: safe_float_or_none(x))
    for col in int_cols:
        if col in out.columns:
            out[col] = out[col].apply(lambda x: safe_int_or_none(x))
    return out


def add_missing_flags(df: pd.DataFrame, cols: list[str], prefix: str = 'MISS__') -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[f'{prefix}{col}'] = out[col].isna().astype(int)
    return out


def build_frame_quality_summary(df: pd.DataFrame, *, required_cols: list[str] | None = None, dataset_name: str = 'dataset') -> dict[str, Any]:
    required_cols = required_cols or []
    missing_required_cols = [c for c in required_cols if c not in df.columns]
    null_rate = {}
    invalid_cols = {}
    for col in df.columns:
        try:
            rate = float(df[col].isna().mean()) if len(df) else 0.0
        except Exception:
            rate = 0.0
        if rate > 0:
            null_rate[col] = round(rate, 4)
        if col in required_cols and rate > 0:
            invalid_cols[col] = round(rate, 4)
    return {
        'generated_at': now_str(),
        'dataset_name': dataset_name,
        'rows': int(len(df)),
        'columns': int(len(df.columns)),
        'missing_required_columns': missing_required_cols,
        'required_columns_with_null_rate': invalid_cols,
        'columns_with_null_rate': null_rate,
    }


def sanitize_generated_training_df(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = df.copy()
    if out.empty:
        report = build_frame_quality_summary(out, required_cols=['Label_Y', 'Target_Return', 'Regime'], dataset_name='ml_training_generated')
        report['status'] = 'empty'
        write_json(TRAINING_RUNTIME_PATH, report)
        return out, report
    if 'Label_Y' not in out.columns and 'Label' in out.columns:
        out['Label_Y'] = out['Label']
    if 'Target_Return' not in out.columns and 'Future_Return_Pct' in out.columns:
        out['Target_Return'] = out['Future_Return_Pct']
    if 'Date' in out.columns:
        out['Date'] = pd.to_datetime(out['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
    numeric_candidates = []
    skip_object_cols = {'Ticker SYMBOL', 'Setup_Tag', 'Regime', 'Date'}
    for col in out.columns:
        if col in skip_object_cols:
            continue
        if out[col].dtype.kind in 'biufc' or col.startswith('MOUNT__') or col in {'Label', 'Label_Y', 'Future_Return_Pct', 'Target_Return'}:
            numeric_candidates.append(col)
    for col in numeric_candidates:
        out[col] = pd.to_numeric(out[col], errors='coerce').replace([np.inf, -np.inf], np.nan)
    missing_flag_cols = [c for c in numeric_candidates if out[c].isna().any()]
    out = add_missing_flags(out, missing_flag_cols)
    out = out.dropna(subset=[c for c in ['Label_Y', 'Target_Return', 'Regime'] if c in out.columns]).reset_index(drop=True)
    if 'Label_Y' in out.columns:
        out['Label_Y'] = pd.to_numeric(out['Label_Y'], errors='coerce').fillna(0).astype(int)
    report = build_frame_quality_summary(out, required_cols=['Label_Y', 'Target_Return', 'Regime'], dataset_name='ml_training_generated')
    report.update({
        'status': 'ok' if len(out) > 0 else 'blocked',
        'missing_flag_columns_added': missing_flag_cols,
    })
    write_json(TRAINING_RUNTIME_PATH, report)
    return out, report


def validate_training_frame(df: pd.DataFrame, *, min_rows: int = 80) -> tuple[pd.DataFrame, dict[str, Any]]:
    out, report = sanitize_generated_training_df(df)
    failures = []
    warnings = []
    if len(out) < min_rows:
        failures.append({'type': 'too_few_rows', 'rows': int(len(out)), 'min_rows': int(min_rows)})
    label_col = 'Label_Y' if 'Label_Y' in out.columns else 'Label'
    if label_col not in out.columns:
        failures.append({'type': 'missing_label_column', 'expected': ['Label_Y', 'Label']})
    else:
        if pd.to_numeric(out[label_col], errors='coerce').dropna().nunique() < 2:
            failures.append({'type': 'label_single_class'})
    if 'Target_Return' not in out.columns:
        failures.append({'type': 'missing_target_return'})
    else:
        zeros = int((pd.to_numeric(out['Target_Return'], errors='coerce').fillna(0.0).abs() < 1e-12).sum())
        if zeros > max(10, int(len(out) * 0.6)):
            warnings.append({'type': 'many_zero_target_return', 'count': zeros})
    report.update({
        'validation_failures': failures,
        'validation_warnings': warnings,
        'validated_rows': int(len(out)),
        'status': 'ok' if not failures else 'blocked',
    })
    write_json(TRAINING_RUNTIME_PATH, report)
    return out, report


def validate_order_contract_dict(order: dict[str, Any]) -> dict[str, Any]:
    ticker = str(order.get('ticker') or order.get('Ticker SYMBOL') or order.get('Ticker') or '').strip().upper()
    side = str(order.get('side') or order.get('Action') or '').strip().upper()
    qty = safe_int_or_none(order.get('qty', order.get('Target_Qty')), allow_zero=False, min_value=1)
    ref_price = safe_float_or_none(order.get('ref_price', order.get('Reference_Price')), allow_zero=False, min_value=0.0001)
    kelly_pos = safe_float_or_none(order.get('Kelly_Pos'), allow_zero=True, min_value=0.0, max_value=1.0) if 'Kelly_Pos' in order else None
    ai_proba = safe_float_or_none(order.get('AI_Proba'), allow_zero=True, min_value=0.0, max_value=1.0) if 'AI_Proba' in order else None
    failures = []
    warnings = []
    if not ticker:
        failures.append('missing_ticker')
    if not side:
        failures.append('missing_side')
    if qty is None:
        failures.append('invalid_qty')
    if ref_price is None:
        failures.append('invalid_ref_price')
    if kelly_pos is None and 'Kelly_Pos' in order:
        warnings.append('invalid_kelly_pos')
    if ai_proba is None and 'AI_Proba' in order:
        warnings.append('invalid_ai_proba')
    if qty is not None and qty <= 0:
        failures.append('non_positive_qty')
    if ref_price is not None and ref_price <= 0:
        failures.append('non_positive_price')
    payload = {
        'generated_at': now_str(),
        'ticker': ticker,
        'side': side,
        'qty': qty,
        'ref_price': ref_price,
        'failures': failures,
        'warnings': warnings,
        'passed': len(failures) == 0,
    }
    return payload


def append_order_quality_report(item: dict[str, Any]) -> Path:
    existing = []
    if ORDER_RUNTIME_PATH.exists():
        try:
            existing = pd.read_json(ORDER_RUNTIME_PATH).to_dict(orient='records')
        except Exception:
            existing = []
    existing.append(item)
    ORDER_RUNTIME_PATH.write_text(pd.DataFrame(existing).to_json(force_ascii=False, orient='records', indent=2), encoding='utf-8')
    return ORDER_RUNTIME_PATH
