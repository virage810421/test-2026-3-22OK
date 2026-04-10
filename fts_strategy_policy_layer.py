# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    from fts_config import PATHS, CONFIG  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
    class _Config:
        strategy_policy_mode = 'explicit'
        strategy_policy_filename = 'strategy_policy_book.json'
    PATHS = _Paths()
    CONFIG = _Config()

_POLICY_BOOK: dict[str, dict[str, dict[str, Any]]] = {
    '趨勢多頭': {
        '多': {'strategy': 'TrendBreakoutStrategy', 'name': '趨勢多頭攻堅', 'multiplier': 1.25, 'min_proba': 0.54, 'stop_scale': 1.00, 'tp_scale': 1.15, 'allow_ignore_tp': True, 'playbook': 'trend_follow_breakout'},
        '空': {'strategy': 'DefensiveStrategy', 'name': '逆勢防守空單', 'multiplier': 0.65, 'min_proba': 0.60, 'stop_scale': 0.85, 'tp_scale': 0.75, 'allow_ignore_tp': False, 'playbook': 'counter_trend_defense'},
        '其他': {'strategy': 'TrendBreakoutStrategy', 'name': '趨勢跟隨', 'multiplier': 1.00, 'min_proba': 0.55, 'stop_scale': 1.00, 'tp_scale': 1.00, 'allow_ignore_tp': False, 'playbook': 'trend_follow_core'},
    },
    '區間盤整': {
        '多': {'strategy': 'MeanReversionStrategy', 'name': '盤整低吸', 'multiplier': 0.92, 'min_proba': 0.53, 'stop_scale': 0.85, 'tp_scale': 0.80, 'allow_ignore_tp': False, 'playbook': 'range_buy_revert'},
        '空': {'strategy': 'MeanReversionStrategy', 'name': '盤整高拋', 'multiplier': 0.92, 'min_proba': 0.53, 'stop_scale': 0.85, 'tp_scale': 0.80, 'allow_ignore_tp': False, 'playbook': 'range_sell_revert'},
        '其他': {'strategy': 'MeanReversionStrategy', 'name': '盤整均值回歸', 'multiplier': 0.90, 'min_proba': 0.53, 'stop_scale': 0.80, 'tp_scale': 0.70, 'allow_ignore_tp': False, 'playbook': 'range_reversion_core'},
    },
    '趨勢空頭': {
        '多': {'strategy': 'DefensiveStrategy', 'name': '逆勢反彈防守', 'multiplier': 0.70, 'min_proba': 0.60, 'stop_scale': 0.80, 'tp_scale': 0.65, 'allow_ignore_tp': False, 'playbook': 'bear_counter_trend'},
        '空': {'strategy': 'TrendBreakoutStrategy', 'name': '趨勢空頭追擊', 'multiplier': 1.20, 'min_proba': 0.55, 'stop_scale': 1.00, 'tp_scale': 1.10, 'allow_ignore_tp': True, 'playbook': 'bear_breakdown_follow'},
        '其他': {'strategy': 'DefensiveStrategy', 'name': '空頭保守應對', 'multiplier': 0.80, 'min_proba': 0.57, 'stop_scale': 0.75, 'tp_scale': 0.75, 'allow_ignore_tp': False, 'playbook': 'bear_defense_core'},
    },
}


@dataclass
class StrategyPolicy:
    regime: str
    side: str
    setup_tag: str
    strategy: str
    name: str
    multiplier: float
    min_proba: float
    stop_scale: float
    tp_scale: float
    allow_ignore_tp: bool
    playbook: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class BaseStrategy:
    def __init__(self, policy: StrategyPolicy | dict[str, Any] | None = None):
        self.strategy_name = '傳統波段策略'
        self.base_mult = 1.0
        self.min_proba = 0.50
        self.stop_scale = 1.0
        self.tp_scale = 1.0
        self.allow_ignore_tp = True
        self.playbook = 'base_playbook'
        self.policy = policy.as_dict() if isinstance(policy, StrategyPolicy) else (policy or {})
        self.apply_policy(self.policy)

    def apply_policy(self, policy: dict[str, Any]) -> None:
        if not policy:
            return
        self.strategy_name = policy.get('name', self.strategy_name)
        self.base_mult = float(policy.get('multiplier', self.base_mult))
        self.min_proba = float(policy.get('min_proba', self.min_proba))
        self.stop_scale = float(policy.get('stop_scale', self.stop_scale))
        self.tp_scale = float(policy.get('tp_scale', self.tp_scale))
        self.allow_ignore_tp = bool(policy.get('allow_ignore_tp', self.allow_ignore_tp))
        self.playbook = str(policy.get('playbook', self.playbook))

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params['SL_MIN_PCT'], min(volatility_pct * self.stop_scale, sys_params['SL_MAX_PCT']))
        tp_raw = sys_params['TP_TREND_PCT'] if (trend_is_with_me and adx_is_strong) else sys_params['TP_BASE_PCT']
        tp = max(tp_raw * self.tp_scale, sys_params['TP_BASE_PCT'] * 0.5)
        ignore_tp = bool(self.allow_ignore_tp and entry_score >= 8)
        return sl, tp, ignore_tp


class TrendBreakoutStrategy(BaseStrategy):
    def __init__(self, policy: StrategyPolicy | dict[str, Any] | None = None):
        super().__init__(policy=policy)
        if not policy:
            self.strategy_name = '趨勢突破'
            self.base_mult = 1.2
            self.min_proba = 0.55


class MeanReversionStrategy(BaseStrategy):
    def __init__(self, policy: StrategyPolicy | dict[str, Any] | None = None):
        super().__init__(policy=policy)
        if not policy:
            self.strategy_name = '均值回歸'
            self.base_mult = 0.9
            self.min_proba = 0.53

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params['SL_MIN_PCT'], min(volatility_pct * 0.8 * self.stop_scale, sys_params['SL_MAX_PCT']))
        tp = max(sys_params['TP_BASE_PCT'] * 0.7 * self.tp_scale, 0.05)
        return sl, tp, False


class DefensiveStrategy(BaseStrategy):
    def __init__(self, policy: StrategyPolicy | dict[str, Any] | None = None):
        super().__init__(policy=policy)
        if not policy:
            self.strategy_name = '防禦反擊'
            self.base_mult = 0.7
            self.min_proba = 0.58

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params['SL_MIN_PCT'], min(volatility_pct * 0.7 * self.stop_scale, sys_params['SL_MAX_PCT']))
        tp = max(sys_params['TP_BASE_PCT'] * 0.6 * self.tp_scale, 0.04)
        return sl, tp, False


def infer_side_from_setup(setup_tag: str) -> str:
    tag = str(setup_tag)
    if '空' in tag or 'SHORT' in tag.upper():
        return '空'
    if '多' in tag or 'LONG' in tag.upper():
        return '多'
    return '其他'


def get_strategy_policy(setup_tag: str, regime: str = '未知') -> dict[str, Any]:
    side = infer_side_from_setup(setup_tag)
    regime_book = _POLICY_BOOK.get(str(regime).strip(), _POLICY_BOOK['區間盤整'])
    base = regime_book.get(side, regime_book['其他']).copy()
    policy = StrategyPolicy(regime=str(regime).strip(), side=side, setup_tag=str(setup_tag), **base)
    return policy.as_dict()


def describe_strategy_policy(setup_tag: str, regime: str = '未知') -> str:
    policy = get_strategy_policy(setup_tag, regime=regime)
    return (
        f"{policy['name']} | regime={policy['regime']} | side={policy['side']} | "
        f"min_proba={policy['min_proba']:.2f} | multiplier={policy['multiplier']:.2f} | "
        f"playbook={policy['playbook']}"
    )


def get_active_strategy(setup_tag: str, regime: str = '未知'):
    policy = get_strategy_policy(setup_tag, regime=regime)
    strat_name = policy.get('strategy', 'MeanReversionStrategy')
    if strat_name == 'DefensiveStrategy':
        return DefensiveStrategy(policy=policy)
    if strat_name == 'TrendBreakoutStrategy':
        return TrendBreakoutStrategy(policy=policy)
    return MeanReversionStrategy(policy=policy)


def export_policy_runtime() -> Path:
    runtime_dir = Path(getattr(PATHS, 'runtime_dir', Path('runtime')))
    runtime_dir.mkdir(parents=True, exist_ok=True)
    out = runtime_dir / getattr(CONFIG, 'strategy_policy_filename', 'strategy_policy_book.json')
    payload = {
        'policy_mode': getattr(CONFIG, 'strategy_policy_mode', 'explicit'),
        'generated_from': 'fts_strategy_policy_layer.py',
        'regime_count': len(_POLICY_BOOK),
        'policies': _POLICY_BOOK,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return out


export_policy_runtime()
