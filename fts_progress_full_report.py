# -*- coding: utf-8 -*-
import json
from typing import Dict, Any

from fts_config import PATHS
from fts_utils import now_str


class ProgressFullReportBuilder:
    MODULE_VERSION = "v69"

    def __init__(self):
        self.json_path = PATHS.runtime_dir / "progress_full_report.json"
        self.md_path = PATHS.runtime_dir / "progress_full_report.md"

    def _load_json(self, name: str) -> Dict[str, Any]:
        p = PATHS.runtime_dir / name
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding='utf-8'))
        except Exception:
            return {}

    def build(self):
        truth = self._load_json('upgrade_truth_report.json')
        live = self._load_json('live_readiness_gate.json')
        train = self._load_json('training_orchestrator.json')
        execb = self._load_json('decision_execution_bridge.json')
        gap = self._load_json('completion_gap_report.json')
        score = self._load_json('target95_scorecard.json')
        input_manifest = self._load_json('training_input_manifest.json')
        hist = self._load_json('local_history_bootstrap.json')

        pct95 = int(score.get('summary', {}).get('at_or_above_95', 0))
        total_modules = int(score.get('summary', {}).get('total_modules', 0))
        true_exec = next((x.get('status') for x in truth.get('truth_table', []) if x.get('layer') == 'Paper execution'), 'unknown')
        remaining = gap.get('remaining_excluding_real_broker', [])
        remaining_count = len(remaining)
        completion_ex_broker_pct = max(0, min(100, 100 - remaining_count * 8))

        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'summary': {
                'package_maturity': f"{pct95}/{total_modules} modules >=95" if total_modules else 'unknown',
                'true_execution_ready': true_exec,
                'live_ready': bool(live.get('live_ready', False)),
                'completion_excluding_real_broker_pct': completion_ex_broker_pct,
                'training_dataset_exists': bool(train.get('dataset', {}).get('exists', False)),
                'training_models_count': int(train.get('models', {}).get('existing_required_count', 0)),
                'price_rows_ready': int(execb.get('rows_with_price', 0)),
                'qty_rows_ready': int(execb.get('rows_with_qty', 0)),
                'market_rule_passed_rows': int(execb.get('rows_market_rule_passed', 0)),
                'modules_95_plus': pct95,
                'modules_total': total_modules,
                'local_universe_count': int(input_manifest.get('rows', {}).get('common_universe', 0)),
                'auto_price_scan_sources': int(len(execb.get('price_scan_files', []))),
                'history_cache_tickers': int(hist.get('cache_ticker_count', 0)),
                'history_missing_tickers': int(hist.get('missing_cache_ticker_count', 0)),
                'history_request_rows': int(hist.get('request_list_rows', 0)),
            },
            'remaining_hard_gaps': remaining,
            'next_priority': truth.get('next_milestones', []),
            'artifacts': {
                'training_bootstrap_recipe': str(PATHS.runtime_dir / 'training_bootstrap_recipe.json'),
                'training_input_manifest': str(PATHS.runtime_dir / 'training_input_manifest.json'),
                'training_bootstrap_universe': str(PATHS.data_dir / 'training_bootstrap_universe.csv'),
                'manual_price_template': str(PATHS.data_dir / 'manual_price_snapshot_template.csv'),
                'auto_price_candidates': str(PATHS.data_dir / 'auto_price_snapshot_candidates.csv'),
                'execution_payload': str(PATHS.data_dir / 'executable_order_payloads.csv'),
                'history_bootstrap_report': str(PATHS.runtime_dir / 'local_history_bootstrap.json'),
                'history_request_list': str(PATHS.data_dir / 'kline_cache_request_list.csv'),
            },
        }
        self.json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

        lines = [
            '# 正式交易系統完整進度報告 v69',
            '',
            f'- 生成時間：{payload["generated_at"]}',
            f'- 套件成熟度：{payload["summary"]["package_maturity"]}',
            f'- 扣除真券商後完成度：{completion_ex_broker_pct}%',
            f'- 真實可執行狀態：{payload["summary"]["true_execution_ready"]}',
            f'- Live readiness：{payload["summary"]["live_ready"]}',
            f'- 95 分以上模組：{pct95}/{total_modules}',
            '',
            '## 目前已完成',
            f'- 訓練資料存在：{payload["summary"]["training_dataset_exists"]}',
            f'- 已有模型數：{payload["summary"]["training_models_count"]}',
            f'- 本地交集訓練宇宙：{payload["summary"]["local_universe_count"]}',
            f'- 有價格列數：{payload["summary"]["price_rows_ready"]}',
            f'- 有股數列數：{payload["summary"]["qty_rows_ready"]}',
            f'- 通過市場規則列數：{payload["summary"]["market_rule_passed_rows"]}',
            f'- 自動價格掃描來源數：{payload["summary"]["auto_price_scan_sources"]}',
            f'- 本地 K 線快取檔數：{payload["summary"]["history_cache_tickers"]}',
            f'- 尚缺 K 線標的數：{payload["summary"]["history_missing_tickers"]}',
            f'- K 線請求清單列數：{payload["summary"]["history_request_rows"]}',
            '',
            '## 剩餘硬缺口',
        ]
        for item in remaining:
            lines.append(f'- {item}')
        lines.extend(['', '## 下一輪最優先'])
        for item in payload['next_priority']:
            lines.append(f'- {item}')
        lines.extend(['', '## 這輪新增輸出'])
        for k, v in payload['artifacts'].items():
            lines.append(f'- {k}: `{v}`')
        self.md_path.write_text('\n'.join(lines), encoding='utf-8')
        return self.json_path, payload
