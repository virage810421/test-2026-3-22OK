# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import log


class ProgressFullReport:
    def __init__(self):
        self.json_path = PATHS.runtime_dir / 'progress_full_report.json'
        self.md_path = PATHS.runtime_dir / 'progress_full_report.md'

    def build(self):
        log('📝 開始彙整完整進度報告...')
        def load(name):
            p = PATHS.runtime_dir / name
            if p.exists():
                return json.loads(p.read_text(encoding='utf-8'))
            return {}

        comp = load('completion_gap_report.json')
        train = load('training_orchestrator.json')
        execb = load('decision_execution_bridge.json')
        gap = load('price_gap_bridge.json')
        payload = {
            'completion_gap_report': comp,
            'training_orchestrator': train,
            'decision_execution_bridge': execb,
            'price_gap_bridge': gap,
            'config': {
                'package_version': CONFIG.package_version,
                'source_mount_dirs': [str(x) for x in PATHS.source_mount_dirs],
                'price_scan_dirs': [str(x) for x in PATHS.price_scan_dirs],
                'history_scan_dirs': [str(x) for x in PATHS.history_scan_dirs],
            }
        }
        self.json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

        missing_preview = ', '.join(gap.get('missing_tickers', [])[:10]) or '無'
        lines = [
            '# FTS 完整進度報告 v73',
            '',
            f"- 缺口數：{len(comp.get('remaining_hard_gaps', []))}",
            f"- 訓練資料存在：{train.get('dataset', {}).get('exists', False)}",
            f"- 模型檔齊全：{train.get('models', {}).get('all_required_present', False)}",
            f"- 有價格列數：{execb.get('rows_with_price', 0)}",
            f"- 有股數列數：{execb.get('rows_with_qty', 0)}",
            f"- 通過市場規則列數：{execb.get('rows_market_rule_passed', 0)}",
            f"- 自動價格候選列數：{gap.get('candidate_rows', 0)}",
            f"- 自動價格掃描成功檔數：{gap.get('scanned_csv_success_count', 0)}",
            f"- 尚缺價格 ticker：{missing_preview}",
            '',
            '## 掛載狀態',
            f"- 來源掛載資料夾：{', '.join(str(x) for x in PATHS.source_mount_dirs) or '未設定'}",
            f"- 價格掃描資料夾：{', '.join(str(x) for x in PATHS.price_scan_dirs[:8])}",
            f"- 歷史掃描資料夾：{', '.join(str(x) for x in PATHS.history_scan_dirs[:8])}",
            '',
            '## 目前最硬的缺口',
        ]
        for item in comp.get('remaining_hard_gaps', []):
            lines.append(f'- {item}')
        self.md_path.write_text('\n'.join(lines), encoding='utf-8')
        log(f'📝 已輸出完整進度報告：{self.md_path}')
        return self.md_path, payload
