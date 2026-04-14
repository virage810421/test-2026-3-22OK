# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log
from fts_tests import PreflightTestSuite
from fts_screening_engine import ScreeningEngine
from fts_market_data_service import MarketDataService
from fts_feature_service import FeatureService
from fts_chip_enrichment_service import ChipEnrichmentService
from fts_sector_service import SectorService
from fts_system_guard_service import SystemGuardService
from fts_risk_gateway import RiskGateway
from fts_watchlist_service import WatchlistService
from fts_market_climate_service import MarketClimateService
from fts_decision_desk_builder import DecisionDeskBuilder
from fts_admin_suite import ABDiffAudit


class ABWaveUpgrade:
    MODULE_VERSION = 'v83_ab_wave_upgrade'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'ab_wave_upgrade.json'

    def run(self) -> tuple[Any, dict[str, Any]]:
        step1 = {
            'status': 'a_is_main_version',
            'system_name': CONFIG.system_name,
            'note': 'A 為主版本；B 只作零件來源，不整包覆蓋 A',
        }

        mkt_path, mkt = MarketDataService().build_summary()
        feat_path, feat = FeatureService().build_summary()
        chip_path, chip = ChipEnrichmentService().build_summary()
        wave1_path, wave1 = ScreeningEngine().build_summary()

        sector_path, sector = SectorService().build_summary()
        guard_path, guard = SystemGuardService().build_summary()
        risk_path, risk = RiskGateway().build_summary()

        wl_path, wl = WatchlistService().build_summary()
        climate_path, climate = MarketClimateService().build_summary()
        desk_path, desk = DecisionDeskBuilder().build_summary()

        diff_path, diff = ABDiffAudit().build()
        smoke = PreflightTestSuite().run()

        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'step1_a_main_version': step1,
            'step2_wave1_screening_absorption': {
                'status': 'complete',
                'outputs': [str(mkt_path), str(feat_path), str(chip_path), str(wave1_path)],
                'note': 'screening.py 核心能力已拆到 market/feature/chip/screening engine',
            },
            'step2_wave2_supporting_services': {
                'status': 'complete',
                'outputs': [str(sector_path), str(guard_path), str(risk_path)],
                'note': 'sector_classifier.py / system_guard.py / risk_gateway.py 已收成 service',
            },
            'step2_wave3_pipeline_rules': {
                'status': 'complete',
                'outputs': [str(wl_path), str(climate_path), str(desk_path)],
                'note': 'master_pipeline.py / live_paper_trading.py 的規則層已抽成 watchlist / market climate / decision desk / gates',
            },
            'step3_diff_patch_only': {
                'status': 'complete',
                'output': str(diff_path),
                'note': '對已收編模組採只補差異，不再整支重收',
            },
            'step4_smoke_tests': {
                'status': 'complete' if smoke.get('all_passed') else 'partial',
                'tests': smoke,
            },
            'status': 'ab_wave_upgrade_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🏗️ A/B 波次升級完成：{self.runtime_path}')
        return self.runtime_path, payload
