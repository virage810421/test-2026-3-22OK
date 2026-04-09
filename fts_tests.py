# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_config import CONFIG, PATHS
from fts_utils import resolve_decision_csv, now_str, log
from fts_broker_real_stub import RealBrokerStub
from fts_models import Order, OrderSide, OrderStatus
from fts_screening_detachment_audit import ScreeningDetachmentAudit


class PreflightTestSuite:
    def run(self):
        tests = {
            'config_check': self._config_check(),
            'paths_check': self._paths_check(),
            'decision_file_check': self._decision_file_check(),
            'mock_broker_roundtrip': self._mock_broker_roundtrip(),
            'absorption_services_check': self._absorption_services_check(),
            'screening_detachment_check': self._screening_detachment_check(),
        }
        tests['all_passed'] = all(x['passed'] for x in tests.values())
        return tests

    def _config_check(self):
        errors = []
        if CONFIG.starting_cash <= 0:
            errors.append('starting_cash 必須 > 0')
        if CONFIG.max_single_position_pct <= 0 or CONFIG.max_single_position_pct > 1:
            errors.append('max_single_position_pct 必須介於 0~1')
        if CONFIG.max_order_notional <= 0:
            errors.append('max_order_notional 必須 > 0')
        return {'passed': len(errors) == 0, 'errors': errors}

    def _paths_check(self):
        errors = []
        if not PATHS.base_dir.exists(): errors.append('base_dir 不存在')
        if not PATHS.data_dir.exists(): errors.append('data_dir 不存在')
        if not PATHS.log_dir.exists(): errors.append('log_dir 不存在')
        if not PATHS.state_dir.exists(): errors.append('state_dir 不存在')
        if not PATHS.runtime_dir.exists(): errors.append('runtime_dir 不存在')
        return {'passed': len(errors) == 0, 'errors': errors}

    def _decision_file_check(self):
        p = resolve_decision_csv()
        exists = p.exists()
        return {'passed': exists, 'path': str(p), 'errors': [] if exists else ['找不到決策檔']}

    def _mock_broker_roundtrip(self):
        try:
            broker = RealBrokerStub(credentials={'simulation_mode': True})
            broker.connect()
            order = Order(
                order_id='TEST-ORDER-001',
                ticker='2330.TW',
                side=OrderSide.BUY,
                qty=1000,
                ref_price=150.0,
                submitted_price=150.0,
                status=OrderStatus.NEW,
                strategy_name='preflight',
                signal_score=1.0,
                ai_confidence=0.5,
                industry='半導體',
                created_at='',
                updated_at='',
            )
            placed, fills = broker.place_order(order)
            callbacks = broker.poll_callbacks(clear=False)
            passed = len(callbacks) >= 1 and placed.status in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}
            return {
                'passed': passed,
                'errors': [] if passed else ['mock broker roundtrip failed'],
                'fill_count': len(fills),
                'callback_count': len(callbacks),
                'status': str(placed.status),
            }
        except Exception as exc:
            return {'passed': False, 'errors': [str(exc)]}

    def _absorption_services_check(self):
        errors = []
        try:
            from fts_market_data_service import MarketDataService
            from fts_feature_service import FeatureService
            from fts_chip_enrichment_service import ChipEnrichmentService
            from fts_screening_engine import ScreeningEngine
            from fts_sector_service import SectorService
            from fts_system_guard_service import SystemGuardService
            from fts_risk_gateway import RiskGateway
            from fts_watchlist_service import WatchlistService
            from fts_market_climate_service import MarketClimateService
            from fts_decision_desk_builder import DecisionDeskBuilder
            MarketDataService(); FeatureService(); ChipEnrichmentService(); ScreeningEngine(); SectorService(); SystemGuardService(); RiskGateway(); WatchlistService(); MarketClimateService(); DecisionDeskBuilder()
        except Exception as exc:
            errors.append(str(exc))
        return {'passed': len(errors) == 0, 'errors': errors}

    def _screening_detachment_check(self):
        try:
            audit = ScreeningDetachmentAudit().run()[1]
            passed = len(audit.get('direct_legacy_fallback_modules', [])) == 0
            return {'passed': passed, 'errors': [] if passed else audit.get('direct_legacy_fallback_modules', []), 'audit': audit}
        except Exception as exc:
            return {'passed': False, 'errors': [str(exc)]}


class FTSTestSuite:
    MODULE_VERSION = 'v83_tests_de_screening'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'fts_tests.json'

    def run(self) -> tuple[Path, dict[str, Any]]:
        tests = PreflightTestSuite().run()
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'tests': tests,
            'screening_detachment_complete_for_scope': tests.get('screening_detachment_check', {}).get('passed', False),
            'status': 'tests_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧪 tests ready: {self.runtime_path}')
        return self.runtime_path, payload
