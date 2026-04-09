# -*- coding: utf-8 -*-
from fts_config import CONFIG
from fts_broker_paper import PaperBroker
from fts_broker_real_stub import RealBrokerStub
from fts_broker_api_adapter import ConfigurableBrokerAdapter


def create_broker():
    broker_type = str(getattr(CONFIG, 'broker_type', 'paper')).strip().lower()
    if broker_type in ('real', 'live', 'broker', 'adapter'):
        adapter = ConfigurableBrokerAdapter()
        _, probe = adapter.probe()
        if probe.get('ready_for_live_connect'):
            return adapter
        return RealBrokerStub(credentials={})
    return PaperBroker(CONFIG.starting_cash)
