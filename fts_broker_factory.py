# -*- coding: utf-8 -*-
from fts_config import CONFIG
from fts_broker_paper import PaperBroker
def create_broker():
    return PaperBroker(CONFIG.starting_cash)
