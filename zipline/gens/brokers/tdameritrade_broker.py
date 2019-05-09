from zipline.gens.brokers.broker import Broker
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from urlparse import parse_qs
import requests
import ssl

import zipline.protocol as zp
import pandas as pd
import numpy as np

from logbook import Logger
import sys

class TDAmeritradeBroker(Broker):
    def __init__(self):
        pass

    def subscribe_to_market_data(self, asset):
        ''' Do nothing '''
        pass

    def subscribed_assets(self):
        ''' Do nothing '''
        return []

    @property
    def positions(self):
        pass

    @property
    def portfolio(self):
        pass

    @property
    def account(self):
        pass

    @property
    def time_skew(self):
        pass

    @property
    def orders(self):
        pass

    @property
    def transactions(self):
        pass

    def is_alive(self):
        pass

    def order(self, asset, amount, style):
        pass

    def cancel_order(self, order_param):
        pass

    def get_last_traded_dt(self, asset):
        pass

    def get_spot_value(self, assets, field, dt, data_frequency):
       pass

    def get_realtime_bars(self, assets, data_frequency):
        pass