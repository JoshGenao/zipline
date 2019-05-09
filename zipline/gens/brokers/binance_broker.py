from zipline.gens.brokers.broker import Broker
from binance.client import Client
from binance.websockets import BinanceSocketManager
from zipline.finance.order import (Order as ZPOrder,
                                   ORDER_STATUS as ZP_ORDER_STATUS)

from zipline.finance.exchange_execution import (MarketOrder,
                                                LimitOrder,
                                                StopOrder,
                                                StopLimitOrder)

import zipline.protocol as zp

from urlparse import parse_qs
import requests
import os

import zipline.protocol as zp
import pandas as pd
import numpy as np

from logbook import Logger
import sys

log = Logger('Binance Exchange')

class BinanceConnection():
    def __init__(self, client):
        self.client = client    # Binance API client
        self._host = "stream.binance.com"
        self._port = "9443"
        self.managed_accounts = None
        self.sockets = {}
        self.time_skew = None
        self.bm = BinanceSocketManager(self.client)     # Creates the websocket manager
        self.connect()
        pass

    def connect(self):
        log.info("Connecting: {}:{}".format(self._host, self._port))
        #for symbol in symbols:
        if self.bm.start_user_socket(self._queue_msg):      # start the account user socket
            log.info("Started User socket")
        else:
            log.info("Error Starting User Socket")

        self.bm.start()

    def _queue_msg(self, msg):
        if msg['e'] == "error":
            log.error("Websocket Received an Error")
            self.bm.close()
            self.connect()
        elif msg['e'] == "outboundAccountInfo":
            # process balance
            log.info("Received Account Info")
            # msg['B']
        elif msg['e'] == "executionReport":
            log.info("Received Execution Report")

    def _download_account_details(self):
        pass

    def _get_fiat_portfolio_value(self):
        pass

    def _get_BTC_pairs(self):
        pass

    def _get_open_positions(self):
        pass


class BinanceBroker(Broker):
    def __init__(self, api_key=None, api_secret=None):
        binance_api_key = api_key
        binance_api_secret = api_secret

        '''
        I suggest not to save api keys as an environment variable since it is not secure.
        If using AWS, look into AWS SSM Parameter Store or AWS SecretManager
        '''
        if api_key is None:
            try:
                binance_api_key = os.environ['BINANCE_API_KEY_ID']
            except KeyError:
                log.error('Load in Binance API Key')

        if api_secret is None:
            try:
                binance_api_secret = os.environ['BINANCE_API_SECRET_KEY_ID']
            except KeyError:
                log.error('Load in Binance Secret API Key')

        self._api = Client(binance_api_key, binance_api_secret)
        self.binance_socket = BinanceConnection(self._api)
        self.fiat_currency = "USD"
        pass

    def subscribe_to_market_data(self, asset):
        ''' Do nothing '''
        pass

    def subscribed_assets(self):
        ''' Do nothing '''
        return []

    @property
    def positions(self):
        '''Get a list of open positions'''
        z_positions = zp.Positions()
        account = self._api.get_account()
        assets = filter(lambda coins: coins['free'] > u'0.00000000', account['balances'])
        positions = [li['asset'] for li in assets]
        for pos in positions:
            z_position = zp.Position(pos)
            #z_position.amount =
            #z_position.cost_basis = float()
            z_position.last_sale_price = None
            z_position.last_sale_date = None
            z_positions[pos] = z_position


    @property
    def portfolio(self):
        account = self._api.get_account()
        z_portfolio = zp.Portfolio()
        pass

    @property
    def account(self):
        pass

    @property
    def time_skew(self):
        return pd.Timedelta("0 sec")    # TODO: Determine timedelta

    @property
    def orders(self):
        pass

    @property
    def transactions(self):
        pass

    def is_alive(self):
        pass

    def order(self, asset, amount, style):
        symbol = asset.symbol
        qty = amount if amount > 0 else -amount
        side = 'buy' if amount > 0 else 'sell'

        limit_price = style.get_limit_price(side == 'buy') or None
        stop_price = style.get_stop_price(side == 'buy') or None

        order_id = 0 #TODO: Get order id
        dt = pd.to_datetime('now', utc=True)

        order_type = 'market'
        if isinstance(style, MarketOrder):
            order_type = 'market'
        elif isinstance(style, LimitOrder):
            order_type = 'limit'
        elif isinstance(style, StopOrder):
            order_type = 'stop'
        elif isinstance(style, StopLimitOrder):
            order_type = 'stop_limit'

        #TODO: Add in Binance order types: TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER

        order = ZPOrder(
            dt=dt,
            asset=asset,
            amount=amount,
            stop=stop_price,
            limit=limit_price,
            id=order_id
        )

        time_in_force = "GTC"   #   GTC = Good Till Cancelled
                                #   FOK = Fill or Kill
                                #   IOC = Immediate or Cancel

        #TODO: Call api to place order

        log.info("Placing order-{order_id}: "
            "{action} {qty} {symbol} with {order_type} order. "
            "limit_price={limit_price} stop_price={stop_price} {tif}".format(
                order_id=order_id,
                action=side,
                qty=qty,
                symbol=symbol,
                order_type=order_type,
                limit_price=limit_price,
                stop_price=stop_price,
                tif=time_in_force
            ))
        return order

    def cancel_order(self, order_param):
        pass

    def get_last_traded_dt(self, asset):
        pass

    def get_spot_value(self, assets, field, dt, data_frequency):
        pass

    def get_realtime_bars(self, assets, data_frequency):
        pass