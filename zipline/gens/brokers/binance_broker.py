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
        self.managed_accounts = None
        self.sockets = {}
        self.bars = {}
        self.time_skew = None
        self.bm = BinanceSocketManager(self.client)     # Creates the websocket manager
        self.connect()

    def connect(self):
        log.info("Connecting to Binance Exchange:")
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

    def _add_bar(self, symbol, last_trade_time, o, h, l, c, v):
        bar = pd.DataFrame(index=pd.DatetimeIndex([last_trade_time]),
                           data={'open': o,
                                 'high': h,
                                 'low': l,
                                 'close': c,
                                 'volume': v})

        # open, high, low, close, volume
        if symbol not in self.bars:
            self.bars[symbol] = bar
        else:
            self.bars[symbol] = self.bars[symbol].append(bar)

    def realtimeData(self, msg):
        if msg['e'] == "error":
            log.error("Websocket Received an Error")
            self.bm.close()
            self.connect()

        elif msg['e'] == "kline":
            asset = msg['s']
            log.info("Received {} Kline".format(asset))
            kline_data = msg['k']
            close_time = msg['T']
            open_p = kline_data['o']
            high = kline_data['h']
            low = kline_data['l']
            close = kline_data['c']
            volume = kline_data['v']
            self._process_tick(asset, close_time, open_p, high, low, close, volume)

    def _process_tick(self, asset, close_time, open_p, high, low, close, volume):

        last_trade_dt = pd.to_datetime(float(close_time), unit='ms', utc=True)

        self._add_bar(asset, last_trade_dt, open_p, high, low, close, volume)

        # Send request to get OHLCV data
        log.info("{} {} {} {} {} {}", asset, open_p, high, low, close, volume)

    def subscribe_to_asset(self, asset, interval=Client.KLINE_INTERVAL_1MINUTE):
        self.bm.start_kline_socket(asset, self.realtimeData, interval)   # Need to create a callback function

    def get_time_skew(self):
        server_time = self.client.get_server_time()
        self.time_skew = (pd.to_datetime('now', utc=True) -
                          pd.to_datetime(long(server_time), unit='ms', utc=True))

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
        self._subscribed_assets = []

    def subscribe_to_market_data(self, asset, interval='1m'):
        if asset not in self._subscribed_assets:
            self.binance_socket.subscribe_to_asset(str(asset.symbol), interval)
            self._subscribed_assets.append(asset)

    @property
    def subscribed_assets(self):
        return self._subscribed_assets

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
        return self.binance_socket.get_time_skew()

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
        symbol = str(assets.symbol)

        assert(field in ('open', 'high', 'low', 'close', 'volume', 'price', 'last_traded'))

        self.subscribe_to_market_data(assets)

        bars = self.binance_socket.bars[symbol]

        last_event_time = bars.index[-1]

        minute_start = (last_event_time - pd.Timedelta('1 min')) \
            .time()
        minute_end = last_event_time.time()

        if bars.empty:
            return pd.NaT if field == 'last_traded' else np.NaN
        else:
            if field == 'price':
                return bars.close.iloc[-1]
            elif field == 'last_traded':
                return last_event_time or pd.NaT

            minute_df = bars.between_time(minute_start, minute_end, include_start=True, include_end=True)

            if minute_df.empty:
                return np.NaN
            else:
                if field == 'open':
                    return minute_df.open.iloc[-1]
                elif field == 'close':
                    return minute_df.close.iloc[-1]
                elif field == 'high':
                    return minute_df.high.iloc[-1]
                elif field == 'low':
                    return minute_df.low.iloc[-1]
                elif field == 'volume':
                    return minute_df.volume.iloc[-1]

    def get_realtime_bars(self, assets, frequency):
        if frequency == '1m':
            resample_freq = '1 Min'
        elif frequency == '1d':
            resample_freq = '24 H'
        else:
            raise ValueError("Invalid frequency specified: %s" % frequency)

        df = pd.DataFrame()

        for asset in assets:
            symbol = str(asset.symbol)
            # Subscribe to market data
            self.subscribe_to_market_data(asset)

            asset_df = self.binance_socket.bars[symbol].copy()
            resample_df = asset_df.resample(resample_freq).sum()

            resample_df.columns = pd.MultiIndex.from_product([[asset, ],
                                                              resample_df.columns])

            df = pd.concat([df, resample_df], axis=1)

        return df
