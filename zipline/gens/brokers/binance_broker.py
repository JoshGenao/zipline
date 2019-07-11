from zipline.gens.brokers.broker import Broker
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from binance.websockets import BinanceSocketManager
from zipline.finance.order import (Order as ZPOrder,
                                   ORDER_STATUS as ZP_ORDER_STATUS)

from zipline.finance.exchange_execution import (MarketOrder,
                                                LimitOrder,
                                                StopOrder,
                                                StopLimitOrder)
from zipline.finance.transaction import Transaction
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
            log.info("Received Order Update")
            # Update Order Information
            self._update_order()

    def _update_order(self, order_id, status, ):
        pass

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

    def realtime_data(self, msg):
        if msg['e'] == "error":
            log.error("Websocket Received an Error")
            self.bm.close()
            self.connect()

        elif msg['e'] == "kline":
            asset = str(msg['s'])
            log.info("Received {} Kline".format(asset))
            kline_data = msg['k']
            close_time = msg['T']
            open_p = float(kline_data['o'])
            high = float(kline_data['h'])
            low = float(kline_data['l'])
            close = float(kline_data['c'])
            volume = kline_data['v']
            self._process_tick(asset, close_time, open_p, high, low, close, volume)

    def _process_tick(self, asset, close_time, open_p, high, low, close, volume):
        last_trade_dt = pd.to_datetime(float(close_time), unit='ms', utc=True)

        self._add_bar(asset, last_trade_dt, open_p, high, low, close, volume)

        # Send request to get OHLCV data
        log.info("{} {} {} {} {} {}", asset, open_p, high, low, close, volume)

    def subscribe_to_asset(self, asset, interval=Client.KLINE_INTERVAL_1MINUTE):
        self.bm.start_kline_socket(asset, self.realtime_data, interval)   # Need to create a callback function

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
        z_positions = zp.Positions()
        account = self._api.get_account()
        assets = filter(lambda coins: float(coins['free']) > 0.00000000, account['balances'])
        positions = [li['asset'] for li in assets]
        for pos in positions:
            symbol = str(pos['asset'])
            z_position = zp.Position(symbol)
            z_position.amount = float(pos['free'])
            btc_pair = symbol + 'BTC'
            try:
                trade = self._api.get_my_trades(symbol=btc_pair)[-1]
                z_position.cost_basis = float(trade['price'])
            except BinanceAPIException:
                continue

            if symbol in self.binance_socket.bars:
                z_position.last_sale_price = float(self.binance_socket.bars[symbol].last_trade_price.iloc[-1])
                z_position.last_sale_date = float(self.binance_socket.bars[symbol].index.values[-1])
            else:
                z_position.last_sale_price = None
                z_position.last_sale_date = None

            z_positions[pos] = z_position
            return z_positions

    @property
    def portfolio(self):
        account = self._api.get_account()
        z_portfolio = zp.Portfolio()
        y = filter(lambda x: x['asset'] == 'BTC', account['balances'])
        z_portfolio.cash = float(y[0]['free'])
        z_portfolio.positions = self.positions
        z_portfolio.positions_value = self.get_position_value(account)
        return z_portfolio

    @property
    def account(self):
        binance_account = self._api.get_account()
        z_account = zp.Account()
        y = filter(lambda x: x['asset'] == 'BTC', binance_account['balances'])
        z_account.buying_power = float(y[0]['free'])
        z_account.total_positions_value = self.get_position_value(binance_account['balances'])
        return z_account

    def get_position_value(self, balance):
        value = 0
        all_tickers = self._api.get_all_tickers()   # get pricing data for all pairs
        for asset in balance:
            # Calculate the position value in BTC?
            if float(asset['free']) > 0.00000000:
                if asset['asset'] != 'BTC':
                    asset_pair = asset['asset'] + 'BTC'
                    data = filter(lambda y: y['symbol'] == asset_pair, all_tickers)
                    asset_value = float(asset['free']) * float(data['price'])
                    value += asset_value
                else:
                    value += float(asset['free'])
        return value


    @property
    def time_skew(self):
        return self.binance_socket.get_time_skew()

    @property
    def orders(self):
        return {
            o.orderId: self._order2zp(o)
            for o in self._api.get_all_orders()
        }

    @property
    def transactions(self):
        orders = self._api.get_all_orders()
        results = {}
        for o in orders:
            if o.status == self._api.ORDER_STATUS_FILLED:
                tx = Transaction(
                    asset=o.symbol,
                    amount=o.executedQty,
                    dt=pd.to_datetime(float(o.time), unit='ms', utc=True),
                    price=float(o.price),
                    order_id=o.orderId,
                    commission=0.0,
                )
                results[o.orderId] = tx
        return results

    def is_alive(self):
        try:
            self._api.ping()
            return True
        except (BinanceAPIException, BinanceRequestException):
            return False

    def _order2zp(self, order):
        zp_order = ZPOrder(
            id=order.orderId,
            asset=order.symbol,
            amount=float(order.origQty) if order.side == self._api.SIDE_BUY else -float(order.origQty),
            stop=float(order.stopPrice) if order.type == self._api.ORDER_TYPE_STOP_LOSS else None,
            limit=float(order.price) if order.type == self._api.ORDER_TYPE_LIMIT else None,
            dt=pd.to_datetime(float(order.time), unit='ms', utc=True),
            commission=0,
        )

        zp_order.status = ZP_ORDER_STATUS.OPEN
        if order.status == self._api.ORDER_STATUS_CANCELED:
            order.status = ZP_ORDER_STATUS.CANCELLED
        if order.status == self._api.ORDER_STATUS_REJECTED:
            order.status = ZP_ORDER_STATUS.REJECTED
        if order.status == self._api.ORDER_STATUS_FILLED:
            order.status = ZP_ORDER_STATUS.FILLED
            order.filled = int(order.executedQty)

        return zp_order

    def order(self, asset, amount, style):
        symbol = asset.symbol
        is_buy = amount > 0
        qty = amount if is_buy else -amount
        side = 'BUY' if is_buy else 'SELL'

        limit_price = style.get_limit_price(is_buy) or 0
        stop_price = style.get_stop_price(is_buy) or 0

        order_type = 'MARKET'
        if isinstance(style, MarketOrder):
            order_type = 'MARKET'
        elif isinstance(style, LimitOrder):
            order_type = 'LIMIT'
        elif isinstance(style, StopOrder):
            order_type = 'STOP_LOSS'
        elif isinstance(style, StopLimitOrder):
            order_type = 'STOP_LOSS_LIMIT'

        time_in_force = "GTC"   # GTC = Good Till Cancelled, FOK = Fill or Kill, IOC = Immediate or Cancel

        try:
            response=''

            if order_type == 'MARKET':
                response = self._api.create_order(symbol=symbol,
                                                  side=side,
                                                  type=order_type,
                                                  timeInForce=time_in_force,
                                                  quantity=amount)
            elif order_type == 'LIMIT':
                response = self._api.create_order(symbol=symbol,
                                                  side=side,
                                                  type=order_type,
                                                  timeInForce=time_in_force,
                                                  quantity=amount,
                                                  price=limit_price)
            elif order_type == 'STOP_LOSS':
                response = self._api.create_order(symbol=symbol,
                                                  side=side,
                                                  type=order_type,
                                                  timeInForce=time_in_force,
                                                  quantity=amount,
                                                  stopPrice=stop_price)
            elif order_type == 'STOP_LOSS_LIMIT':
                response = self._api.create_order(symbol=symbol,
                                                  side=side,
                                                  type=order_type,
                                                  timeInForce=time_in_force,
                                                  quantity=amount,
                                                  price=limit_price,
                                                  stopPrice=stop_price)

        except (BinanceRequestException, BinanceAPIException) as e:
            log.error("Order Error! {}:{}".format(symbol, e))

        dt = pd.to_datetime(float(response['transactTime']), unit='ms', utc=True)

        order = ZPOrder(
            dt=dt,
            asset=asset,
            amount=amount,
            stop=stop_price,
            limit=limit_price,
            id=response['orderId']
        )

        order.status = ZP_ORDER_STATUS.OPEN
        if response['status'] == self._api.ORDER_STATUS_CANCELED:
            order.status = ZP_ORDER_STATUS.CANCELLED
        if response['status'] == self._api.ORDER_STATUS_REJECTED:
            order.status = ZP_ORDER_STATUS.REJECTED
        if response['status'] == self._api.ORDER_STATUS_FILLED:
            order.status = ZP_ORDER_STATUS.FILLED
            order.filled = int(response['executedQty'])


        log.info("Placing order-{order_id}: "
            "{action} {qty} {symbol} with {order_type} order. "
            "limit_price={limit_price} stop_price={stop_price} {tif}".format(
                order_id=response['orderId'],
                action=side,
                qty=qty,
                symbol=symbol,
                order_type=order_type,
                limit_price=limit_price,
                stop_price=stop_price,
                tif=time_in_force
            ))

        return order

    def cancel_order(self, zp_order_id):
        try:
            order = self.orders[zp_order_id].id
            symbol = self.orders[zp_order_id].asset
            self._api.cancel_order(symbol=symbol, orderid=order)
        except (BinanceRequestException, BinanceAPIException) as e:
            log.error(e)
            return

    def get_last_traded_dt(self, asset):
        self.subscribe_to_market_data(asset)
        return self.binance_socket.bars[asset.symbol].index[-1]

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