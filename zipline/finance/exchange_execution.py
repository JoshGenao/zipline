# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from zipline.finance.execution import (MarketOrder,
                                       LimitOrder,
                                       StopOrder,
                                       StopLimitOrder)


class ExchangeMarketOrder(MarketOrder):
    def get_limit_price(self, _is_buy):
        return None

    def get_stop_price(self, _is_buy):
        return None


class ExchangeLimitOrder(LimitOrder):
    def get_limit_price(self, is_buy):
        """
        We may be trading Satoshis with 8 decimals, we cannot round numbers

        :param is_buy: bool
        :return: stop_price: float
        """
        return self.limit_price


class ExchangeStopOrder(StopOrder):
    def get_stop_price(self, is_buy):
        """
        We may be trading Satoshis with 8 decimals, we cannot round numbers

        :param is_buy: bool
        :return: stop_price: float
        """
        return self.stop_price


class ExchangeStopLimitOrder(StopLimitOrder):
    def get_limit_price(self, is_buy):
        """
        We may be trading Satoshis with 8 decimals, we cannot round numbers

        :param is_buy: bool
        :return: stop_price: float
        """
        return self.limit_price

    def get_stop_price(self, is_buy):
        """
        We may be trading Satoshis with 8 decimals, we cannot round numbers

        :param is_buy: bool
        :return: stop_price: float
        """
        return self.stop_price