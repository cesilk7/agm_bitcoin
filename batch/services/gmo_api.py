from bs4 import BeautifulSoup
import json
from json import JSONDecodeError
import hashlib
import hmac
import time
import logging
import math
import websocket
from datetime import datetime, timedelta

import requests
from requests.exceptions import RequestException

from config import settings, constants

logger = logging.getLogger(__name__)

public_end_point = 'https://api.coin.z.com/public'
private_end_point = 'https://api.coin.z.com/private'


class Ticker(object):
    def __init__(self, timestamp, ask, bid, high, last, low, volume):
        self.timestamp = timestamp
        self.ask = float(ask)
        self.bid = float(bid)
        self.high = float(high)
        self.last = float(last)
        self.low = float(low)
        self.volume = float(volume)

    @property
    def mid_price(self):
        return (self.bid + self.ask) / 2

    @property
    def time(self):
        time = self.timestamp.replace('T', ' ').replace('Z', '')[:19]
        time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
        time += timedelta(hours=constants.DIFF_JST_FROM_UTC)
        return time

    def truncate_date_time(self, duration):
        ticker_time = self.time
        time_format = '%Y-%m-%d %H:%M'

        if duration == constants.DURATION_1M:
            pass
        elif duration == constants.DURATION_5M:
            new_min = math.floor(self.time.minute / 5) * 5
            ticker_time = datetime(
                ticker_time.year, ticker_time.month, ticker_time.day,
                ticker_time.hour, new_min)
        elif duration == constants.DURATION_15M:
            new_min = math.floor(self.time.minute / 15) * 15
            ticker_time = datetime(
                ticker_time.year, ticker_time.month, ticker_time.day,
                ticker_time.hour, new_min)
        elif duration == constants.DURATION_30M:
            new_min = math.floor(self.time.minute / 30) * 30
            ticker_time = datetime(
                ticker_time.year, ticker_time.month, ticker_time.day,
                ticker_time.hour, new_min)
        elif duration == constants.DURATION_1H:
            time_format = '%Y-%m-%d %H'
        else:
            logger.warning(
                'action=truncate_date_time error no datetime format')

        ticker_time = datetime.strftime(ticker_time, time_format)
        return datetime.strptime(ticker_time, time_format)


class PublicWebSocketApi(object):
    def __init__(self):
        websocket.enableTrace(True)
        self.ws_path = 'wss://api.coin.z.com/ws/public/v1'

    @staticmethod
    def on_open(ws):
        message = {
            "command": "subscribe",
            "channel": "ticker",
            "symbol": settings.symbol
        }
        ws.send(json.dumps(message))

    @staticmethod
    def on_message(ws, message):
        print(message)
        # ws.close()

    def get_real_time_ticker(self, on_message=None):
        if on_message is None:
            on_message = self.on_message

        wsapp = websocket.WebSocketApp(
            self.ws_path, on_open=self.on_open, on_message=on_message)
        wsapp.run_forever()


class ApiClient(object):
    def __init__(self, api_key=settings.api_key, secret_key=settings.secret_key):
        self.api_key = api_key
        self.secret_key = secret_key

    @staticmethod
    def get_ticker():
        url = public_end_point + '/v1/ticker?symbol=' + settings.symbol
        try:
            resp = requests.get(url)
        except RequestException as e:
            logger.error(f'action=get_ticker error={e}')
            raise
        return eval(json.dumps(resp.json()))['data'][0]

    def call_private_get_api(self, path, params=None):
        timestamp = '{0}000'.format(
            int(time.mktime(datetime.now().timetuple()))
        )
        method = 'GET'
        url = private_end_point + path
        text = timestamp + method + path
        sign = hmac.new(bytes(self.secret_key.encode('ascii')),
                        bytes(text.encode('ascii')), hashlib.sha256).hexdigest()
        headers = {
            'API-KEY': self.api_key,
            'API-TIMESTAMP': timestamp,
            'API-SIGN': sign,
        }
        try:
            resp = requests.get(url, headers=headers, params=params)
            return eval(json.dumps(resp.json()))
        except RequestException as e:
            logger.error(f'action=call_private_get_api params={params} error={e}')
            raise

    def get_available_amount(self):
        path = '/v1/account/margin'
        return self.call_private_get_api(path)

    def get_contract_last_day(self):
        path = '/v1/latestExecutions'
        params = {
            'symbol': settings.symbol,
            'page': 1,
            'count': 100,
        }
        return self.call_private_get_api(path, params)

    def get_open_interest(self):
        path = '/v1/positionSummary'
        params = {'symbol': settings.symbol}
        return self.call_private_get_api(path, params)

    def call_private_post_api(self, path, data):
        timestamp = '{0}000'.format(
            int(time.mktime(datetime.now().timetuple()))
        )
        method = 'POST'
        url = private_end_point + path
        text = timestamp + method + path + json.dumps(data)
        sign = hmac.new(bytes(self.secret_key.encode('ascii')),
                        bytes(text.encode('ascii')), hashlib.sha256).hexdigest()
        headers = {
            'API-KEY': self.api_key,
            'API-TIMESTAMP': timestamp,
            'API-SIGN': sign,
        }
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(data))
            return eval(json.dumps(resp.json()))
        except RequestException as e:
            logger.error(f'action=call_private_post_api data={data} error={e}')
            raise
        except JSONDecodeError:
            soup = BeautifulSoup(resp.text, 'html.parser')
            error = soup.text.replace('\n', '')
            logger.error(f'action=call_private_post_api error={error}')
            raise

    def order(self, side):
        path = '/v1/order'
        data = {
            'symbol': settings.symbol,
            'side': side,
            'executionType': settings.execution_type,
            # 'timeInForce': 'FAK',
            # 'price': settings.price,
            # 'losscutPrice': settings.loss_cut_price,
            'size': settings.size,
        }
        resp = self.call_private_post_api(path, data)
        logger.info(f'action=order side={side} resp={resp}')

    def pay_all_order(self, side):
        path = '/v1/closeBulkOrder'
        data = {
            'symbol': settings.symbol,
            'side': side,
            'executionType': settings.execution_type,
            'timeInForce': 'FAK',
            # 'price': settings.price,
            'size': settings.size,
        }
        resp = self.call_private_post_api(path, data)
        logger.info(f'action=pay_all_order resp={resp}')
