import logging
from threading import Lock, Thread

from services.gmo_api import PublicWebSocketApi
from services.gmo_api import Ticker
from models.candle import create_candle_with_duration
from models.ai import AI

from config import settings

logger = logging.getLogger(__name__)


class AiTrade(object):
    def __init__(self):
        self.ai = AI(
            symbol=settings.symbol,
            use_percent=settings.use_percent,
            duration=settings.trade_duration,
            past_period=settings.past_period,
            stop_limit_percent=settings.stop_limit_percent,
            back_test=settings.back_test)
        self.trade_lock = Lock()

    def trade_start(self):
        pwsa = PublicWebSocketApi()
        pwsa.get_real_time_ticker(self.write_ticker_info)

    def write_ticker_info(self, ws, message):
        dic = eval(message)
        ticker = Ticker(
            timestamp=dic['timestamp'],
            ask=dic['ask'],
            bid=dic['bid'],
            high=dic['high'],
            last=dic['last'],
            low=dic['low'],
            volume=dic['volume'])
        logger.info(f'action=write_ticker_info ticker={ticker.__dict__}')
        for duration in settings.durations:
            is_created = create_candle_with_duration(settings.symbol, duration, ticker)
            if is_created and duration == settings.trade_duration:
                thread = Thread(target=self._trade, args=(self.ai,))
                thread.start()

    def _trade(self, ai: AI):
        with self.trade_lock:
            ai.trade()
