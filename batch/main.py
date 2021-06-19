from datetime import datetime, timedelta
import logging
import sys
from threading import Thread

from config import settings, constants
from models.candle import create_candle_with_duration
from models.dfcandle import DataFrameCandle
from models.events import SignalEvents, SignalEvent
from services.gmo_api import PublicWebSocketApi
from services.gmo_api import Ticker
from services.trade import AiTrade


log_format = '%(asctime)s %(name)-2s %(levelname)s：%(message)s'
logging.basicConfig(
    level=logging.INFO, filename=settings.log_path, format=log_format)


if __name__ == '__main__':

    args = '0' if len(sys.argv) == 1 else sys.argv[1]

    # production
    if args == '0':
        ai_trade = AiTrade()
        thread = Thread(target=ai_trade.trade_start)
        thread.start()
        thread.join()

    # Sample
    if args == '1':
        # from models.ai import AI
        # ai = AI(
        #     symbol=settings.symbol,
        #     use_percent=settings.use_percent,
        #     duration=settings.trade_duration,
        #     past_period=settings.past_period,
        #     stop_limit_percent=settings.stop_limit_percent,
        #     back_test=False,
        # )
        # ai.start_trade -= timedelta(days=10)
        #
        # df = DataFrameCandle()
        # df.set_all_candles(limit=1000)
        # candle = df.candles[-1]
        # ai.sell(candle)
        print('### test ###')
    