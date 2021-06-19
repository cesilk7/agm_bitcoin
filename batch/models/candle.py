import logging

from sqlalchemy import Column
from sqlalchemy import desc
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy.exc import IntegrityError

from models.base import Base
from models.base import session_scope

from config import constants

logger = logging.getLogger(__name__)


class BaseCandleMixin(object):
    time = Column(DateTime, primary_key=True, nullable=False)
    open = Column(Float)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Float)

    @classmethod
    def create(cls, time, open, close, high, low, volume):
        candle = cls(time=time,
                     open=open,
                     close=close,
                     high=high,
                     low=low,
                     volume=volume)
        try:
            with session_scope() as session:
                session.add(candle)
            return candle
        except IntegrityError:
            return False

    @classmethod
    def get(cls, time):
        with session_scope() as session:
            candle = session.query(cls).filter(
                cls.time == time).first()
        if candle is None:
            return None
        return candle

    def save(self):
        with session_scope() as session:
            session.add(self)

    @classmethod
    def get_all_candles(cls, limit=100):
        with session_scope() as session:
            candles = session.query(cls).order_by(
                desc(cls.time)).limit(limit).all()

        if candles is None:
            return None

        candles.reverse()
        return candles

    @property
    def value(self):
        return {
            'time': self.time,
            'open': self.open,
            'close': self.close,
            'high': self.close,
            'low': self.low,
            'volume': self.volume,
        }


class BtcBaseCandle1M(BaseCandleMixin, Base):
    __tablename__ = 'BTC_1M'


class BtcBaseCandle5M(BaseCandleMixin, Base):
    __tablename__ = 'BTC_5M'


class BtcBaseCandle15M(BaseCandleMixin, Base):
    __tablename__ = 'BTC_15M'


class BtcBaseCandle30M(BaseCandleMixin, Base):
    __tablename__ = 'BTC_30M'


class BtcBaseCandle1H(BaseCandleMixin, Base):
    __tablename__ = 'BTC_1H'


def factory_candle_class(symbol, duration):
    if symbol == constants.SYMBOL_BTC:
        if duration == constants.DURATION_1M:
            return BtcBaseCandle1M
        if duration == constants.DURATION_5M:
            return BtcBaseCandle5M
        if duration == constants.DURATION_15M:
            return BtcBaseCandle15M
        if duration == constants.DURATION_30M:
            return BtcBaseCandle30M
        if duration == constants.DURATION_1H:
            return BtcBaseCandle1H


def create_candle_with_duration(symbol, duration, ticker):
    cls = factory_candle_class(symbol, duration)
    ticker_time = ticker.truncate_date_time(duration)
    current_candle = cls.get(ticker_time)
    price = ticker.last
    # price = ticker.mid_price

    if current_candle is None:
        cls.create(ticker_time, price, price, price, price, ticker.volume)
        return True

    if current_candle.high <= price:
        current_candle.high = price
    elif current_candle.low >= price:
        current_candle.low = price
    current_candle.volume = ticker.volume
    current_candle.close = price
    current_candle.save()
    return False
