import datetime

import omitempty
from sqlalchemy import Column
from sqlalchemy import desc
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import String

from models.base import Base
from models.base import session_scope

from config import settings, constants


class SignalEvent(Base):
    __tablename__ = 'SIGNAL_EVENT'

    time = Column(DateTime, primary_key=True, nullable=False)
    symbol = Column(String, nullable=False)
    side = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    size = Column(Float, nullable=False)

    def save(self):
        with session_scope() as session:
            session.add(self)

    @property
    def value(self):
        dict_values = omitempty({
            'time': self.time,
            'symbol': self.symbol,
            'side': self.side,
            'price': self.price,
            'size': self.size,
        })
        if not dict_values:
            return None
        return dict_values

    @classmethod
    def get_signal_events_by_count(cls, count, symbol=settings.symbol):
        with session_scope() as session:
            rows = session.query(cls).filter(cls.symbol == symbol)\
                .order_by(desc(cls.time)).limit(count).all()
            if rows is None:
                return []
            rows.reverse()
            return rows

    @classmethod
    def get_signal_events_after_time(cls, time):
        with session_scope() as session:
            rows = session.query(cls).filter(cls.time >= time).all()
            if rows is None:
                return []
            return rows


class SignalEvents(object):
    def __init__(self, signals=None):
        if signals is None:
            self.signals = []
        else:
            self.signals = signals

    def can_buy(self, time):
        if len(self.signals) == 0:
            return True
        last_signal = self.signals[-1]

        if last_signal.side == constants.SELL and last_signal.time < time:
            return True

        return False

    def can_sell(self, time):
        if len(self.signals) == 0:
            return False
        last_signal = self.signals[-1]

        if last_signal.side == constants.BUY and last_signal.time < time:
            return True
        return False

    def buy(self, time, symbol, price, size, save):
        if not self.can_buy(time):
            return False

        signal_event = SignalEvent(
            time=time, symbol=symbol, side=constants.BUY, price=price, size=size
        )
        if save:
            signal_event.save()

        self.signals.append(signal_event)
        return True

    def sell(self, time, symbol, price, size, save):
        if not self.can_sell(time):
            return False

        signal_event = SignalEvent(
            time=time, symbol=symbol, side=constants.SELL, price=price, size=size
        )
        if save:
            signal_event.save()

        self.signals.append(signal_event)
        return True

    @staticmethod
    def get_signal_events_by_count(count: int):
        signal_events = SignalEvent.get_signal_events_by_count(count)
        return SignalEvents(signal_events)

    @staticmethod
    def get_signal_events_after_time(time: datetime.datetime.time):
        signal_events = SignalEvent.get_signal_events_after_time(time)
        return SignalEvents(signal_events)

    @property
    def profit(self):
        total = 0.0
        before_sell = 0.0
        is_holding = False
        for i in range(len(self.signals)):
            signal_event = self.signals[i]
            if i == 0 and signal_event.side == constants.SELL:
                continue
            if signal_event.side == constants.BUY:
                total -= signal_event.price * signal_event.size
                is_holding = True
            if signal_event.side == constants.SELL:
                total += signal_event.price * signal_event.size
                is_holding = False
                before_sell = total
        if is_holding:
            return before_sell
        return total

    @property
    def value(self):
        signals = [s.value for s in self.signals]
        if not signals:
            signals = None
        profit = self.profit

        if not self.profit:
            profit = None

        return {
            'signals': signals,
            'profit': profit,
        }

