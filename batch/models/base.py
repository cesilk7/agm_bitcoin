from contextlib import contextmanager
import logging
import threading

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session

from config import settings

logger = logging.getLogger(__name__)
Base = declarative_base()
engine = create_engine(settings.database_url)
Session = scoped_session(sessionmaker(bind=engine))
lock = threading.Lock()


@contextmanager
def session_scope():
    session = Session()
    session.expire_on_commit = False
    try:
        lock.acquire()
        yield session
        session.commit()
    except Exception as e:
        logger.error(f'action=session_scope error={e}')
        session.rollback()
        raise
    finally:
        session.expire_on_commit = True
        session.close()
        lock.release()


def init_db():
    from models import candle
    from models import events
    Base.metadata.create_all(bind=engine)
