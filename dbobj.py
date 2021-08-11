from sqlalchemy import (
    Column,
    String,
    DateTime,
    Float,
    Boolean
)
from urllib.parse import quote_plus as urlquote
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy.ext.declarative import declarative_base

TableBase = declarative_base()

class PriceData(TableBase):
    """Port for task system"""

    __tablename__ = "price_data"

    date = Column(DateTime, primary_key=True)
    ticker = Column(String, primary_key=True)
    data_date = Column(DateTime, primary_key=True)
    price = Column(Float)
    book_value = Column(Float)
    dividends = Column(Float)
    earnings = Column(Float)
    ep = Column(Float)
    bvpp = Column(Float)
    dpp = Column(Float)
    best = Column(Boolean)



DB_CONNTION = {}

def gen_engine(
    database=None,
    connection_name=None,
    db_password=None,
    db_user=None,
    is_new_db=False,
):
    """create db engine to connect db"""
    dialect, driver, host, port, db_name, password, user = DB_CONNTION[connection_name]
    if db_password:
        password = db_password
    if db_user:
        user = db_user

    db_string = f"{dialect}+{driver}://{user}:{urlquote(str(password))}@{host}:{port}"
    if not is_new_db:
        db_name = database if database else db_name
        db_string = f"{db_string}/{db_name}"

    engine = create_engine(db_string, echo=False)

    return engine


@contextmanager
def rw_session(database="test", connection_name="mysql", **kwargs):
    """read and write session"""
    engine = gen_engine(database, connection_name, **kwargs)
    session_obj = sessionmaker(bind=engine)
    session = session_obj()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()