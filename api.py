import datetime
import pandas as pd
from functools import lru_cache
from dbobj import PriceData, rw_session

def process_params(tickers, end_date, start_date):
    if isinstance(tickers, str):
        tickers = [tickers]

    if not end_date:
        end_date = datetime.date.today()

    if not start_date and not end_date:
        start_date = end_date = datetime.date.today()
    
    return tickers, end_date, start_date


@lru_cache
def get_data(tickers, start_date, end_date, is_all_data):
    tickers, end_date, start_date = process_params(tickers, end_date, start_date)
    with rw_session() as session:
        query = session.query(PriceData).filter(
            PriceData.ticker.in_(tickers),
            PriceData.date.between(start_date, end_date)
        )
        if not is_all_data:
            query = query.filter(PriceData.best==True)

        return pd.read_sql(query.statement, query.session.bind)

@lru_cache
def get_history_data_change(tickers, start_date, end_date=None):
    tickers, end_date, start_date = process_params(tickers, end_date, start_date)
    with rw_session() as session:
        query = session.query(PriceData).filter(
            PriceData.ticker.in_(tickers),
            PriceData.date_date.between(start_date, end_date)
        )

        return pd.read_sql(query.statement, query.session.bind)


def get_best_data(tickers, start_date, end_date=None):
    return get_data(tickers, start_date, end_date, is_all_data=False)

def get_all_data(tickers, start_date, end_date=None):
    return get_data(tickers, start_date, end_date, is_all_data=True)


