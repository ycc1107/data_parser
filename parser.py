import attr
import datetime
import logging
from collections import defaultdict
import pandas as pd
import csv
from sqlalchemy.sql import case

from dbobj import PriceData, rw_session

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

class PaserError(Exception):
    pass

attr.s
class ParserBase:
    file_name = attr.ib(validator=attr.validators.instance_of(str))
    is_load_all = attr.ib()
    date_format = attr.ib(default="%Y%m%d")
    data_holder = attr.ib()
    threshold = attr.ib(default=10000)
    curr_date = attr.ib()
    curr_records = attr.ib(default=defaultdict(list))
    
    @staticmethod
    def value_validation(values):
        if any(True for value in values if value <= 0):
            raise PaserError("Value error: contain value less than 0 [%r]", values)

    @staticmethod
    def date_validation(date, data_date):
        if data_date > date:
            raise PaserError("Date error: [%s] should less or equal to [%s] ", data_date, date)

    @staticmethod
    def ticket_validation(ticket):
        if not ticket:
            raise PaserError("Ticket error: ticket can't be empty")

    def delete_curr_data(self):
        with rw_session() as session:
            status = session.query(PriceData).filter(
                PriceData.date==self.curr_date
            ).delete()
            logger.info("Delete status [%s]", status)

    def update_curr_record(self):
        self.curr_records = defaultdict(list)

    def process_line_data(self, line):
        price = float(line["Price"])
        book_value = float(line["BookValue"])
        dividends = float(line["Dividends"])
        self.value_validation([price, book_value, dividends])

        earnings = float(line["Earnings"])

        date = datetime.datetime.strptime(line["Date"], self.date_format)
        data_date = datetime.datetime.strptime(line["DataDate"], self.date_format)
        self.date_validation(date, data_date)
        if self.curr_date != date:
            logger.warming("Date mismatch curr: [%s] on file: [%s]", self.curr_date, date)
            return 

        ticker = line["Ticker"].strip()
        self.ticket_validation(ticker)

        self.data_holder.append(
            PriceData(
                date=date,
                ticker=ticker,
                data_date=data_date,
                price=price,
                book_value=book_value,
                dividends=dividends,
                earnings=earnings,
                ep=earnings/price,
                bvpp=book_value/price,
                dpp=dividends/price,
                best=True
            )
        )
        self.curr_records[date].append(ticker)

        if len(self.data_holder) > self.threshold:
            self.update_curr_record()
            self.wirte_line_data()

    def process_whole_data(self, dataframe):
        err_df = dataframe[(dataframe.Price <= 0) | (dataframe.BookValue <= 0 ) | (dataframe.Dividends <= 0) | (dataframe.Ticker.isnull())]
        if not err_df.empty:
            logger.error("Wrong data %r", err_df)
            dataframe = dataframe[(dataframe.Price > 0) & (dataframe.BookValue > 0 ) & (dataframe.Dividends > 0) & (~dataframe.Ticker.isnull())]

        dataframe["ep"] = dataframe["Earnings"] / dataframe["Price"]
        dataframe["bvpp"] = dataframe["BookValue"] / dataframe["Price"]
        dataframe["dpp"] = dataframe["Dividends"] / dataframe["Price"]

        self.curr_records = dataframe.groupby("date")["ticker"].apply(list).to_dict()
        self.update_curr_record()

        logger.info("Insert %s records", dataframe.shape[0])
        with rw_session() as session:
            session.bulk_insert_mappings(PriceData, dataframe.to_dict("records"))


    def wirte_line_data(self):
        logger.info("Insert %s records", len(self.data_holder))
        with rw_session() as session:
            session.bulk_save_objects(self.data_holder)

        self.data_holder = []

    def run(self):
        if self.is_load_all:
            data_df = pd.read_csv(self.file_name)
            self.process_data(data_df)
        else:
            try:
                fin = open(self.file_name)
                file_obj = csv.DictReader(fin)
                try:
                    self.process_line_data(file_obj.next())
                except PaserError as e:
                    logger.error(e)
            except StopIteration:
                self.wirte_line_data()
            finally:
                fin.close()
            

@attr.s
class ReinstatParser(ParserBase):
    def update_curr_record(self):
        with rw_session() as session:
            for date, tickers in self.curr_records.items():
                status = session.query(PriceData).filter(
                    PriceData.date.in_(date)
                ).update(
                    {
                        PriceData.ticker: case(
                            {t: False for t in tickers},
                            value=PriceData.best
                        )
                    },
                    synchronize_session=False
                )
                logger.info("Update %s %r", date, tickers)
