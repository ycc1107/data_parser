import csv
import os
import psutil
import logging 
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

class ProcessData:
    def __init__(self, parser, file_name, overwrite) -> None:
        self.parser = parser
        self.file_name = file_name

        self.parser.file_name = file_name
        self.parser.overwrite = overwrite

    def pre_check_file(self):
        mem_size = psutil.virtual_memory().available / 1024 / 1024
        file_size =  os.stat(self.file_name).st_size
        if file_size > mem_size:
            logger.info("Could not load whole file in memory. Will load line by line")
            self.parser.reader = csv.DictReader
        else:
            self.parser.reader = pd.read_csv

    def process_data(self) -> None:
        self.parser.run()



    


