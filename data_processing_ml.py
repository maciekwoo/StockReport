from quandl_data import short_ticker
import pandas as pd
import os

# where raw data is stored within the project folder structure
data_folder = 'raw_data'


class PreProcessedData:
    def __init__(self, start_dt, end_dt, ticker):
        self.start_date = start_dt
        self.end_date = end_dt
        self.ticker = ticker
        self._data = None
        self._short_ticker = short_ticker(self.ticker)

    @property
    def raw_data(self):
        if self._data is None:
            my_data = []
            files = os.listdir(f'{os.path.dirname(os.path.abspath(__file__))}/{data_folder}')
            # TODO: add reading only from start_dt to end_dt and not all data
            files_filtered = [filename for filename in files if filename.startswith(self._short_ticker)]

            for file in files_filtered:
                parquet_data = pd.read_parquet(f'{os.path.dirname(os.path.abspath(__file__))}/{data_folder}/{file}',
                                               engine='fastparquet')
                my_data.append(parquet_data)

            if len(my_data) > 1:
                output = pd.concat(my_data).sort_index() \
                    .reset_index() \
                    .drop_duplicates(subset='Date', keep='first') \
                    .set_index('Date')
            else:
                output = pd.DataFrame()

            self._data = output
        return self._data
