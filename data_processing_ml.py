from quandl_data import short_ticker
import pandas as pd
import os

pd.set_option('display.max_columns', 10)


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
            files = os.listdir('raw_data')
            files_filtered = [filename for filename in files if filename.startswith(self._short_ticker)]

            for file in files_filtered:
                parquet_data = pd.read_parquet(f'raw_data/{file}', engine='fastparquet')
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


class MachineLearning(PreProcessedData):
    def create_samples(self):
        pass


class ValueAtRisk(PreProcessedData):
    pass

# For debugging purposes
# my_data_called = PreProcessedData(ticker="WSE/CDPROJEKT", start_dt='2019-01-01', end_dt='2019-08-01')
# my_df = my_data_called.raw_data.drop("%Change", axis=1)
