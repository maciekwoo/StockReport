from resources import _strip_ticker, _get_quandl_data
import pandas as pd
import os

pd.set_option('display.max_columns', 10)


class PreProcessedData:
    def __init__(self, start_dt, end_dt, ticker):
        self.start_date = start_dt
        self.end_date = end_dt
        self.ticker = ticker

    @property
    def raw_data(self):
        short_ticker = _strip_ticker(self.ticker)
        _get_quandl_data(self.ticker, self.start_date, self.end_date)
        my_data = []
        for filename in os.listdir('raw_data'):
            if filename.startswith(short_ticker):
                parquet_data = pd.read_parquet(f'raw_data/{filename}', engine='fastparquet',
                                               columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', '# of Trades',
                                                        'Turnover (1000)'])
                my_data.append(parquet_data)
            else:
                continue
        return pd.concat(my_data).sort_index() if len(my_data) > 1 else pd.DataFrame()


class MachineLearning(PreProcessedData):
    pass


class ValueAtRisk(PreProcessedData):
    pass

# # For debugging purposes
# my_data_called = PreProcessedData(ticker='WSE/CDPROJEKT', start_dt='2019-01-01', end_dt='2019-08-01')
# my_df = my_data_called.raw_data
