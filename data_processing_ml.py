from quandl_data import short_ticker
import pandas as pd
import os
from config import data_folder
from datetime import timedelta, date
import numpy as np
from random import choice


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
                parquet_data = pd.read_parquet(
                    f'{os.path.dirname(os.path.abspath(__file__))}/{data_folder}/{file}',
                    engine='fastparquet')
                my_data.append(parquet_data)

            if len(my_data) > 1:
                # Reset index after dropping duplicates
                output = pd.concat(my_data).sort_index() \
                    .reset_index() \
                    .drop_duplicates(subset='Date', keep='first') \
                    .reset_index()

                output['avg_price'] = (output['Open'] + output['Close']) / 2

            else:
                output = pd.DataFrame()

            self._data = output
        return self._data

    def next_batch(self, batch_size, days):
        # If the rand id is id = 1 there will be no id to -1 in batch_X and it will fail
        dates = self.raw_data.iloc[1:-days, :] \
            .index.tolist()

        X_batch_list = []
        y_batch_list = []

        for num in range(batch_size):
            rand_id = choice(dates)
            batch_X = self.raw_data['avg_price'].iloc[rand_id - 1:rand_id + days - 1]
            X_batch_list.append(batch_X.tolist())

            batch_y = self.raw_data['avg_price'].iloc[rand_id:rand_id + days]
            y_batch_list.append(batch_y.tolist())

        X_batch_array = np.array(X_batch_list).reshape(-1, days, 1)
        y_batch_array = np.array(y_batch_list).reshape(-1, days, 1)

        return X_batch_array, y_batch_array

    def t_instance(self, rand_id=78, days=20):
        test_batch = self.raw_data['avg_price'].iloc[rand_id:rand_id + days].tolist()
        return np.array(test_batch).reshape(-1, days, 1)

    def t_instance_dates(self, rand_id=78, days=20):
        dates = self.raw_data['Date'].iloc[rand_id:rand_id + days].tolist()
        return np.array(dates).reshape(-1, days, 1)

# For debugging purposes
# my_data = PreProcessedData(ticker="WSE/CDPROJEKT", start_dt='2019-01-01', end_dt='2019-08-01')
# raw_data = my_data.raw_data
