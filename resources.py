import quandl
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import os
import re
from quandl.errors.quandl_error import NotFoundError
import time

quandl.ApiConfig.api_key = os.environ['API_KEY']


def _get_all_months(start_dt, end_dt):
    """
    Create collection of dates to iterate through while getting quandl data
    :param start_dt: fetch data from date
    :param end_dt: fetch data to date, can be > today, will fetch all available data
    :return:
    """
    start = datetime.strptime(start_dt, '%Y-%m-%d')
    end = datetime.strptime(end_dt, '%Y-%m-%d')
    start_plus_one = start + relativedelta(months=1)
    api_dates = []
    if start > end:
        print(f'{start} is greater than {end}, aborting operation')
    else:
        api_dates.append((date.isoformat(start), date.isoformat(start_plus_one)))
        # TODO if today is May 15th, June 01 will be written and an incomplete file will persist
        while start < end:
            start += relativedelta(months=1)
            start_plus_one += relativedelta(months=1)
            api_dates.append((date.isoformat(start), date.isoformat(start_plus_one)))
    return api_dates


def _strip_ticker(key):
    return re.sub(r'\w*/', '', key)


def _write_quandl_parquet(ticker, start_dt, end_dt):
    short_ticker = _strip_ticker(ticker)
    if os.path.isfile(f'raw_data/{short_ticker}_{start_dt}_{end_dt}.parquet'):
        print(f'File raw_data/{short_ticker}_{start_dt}_{end_dt}.parquet already exists, skipping write')
    else:
        try:
            data = quandl.get(ticker, start_date=start_dt, end_date=end_dt)
            data.to_parquet(f'raw_data/{short_ticker}_{start_dt}_{end_dt}.parquet', engine='fastparquet',
                            compression=None)
            print(f'File raw_data/{short_ticker}_{start_dt}_{end_dt}.parquet downloaded and created')
            time.sleep(0.5)
        except NotFoundError:
            print(f'Ticker not found, write to parquet aborted for {ticker} from {start_dt} to {end_dt}')


def _get_quandl_data(ticker, start_dt, end_dt):
    """
    Get quandl data for given ticker
    :param ticker: Ticker
    :param start_dt: fetch data from date
    :param end_dt: can be > today, will fetch all available
    :return:
    """
    months = _get_all_months(start_dt, end_dt)
    for start, end in months:
        _write_quandl_parquet(ticker, start, end)

# For debugging purposes
# get_quandl_data("WSE/CDPROJEKT", '2018-01-01', '2019-08-01')
