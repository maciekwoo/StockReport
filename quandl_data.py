import quandl
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import os
import re
from quandl.errors.quandl_error import NotFoundError
import time

quandl.ApiConfig.api_key = os.environ['API_KEY']


def short_ticker(ticker):
    return re.sub(r'\w*/', '', ticker)


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


def _download_quandl_parquet(ticker, start_dt, end_dt):
    try:
        data = quandl.get(ticker, start_date=start_dt, end_date=end_dt)
        # Only write non-empty files
        if data.shape[0] != 0:
            data.to_parquet(f'raw_data/{short_ticker(ticker)}_{start_dt}_{end_dt}.parquet', engine='fastparquet',
                            compression=None)
            print(f'File raw_data/{short_ticker(ticker)}_{start_dt}_{end_dt}.parquet downloaded and created')
            time.sleep(0.5)
    except NotFoundError:
        print(f'Ticker not found, write to parquet aborted for {ticker} from {start_dt} to {end_dt}')


def write_new_quandl_data(ticker, start_dt, end_dt):
    months = _get_all_months(start_dt, end_dt)
    months_write = [month for month in months if
                    not os.path.isfile(f'raw_data/{short_ticker(ticker)}_{month[0]}_{month[1]}.parquet')]
    months_skipped = [month for month in months if month not in months_write]
    for month in months_write:
        _download_quandl_parquet(ticker, month[0], month[1])
    for month in months_skipped:
        print(f'File {short_ticker(ticker)}_{month[0]}_{month[1]} skipped, already exists')

# For debugging purposes
# write_new_quandl_data("WSE/CDPROJEKT", '2018-01-01', '2019-10-01')
