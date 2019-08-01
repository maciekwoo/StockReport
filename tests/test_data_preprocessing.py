from quandl_data import short_ticker
from quandl_data import _get_all_months
from quandl_data import _download_quandl_parquet
from data_processing_ml import PreProcessedData
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
import data_processing_ml
import pytest
from quandl.errors.quandl_error import NotFoundError


def generate_my_dates():
    today = datetime.now().date()
    this_month_first = today.replace(day=1)
    plus_six_months = this_month_first + relativedelta(months=6)
    two_months_back = this_month_first + relativedelta(months=-2)
    one_month_back = this_month_first + relativedelta(months=-2)
    return two_months_back, one_month_back, plus_six_months


def test_short_ticker():
    fn = short_ticker('WSE/CDPROJEKT')
    assert fn == 'CDPROJEKT'


def test_get_all_months(capfd):
    fn = _get_all_months('2019-01-01', '2018-01-01')
    out, err = capfd.readouterr()
    assert out == 'End date:2018-01-01 00:00:00 has to be greater than Start date:2019-01-01 00:00:00, aborting operation\n'


def test_get_all_months_output_tuple():
    fn = _get_all_months('2019-01-01', '2019-10-01')
    for item in fn:
        assert type(item) == tuple


def test_get_all_months_output_string():
    fn = _get_all_months('2019-01-01', '2019-10-01')
    for item in fn:
        assert type(item[0]) == str
        assert type(item[1]) == str
        assert type(item[2]) == str


def test_get_months_output_part_full():
    two_months_back, _, plus_six_months = generate_my_dates()
    fn = _get_all_months(date.isoformat(two_months_back), date.isoformat(plus_six_months))
    assert fn[-1][2] == 'partial'
    assert fn[0][2] == 'full'


def test_dl_quandl_parquet_wrong_ticker(capfd):
    x = _download_quandl_parquet('DATABASE_GIBBERISH/TEST_GIBBERISH', '2019-01-01', '2019-02-01', 'full')
    out, err = capfd.readouterr()
    assert out == 'Ticker not found, write to parquet aborted for ' \
                  'DATABASE_GIBBERISH/TEST_GIBBERISH from 2019-01-01 to 2019-02-01\n'


def test_pre_processed_raw_is_df():
    two_months_back, one_month_back, _ = generate_my_dates()
    df = PreProcessedData(ticker='WSE/CDPROJEKT', start_dt=date.isoformat(two_months_back),
                          end_dt=date.isoformat(one_month_back))
    assert isinstance(df.raw_data, pd.DataFrame)
