import matplotlib.pyplot as plt
from data_processing_ml import PreProcessedData
import pandas as pd

pd.set_option('display.max_columns', 10)

my_data = PreProcessedData(ticker="WSE/CDPROJEKT", start_dt='2019-01-01', end_dt='2019-08-01')
my_df = my_data.raw_data.drop("%Change", axis=1)

open_data = my_df.reset_index()
# plot
plt.style.use('Solarize_Light2')
open_data.plot.line(x='Date', y='Open', figsize=(12, 3), lw=1)
plt.show()
