import matplotlib.pyplot as plt
from data_processing_ml import PreProcessedData
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

pd.set_option('display.max_columns', 10)

my_data = PreProcessedData(ticker="WSE/CDPROJEKT", start_dt='2019-01-01', end_dt='2019-08-01')
my_df = my_data.raw_data.drop("%Change", axis=1)

my_df = my_df.reset_index()
my_df['mean_price'] = (my_df['Open'] + my_df['Close']) / 2

# matplotlib example
plt.style.use('Solarize_Light2')
my_df.plot.line(x='Date', y='mean_price', figsize=(12, 3), lw=1)
plt.show()

# # Using plotly
# fig = go.Figure()
#
# fig = go.Figure(data=[go.Candlestick(x=my_df['Date'],
#                                      open=my_df['Open'],
#                                      high=my_df['High'],
#                                      low=my_df['Low'],
#                                      close=my_df['Close'])])
#
# fig.show()
