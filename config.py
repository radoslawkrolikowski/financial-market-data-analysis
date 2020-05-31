"""Configuration file.

"""
import pytz

tokens = {'av_token': '<Your-token>',
    'iex_token': '<Your-token>'}

time_zone = {'EST': pytz.timezone('US/Eastern'),
    'UTC': pytz.timezone('UTC'),
    'iex_stock': pytz.timezone('US/Eastern'),
    'iex_forex': pytz.timezone('UTC')}

# Specify Kafka brokers addresses and topics
kafka_config = {'servers':['localhost:9092'], 'topics': ['vix', 'volume', 'cot', 'ind', 'deep', 'predict_timestamp', 'prediction']}

# Scrapy user agent
user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:69.0) Gecko/20100101 Firefox/69.0'

# Database (MySQL/MariaDB) properties
mysql_user = 'admin'
mysql_password = 'admin'

mysql_hostname = 'localhost'
mysql_port = '3306'

mysql_database_name = 'stock_data'
mysql_table_name = 'stock_data_joined'

# Whether to fetch COT, VIX or volume data
get_cot = True
get_vix = True
get_stock_volume = 'SPY' # Otherwise False

# Number of order book price levels to include
bid_levels = 7
ask_levels = 7

# Specify Moving Averages periods (False if not used)
volume_MA_periods = [6, 20]
price_MA_periods = [20]
delta_MA_periods = [12]

# Specify period and number of standard deviations for Bollinger Bands
bollinger_bands_period = 20 # False if not used
bollinger_bands_std = 2

# Whether to add Stochastic Oscillator
stochastic_oscillator = True

# List of economic indicators to use
event_list = ['Crude Oil Inventories', 'ISM Non-Manufacturing PMI', 'ISM Non-Manufacturing Employment',
    'Services PMI', 'ADP Nonfarm Employment Change', 'Core CPI', 'Fed Interest Rate Decision', 'Building Permits',
    'Core Retail Sales', 'Retail Sales', 'JOLTs Job Openings', 'Nonfarm Payrolls', 'Unemployment Rate']

# Create economic indicators message template. New scraped values of indicators will replace corresponding
# 0 values in this template.
event_list_repl = [event_name.replace(" ", "_").replace("-", "_") for event_name in event_list]
event_values = ["Actual", "Prev_actual_diff", "Forc_actual_diff"]
empty_ind_dict = {"Timestamp": 0}

for event in event_list_repl:
    empty_ind_dict.setdefault(event, {})
    for value in event_values:
        empty_ind_dict[event].setdefault(value, 0)

