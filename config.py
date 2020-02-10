"""Configuration file.

"""
import pytz

tokens = {'av_token': '6CK0W5ZFDAUWX0DQ',
    'iex_token': 'pk_e01ee438a3b545818531ea9d669ddee0'}

time_zone = {'EST': pytz.timezone('US/Eastern'),
    'UTC': pytz.timezone('UTC'),
    'iex_stock': pytz.timezone('US/Eastern'),
    'iex_forex': pytz.timezone('UTC')}

kafka_config = {'servers':['localhost:9092'], 'topics': ['vix', 'volume', 'cot', 'ind', 'deep']}

user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:69.0) Gecko/20100101 Firefox/69.0'

mysql_user = 'admin'
mysql_password = 'admin'

mysql_hostname = 'localhost'
mysql_port = '3306'

mysql_database_name = 'stock_data'
mysql_table_name = 'stock_data_joined'

get_cot = True
get_vix = True
get_stock_volume = 'SPY'

event_list = ['Crude Oil Inventories', 'ISM Non-Manufacturing PMI', 'ISM Non-Manufacturing Employment',
    'Services PMI', 'ADP Nonfarm Employment Change', 'Core CPI', 'Fed Interest Rate Decision', 'Building Permits',
    'Core Retail Sales', 'Retail Sales', 'JOLTs Job Openings', 'Nonfarm Payrolls', 'Unemployment Rate']

temp_event_list = [event_name.replace(" ", "_") for event_name in event_list]
event_values = ["Actual", "Prev_actual_diff", "Forc_actual_diff"]
empty_ind_dict = {"Timestamp": None}

for event in temp_event_list:
    empty_ind_dict.setdefault(event, {})
    for value in event_values:
        empty_ind_dict[event].setdefault(value, None)

