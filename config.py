"""Configuration file.

"""
import pytz

tokens = {'av_token': '6CK0W5ZFDAUWX0DQ',
          'iex_token': 'pk_e01ee438a3b545818531ea9d669ddee0'}


time_zone = {'EST': pytz.timezone('US/Eastern'),
    'UTC': pytz.timezone('UTC'),
    'iex_stock': pytz.timezone('US/Eastern'),
    'iex_forex': pytz.timezone('UTC')}

topic = ['main', 'economic', 'news']

user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:69.0) Gecko/20100101 Firefox/69.0'

event_list = ['Crude Oil Inventories']
