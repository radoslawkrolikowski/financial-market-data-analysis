import datetime
import time
import json
import pytz
import logging
from kafka import KafkaProducer
from config import tokens, time_zone, topic
from getMarketData import GetData, get_market_calendar
from economic_indicators_spider import ItemsCrawler




def get_data_point(source, tokens, timestamp, request=None, function=None, symbol=None, interval=None,\
                   output_format='json'):
    """Performs the single API call.

    """
    get = GetData(tokens, output_format)
    if source == 'IEX':
        raw_data = get.get_iex_data(request, timestamp)
    elif source == 'AV':
         raw_data = get.get_av_data(timestamp, function, symbol, interval, request)
    else:
        logging.warning('Source doesn\'t recognized')
    return raw_data


def last_day_of_month(date):
    """Returns the last day of the month of passed datetime object.

    """
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)


def market_hour_to_dt(current_datetime, hour_str):
    """Converts time string to localize datetime object.

    """
    dt = datetime.datetime.strptime(hour_str, '%H:%M')
    mh = dt.hour
    mm = dt.minute
    dt = current_datetime.replace(hour=mh, minute=mm, second=0, microsecond=0)
    return dt



def intraday_data(producer, freq, market_hours, current_datetime, source, tokens, economic_data, request=None,\
                  function=None, symbol=None, interval=None, output_format='json'):
    """Get the intraday data from Alpha Vantage or IEX APIs. Function will call the source API with
    the frequency of 'freq' until market is closed.

    Parameters
    ----------
    producer: KafkaProducer
        KafkaProducer object that will be used to send the data.
    freq: int
        Frequency of data transmissions in seconds.
    market_hours: dict
        Dictionary of today's market start and end times.
    current_datetime: datetime.datetime()
        Initial current time (start of the session time).
    source: str
        Specify the source of data. Available values: 'AV' or 'IEX'.
    token: dict
        Dictionary of API (keys) tokens. Dictionary keys: 'av_token' and 'iex_token'.
    economic_data: dict
        Dict of economic input data required to parse indicators, such as datetime objects that represents economic
        data release schedule ['schedule'], countries of interest ['counties'] or importance of data ['importance'].
    request: str, optional (default=None)
        If source == 'AV':
            Specify request to call API with more complex queries, instead of using: function, symbol, interval.
            For example, call TECHNICAL INDICATORS API:
            'function=RSI&symbol=MSFT&interval=weekly&time_period=10&series_type=open'
        if source == 'IEX':
            Specify the API HTTP request, for instance '/deep/book?symbols={symbol}{?or&}',
            '/data-points/market/{symbol}{?or&}'. Check in documentation whether token should be added using & or ?.
            Available HTTP requests: https://iexcloud.io/docs/api/
            The full list of symbols can be found here: https://iextrading.com/trading/eligible-symbols/
    function: str, optional (default=None)
        Use only with source == 'AV'. Specify the API function to use, for instance: TIME_SERIES_INTRADAY,
        FX_INTRADAY, STOCH.
    symbol: str, optional (default=None)
        Use only with source == 'AV'. The name of the equity. For example: MSFT, SPY. In case of using FOREX API
        pass a string that specifies pair of currencies, for example: EURUSD (length of 6 chars).
    interval: str, optional (default=None)
        Use only with source == 'AV'. Time interval between two consecutive data points in the time series.
        Specify only for INTRADAY data and TECHNICAL INDICATORS: 1min, 5min, 15min, 30min, 60min.
    output_format: str, optional (default='json')
        Specify the output format. Available formats: 'json' or 'csv'.

    """

    start_crawler = True
    previous_datetime = current_datetime
    counter = 0

    crawler = ItemsCrawler()

    while (current_datetime >= market_hours['market_start']) and (current_datetime <= market_hours['market_end']):

        # Get market data
        raw_data = get_data_point(source, tokens, current_datetime, request=request, function=function, symbol=symbol,\
            interval=interval, output_format=output_format)

        # Send data through kafka producer
        producer.send(topic=topic[0], value=raw_data)

        # First call returns empty dictionary
        if counter <= 1:
            economic_data['schedule'] = crawler.get_schedule()
            print('economic schedule: ', economic_data['schedule'])

        # Get economic indicators
        if not economic_data['schedule']:
            indicators = crawler.get_indicators(economic_data['countries'], economic_data['importance'],\
                        current_datetime, freq, start_crawler)
        else:
            for dt in economic_data['schedule']:
                if current_datetime >= dt and current_datetime <= dt + datetime.timedelta(seconds=freq):
                    indicators = crawler.get_indicators(economic_data['countries'], economic_data['importance'],\
                        current_datetime, freq, start_crawler)

        start_crawler = False

        print('INDICATORS: ', indicators)

        # Send economic data through kafka producer
        producer.send(topic=topic[1], value=indicators)

        previous_datetime = current_datetime

        # Update economic data release schedule every hour
        if current_datetime.hour > previous_datetime.hour:
            economic_data['schedule'] = crawler.get_schedule()

        time.sleep(freq)

        # Update current time
        current_datetime = pytz.utc.localize(datetime.datetime.now()).astimezone(time_zone['EST'])

    else:
        logging.warning('Market is closed.')
        logging.warning('Current time: {} {}'.format(datetime.datetime.strftime(current_datetime, "%Y-%m-%d %I:%M %p"),\
            current_datetime.tzname()))
        logging.warning('Market trade hours: from {} to {} {}'.format(datetime.datetime.strftime(market_hours['market_start'],\
            "%Y-%m-%d %I:%M %p"), datetime.datetime.strftime(market_hours['market_end'], "%Y-%m-%d %I:%M %p"),\
            market_hours['market_end'].tzname()))


def start_day_session(producer, freq, source, tokens, economic_data, request=None, function=None, symbol=None,\
                      interval=None, output_format='json'):
    """Get the single day market session data from Alpha Vantage or IEX APIs.

    Parameters
    ----------
    producer: KafkaProducer
        KafkaProducer object that will be used to send the data.
    freq: int
        Frequency of data transmissions in seconds.
    source: str
        Specify the source of data. Available values: 'AV' or 'IEX'.
    token: dict
        Dictionary of API (keys) tokens. Dictionary keys: 'av_token' and 'iex_token'.
    economic_data: dict
        Dict of economic input data required to parse indicators, such as datetime objects that represents economic
        data release schedule ['schedule'], countries of interest ['counties'] or importance of data ['importance'].
    request: str, optional (default=None)
        If source == 'AV':
            Specify request to call API with more complex queries, instead of using: function, symbol, interval.
            For example, call TECHNICAL INDICATORS API:
            'function=RSI&symbol=MSFT&interval=weekly&time_period=10&series_type=open'
        if source == 'IEX':
            Specify the API HTTP request, for instance '/deep/book?symbols={symbol}{?or&}',
            '/data-points/market/{symbol}{?or&}'. Check in documentation whether token should be added using & or ?.
            Available HTTP requests: https://iexcloud.io/docs/api/
            The full list of symbols can be found here: https://iextrading.com/trading/eligible-symbols/
    function: str, optional (default=None)
        Use only with source == 'AV'. Specify the API function to use, for instance: TIME_SERIES_INTRADAY,
        FX_INTRADAY, STOCH.
    symbol: str, optional (default=None)
        Use only with source == 'AV'. The name of the equity. For example: MSFT, SPY. In case of using FOREX API
        pass a string that specifies pair of currencies, for example: EURUSD (length of 6 chars).
    interval: str, optional (default=None)
        Use only with source == 'AV'. Time interval between two consecutive data points in the time series.
        Specify only for INTRADAY data and TECHNICAL INDICATORS: 1min, 5min, 15min, 30min, 60min.
    output_format: str, optional (default='json')
        Specify the output format. Available formats: 'json' or 'csv'.

    """
    current_datetime = pytz.utc.localize(datetime.datetime.now()).astimezone(time_zone['EST'])
    current_date = current_datetime.date()

    market_calendar = get_market_calendar()

    # Check if market is open today.
    market_day = list(filter(lambda date_dict: date_dict.get('date') == current_date.strftime('%Y-%m-%d'),\
                        market_calendar))[0]
    is_open = market_day.get('status') == 'open'

    if is_open:

        if source == 'IEX':
            # Extract Stock market hours as strings
            premarket_start, premarket_end = market_day.get('premarket').values()
            market_start, market_end = market_day.get('open').values()
            postmarket_start, postmarket_end = market_day.get('postmarket').values()

            # Convert market hours to datetime objects (EST)
            hours = [('premarket_start', premarket_start), ('premarket_end', premarket_end), ('market_start', market_start),\
                     ('market_end', market_end), ('postmarket_start', postmarket_start), ('postmarket_end', postmarket_end)]
            market_hours = {key: market_hour_to_dt(current_datetime, value) for key, value in hours}

        else:
            market_hours = {}

            # FOREX market trade hours
            market_start = current_datetime.replace(hour=17, minute=0, second=0, microsecond=0, tzinfo=time_zone['EST'])
            market_hours['market_start'] = market_start - datetime.timedelta(days=(current_datetime.weekday() + 1))

            market_end = current_datetime.replace(hour=16, minute=0, second=0, microsecond=0, tzinfo=time_zone['EST'])
            market_hours['market_end'] = market_end + datetime.timedelta(days=-(current_datetime.weekday() - 4))


        # Call the function that is responsible for fetching the intraday data
        intraday_data(producer, freq, market_hours, current_datetime, source, tokens, economic_data, request=request,
                      function=function, symbol=symbol, interval=interval, output_format=output_format)

    else:
        logging.warning('Current time: {} {}'.format(datetime.datetime.strftime(current_datetime, "%Y-%m-%d %I:%M %p"),\
            current_datetime.tzname()))
        logging.warning('Today market is closed')


freq = 60

economic_data = {'countries': ['United States'], 'importance': ['2', '3']}

kafka_servers = ['localhost:9092']

producer = KafkaProducer(bootstrap_servers=kafka_servers,
                         value_serializer=lambda x:
                         json.dumps(x).encode('utf-8'))

try:
    start_day_session(producer, freq, 'IEX', tokens, economic_data, request='/stock/spy/news/last/1?')
finally:
    producer.close()
