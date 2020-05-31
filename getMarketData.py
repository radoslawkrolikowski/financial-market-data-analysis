import requests
import json
import pandas as pd
import io
import datetime
import logging
from config import time_zone

# Function responsible for replacing unwanted characters in dictionary keys
def change_keys(obj, old, new):
    """Recursively goes through the dictionary obj and changes keys by
    replacing old chars with new ones.

    """
    if isinstance(obj, dict):
        new_obj = obj.__class__()
        for k, v in obj.items():
            new_obj[k.replace(old, new)] = change_keys(v, old, new)
    elif isinstance(obj, (list, set, tuple)):
        new_obj = obj.__class__(change_keys(v, old, new) for v in obj)
    else:
        return obj

    return new_obj

def to_number(v):
    """Casts string to int or float

    """
    try:
        if v.isdigit():
            return int(v)
        else:
            return float(v)
    except ValueError:
            return v

def value_to_number(obj):
    """Recursively goes through the dictionary obj and converts strings
    to ints or floats if possible.

    """
    if isinstance(obj, dict):
        new_obj = obj.__class__()
        for k, v in obj.items():
            if isinstance(v, dict):
                new_obj[k] = value_to_number(v)
            elif isinstance(v, (list, set, tuple)):
                new_obj[k] = v.__class__(to_number(lv) for lv in v)
            else:
                new_obj[k] = to_number(v)

    elif isinstance(obj, (list, set, tuple)):
        new_obj = obj.__class__(value_to_number(lv) for lv in obj)
    else:
        return obj

    return new_obj


class GetData:
    """Get the data from Alpha Vantage or IEX APIs.

    Parameters
    ----------
    token: dict
        Dictionary of API (keys) tokens. Dictionary keys: 'av_token' and 'iex_token'.
    output_format: str, optional (default='json')
        Specify the output format. Available formats: 'json' or 'csv'.

    """

    def __init__(self, token, output_format='json'):

        self.__token = token
        self.output_format = output_format

        assert (output_format == 'json' or output_format == 'csv'), '{} format is not supported'\
            .format(output_format)


    def get_iex_data(self, request, timestamp):
        """Get the real-time data from the IEX API.

        Parameters
        ----------
        request: str
            Specify the API HTTP request, for instance '/deep/book?symbols={symbol}{?or&}',
            '/data-points/market/{symbol}{?or&}'. Check in documentation whether token should be added using & or ?.
            Available HTTP requests: https://iexcloud.io/docs/api/
            The full list of symbols can be found here: https://iextrading.com/trading/eligible-symbols/
        timestamp: datetime.datetime()
            Timestamp of real-time data (EST)

        Returns
        -------
        dict / pd.DataFrame
            Dictionary or pandas DataFrame depending on the output_format.

        """
        self.url = 'https://cloud.iexapis.com/v1{request}token={token}&format={output_format}'\
            .format(request=request, token=self.__token['iex_token'], output_format=self.output_format)

        try:
            req = requests.get(self.url)

            if self.output_format == 'json':
                raw_data = json.loads(req.content)
            else:
                raw_data = pd.read_csv(io.StringIO(req.content.decode('utf-8')))

            if isinstance(raw_data, dict):
                raw_data['Timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")

                # Change the structure of the message and make dictionary keys in DEEP book distinctive
                if '/deep/book' in request:
                    symbol = list(raw_data.keys())[0]

                    for i, level in enumerate(raw_data[symbol]['bids']):
                        raw_data['bids_{:d}'.format(i)] = {'bid_{:d}'.format(i): level['price'],
                                                           'bid_{:d}_size'.format(i): level['size']}

                    for i, level in enumerate(raw_data[symbol]['asks']):
                        raw_data['asks_{:d}'.format(i)] = {'ask_{:d}'.format(i): level['price'],
                                                           'ask_{:d}_size'.format(i): level['size']}

                    del raw_data[symbol]

            if isinstance(raw_data, list):
                for mssg in raw_data:
                    mssg['Timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")

        except requests.exceptions.ConnectionError as mssg:
            print(mssg)

        return raw_data


    def get_av_data(self, timestamp, function=None, symbol=None, interval=None, request=None):
        """Get the data from the Alpha Vantage API. With INTRADAY functions or FOREX(DAILY, WEEKLY, MONTHLY)
        it returns 100 latest data points, with FOREX(INTRADAY) function returns real-time data point, otherwise
        the time series covering 20+ years of historical data is returned.

        More information about Alpha Vantage API: https://www.alphavantage.co/documentation/

        Parameters
        ----------
        function: str, optional (default=None)
            Specify the API function to use, for instance: TIME_SERIES_INTRADAY, FX_INTRADAY, STOCH.
        symbol: str, optional (default=None)
            The name of the equity. For example: MSFT, SPY. In case of using FOREX API pass a string that
            specifies pair of currencies, for example: EURUSD (length of 6 chars).
        interval: str, optional (default=None)
            Time interval between two consecutive data points in the time series. Specify only for INTRADAY
            data and TECHNICAL INDICATORS: 1min, 5min, 15min, 30min, 60min.
        timestamp: datetime.datetime()
            Timestamp of real-time data (EST).
        request: str, optional (default=None)
            Specify request to call API with more complex queries, instead of using: function, symbol, interval.
            For example, call TECHNICAL INDICATORS API:
            'function=RSI&symbol=MSFT&interval=weekly&time_period=10&series_type=open'

        Returns
        -------
        dict / pd.DataFrame
            Dictionary or pandas DataFrame depending on the output_format.

        """
        if not request:
            # Define FOREX data request
            if function in ['FX_INTRADAY', 'FX_DAILY', 'FX_WEEKLY', 'FX_MONTHLY']:
                symbol1, symbol2 = symbol[:3], symbol[3:]
                self.url = 'https://www.alphavantage.co/query?function={function}&from_symbol={symbol1}'\
                    '&to_symbol={symbol2}&interval={interval}&apikey={token}&datatype={output_format}'\
                    .format(function=function, symbol1=symbol1, symbol2=symbol2, interval=interval, \
                            token=self.__token['av_token'], output_format=self.output_format)

            else:
                self.url = 'https://www.alphavantage.co/query?function={function}&symbol={symbol}'\
                    '&interval={interval}&apikey={token}&datatype={output_format}'\
                    .format(function=function, symbol=symbol, interval=interval, token=self.__token['av_token'],\
                            output_format=self.output_format)
        else:
            self.url = 'https://www.alphavantage.co/query?' + request + '&apikey={token}&datatype={output_format}'\
                .format(token=self.__token['av_token'], output_format=self.output_format)

        try:
            req = requests.get(self.url)
            if self.output_format == 'json':
                raw_data = json.loads(req.content)

                if not raw_data:
                    raise Exception("Alpha Vantage API currently not available!")

                if 'Error Message' in raw_data:
                    raise Exception(raw_data['Error Message'])

                keys_level_1 = list(raw_data.keys())
                last_dt_str = list(raw_data[keys_level_1[1]].keys())[0]

                # Extract datetime of latest data point (EST)
                last_dt = datetime.datetime.strptime(last_dt_str, "%Y-%m-%d %H:%M:%S")
                last_dt = time_zone['EST'].localize(last_dt)

                # Extract only the latest data point
                raw_data = raw_data[keys_level_1[1]][last_dt_str]

                if last_dt < timestamp - datetime.timedelta(minutes=4):
                    logging.warning('RETURNED DATA IS DELAYED!')
                    # Beacuse of the high API usage the latest returned data point can be delayed.
                    # In that case, returned data point usually contains values that represent only
                    # the fraction of proper values (for example volume of 5 minute bar can be order of
                    # magnitud lower than expected).
                    # Nevertheless to avoid data gaps, we will allow delayed data to be used as current one.
                    raw_data['Timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")
                    # raw_data['Timestamp'] = last_dt_str
                else:
                    raw_data['Timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")

            else:
                raw_data = pd.read_csv(io.StringIO(req.content.decode('utf-8')))

                if 'Error Message' in raw_data.iloc[0, 0]:
                    raise Exception(raw_data.iloc[0, 0])

                # Extract only the latest data point
                raw_data = raw_data.iloc[0:1, :]
                last_dt_str = raw_data.loc[0, 'Timestamp']
                last_dt = datetime.datetime.strptime(last_dt_str, "%Y-%m-%d %H:%M:%S")
                last_dt = time_zone['EST'].localize(last_dt)

                if last_dt < timestamp - datetime.timedelta(minutes=4):
                    logging.warning('RETURNED DATA IS DELAYED!')
                    # Accept delayed data!
                    raw_data.iloc[0, 'Timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")
                else:
                    raw_data.iloc[0, 'Timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")

            # Get rid of unwanted characters from dictionary keys
            raw_data = change_keys(raw_data, ". ", "_")

            # Cast string to int or float if possible
            raw_data = value_to_number(raw_data)

            return raw_data

        except requests.exceptions.ConnectionError as msg:
            print(msg)


def get_market_calendar():
    """Returns market calendar of the current month. Hours in Eastern Time (ET).

    """
    response = requests.get('https://api.tradier.com/v1/markets/calendar',
        headers={'Authorization': 'Bearer <TOKEN>', 'Accept': 'application/json'})
    return response.json()['calendar']['days']['day']
