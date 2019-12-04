import requests
import json
import pandas as pd
import io
import datetime
import pytz
import logging
from config import time_zone


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
            Timestamp of real-time data.

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
                raw_data['timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")
            if isinstance(raw_data, list):
                for mssg in raw_data:
                    mssg['timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")

        except requests.exceptions.ConnectionError as mssg:
            print(mssg)

        return raw_data


    def get_av_data(self, timestamp, function=None, symbol=None, interval=None, request=None):
        """Get the data from the Alpha Vantage API. With INTRADAY functions or FOREX(DAILY, WEEKLY, MONTHLY)
        returns 100 latest data points, with FOREX(INTRADAY) returns real-time data point, otherwise the time series
        covering 20+ years of historical data is returned.

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
            Timestamp of real-time data.
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

                if 'Error Message' in raw_data:
                    raise Exception(raw_data['Error Message'])

                keys_level_1 = list(raw_data.keys())
                last_dt_str = list(raw_data[keys_level_1[1]].keys())[0]
                last_dt = datetime.datetime.strptime(last_dt_str, "%Y-%m-%d %H:%M:%S")
                last_dt = pytz.utc.localize(last_dt).astimezone(time_zone['EST'])

                # Extract only the last data point
                raw_data = raw_data[keys_level_1[1]][last_dt_str]

                if last_dt < timestamp - datetime.timedelta(minutes=4):
                    logging.warning('RETURNED DATA IS DELAYED!')
                    raw_data['timestamp'] = last_dt_str
                else:
                    raw_data['timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")

            else:
                raw_data = pd.read_csv(io.StringIO(req.content.decode('utf-8')))

                if 'Error Message' in raw_data.iloc[0, 0]:
                    raise Exception(raw_data.iloc[0, 0])

                # Extract only the last data point
                raw_data = raw_data.iloc[0:1, :]
                last_dt_str = raw_data.loc[0, 'timestamp']
                last_dt = datetime.datetime.strptime(last_dt_str, "%Y-%m-%d %H:%M:%S")
                last_dt = pytz.utc.localize(last_dt).astimezone(time_zone['EST'])

                if last_dt < timestamp - datetime.timedelta(minutes=4):
                    logging.warning('RETURNED DATA IS DELAYED!')
                else:
                    raw_data.iloc[0, 'timestamp'] = datetime.datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")


        except requests.exceptions.ConnectionError as msg:
            print(msg)

        return raw_data


def get_market_calendar():
    """Returns market calandar of current month. Hours in Eastern Time (ET).

    """
    response = requests.get('https://api.tradier.com/v1/markets/calendar',
        headers={'Authorization': 'Bearer <TOKEN>', 'Accept': 'application/json'})
    return response.json()['calendar']['days']['day']