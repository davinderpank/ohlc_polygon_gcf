import requests
import pandas as pd
from common.bq_upload import df_to_bqupload
from common.config import POLYGON_API_KEY
from datetime import datetime

# request variables
params = {'adjusted': 'true',
          'apiKey': POLYGON_API_KEY,
          }

base_url = 'https://api.polygon.io/v2/'
ohlc_url = 'aggs/grouped/locale/us/market/stocks/'
trading_day = datetime.today().strftime('%Y-%m-%d')

# bq variables
project = 'stock-data-331621'
table_name = 'historical.stock-ohlc-us'


def stock_ohlc_load(request):

    try:
        r = requests.get(base_url + ohlc_url + trading_day, params=params)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)
    else:
        response_content = r.json()

        if response_content['resultsCount'] > 0:
            results = response_content['results']

            # column of data response {T: ticker, c: close price, h: high price, l: lowest price, o: open price,
            #                           n: number of transactions, t: UNIX Msec for start of day/window, v: trading volume,
            #                           vw: volume weighted price}

            df = pd.DataFrame(results)
            df.rename(columns={'T': 'ticker', 'c': 'close', 'h': 'high', 'o': 'open', 'l': 'low', 'n': 'transactions',
                               'v': 'volume', 'vw': 'volume_weighted_price', 't': 'date'}, inplace=True)

            # convert UNIX Msec to pandas datetime
            df['date'] = pd.to_datetime(df['date'], unit='ms')

            # remove NaNs where there have been no trades
            df.fillna(0, inplace=True)

            # change ordering of columns
            df = df[['date', 'ticker', 'open', 'close', 'high', 'low', 'transactions', 'volume', 'volume_weighted_price']]

            # upload to bq
            df_to_bqupload(project=project, table_name=table_name, df=df)

            return f"Load for {trading_day} completed at {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"

        else:
            return f"No data available for {trading_day}, attempted at {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"

