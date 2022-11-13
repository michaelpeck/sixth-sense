import os
from dotenv import load_dotenv
import hashlib
from datetime import datetime, timedelta
from polygon import RESTClient
import pandas as pd
from pandasgui import show


load_dotenv()
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

client = RESTClient(api_key=POLYGON_API_KEY)

from pandas.io import sql
from sqlalchemy import create_engine

engine = create_engine(os.getenv('SQLALCHEMY_DATABASE_URI'))


ticker_list = [
    # {
    #     'ticker': 'X:ADAUSD',
    #     'asset': 'ADA',
    #     'base': 'USD',
    #     'start_timestamp': 1519669920, 
    # },
    # {
    #     'ticker': 'X:BTCUSD',
    #     'asset': 'BTC',
    #     'base': 'USD',
    #     'start_timestamp': 1483228800, 
    # },
    # {
    #     'ticker': 'X:ETHUSD',
    #     'asset': 'ETH',
    #     'base': 'USD',
    #     'start_timestamp': 1483228800, 
    # },
    # {
    #     'ticker': 'X:DOTUSD',
    #     'asset': 'DOT',
    #     'base': 'USD',
    #     'start_timestamp': 1597784760, 
    # },
    # {
    #     'ticker': 'X:SOLUSD',
    #     'asset': 'SOL',
    #     'base': 'USD',
    #     'start_timestamp': 1614246000, 
    # },
    # {
    #     'ticker': 'X:ATOMUSD',
    #     'asset': 'ATOM',
    #     'base': 'USD',
    #     'start_timestamp': 1579025760, 
    # },
    # {
    #     'ticker': 'X:ALGOUSD',
    #     'asset': 'ALGO',
    #     'base': 'USD',
    #     'start_timestamp': 1565884980, 
    # },
    # {
    #     'ticker': 'X:LINKUSD',
    #     'asset': 'LINK',
    #     'base': 'USD',
    #     'start_timestamp': 1561653120, 
    # },
    # {
    #     'ticker': 'X:XLMUSD',
    #     'asset': 'XLM',
    #     'base': 'USD',
    #     'start_timestamp': 1484684220, 
    # },
    {
        'ticker': 'X:VETUSD',
        'asset': 'VET',
        'base': 'USD',
        'start_timestamp': 1535460660, 
    },
    {
        'ticker': 'X:MATICUSD',
        'asset': 'MATIC',
        'base': 'USD',
        'start_timestamp': 1615482360, 
    },
]

stock_list = [
    {
        'ticker': 'QQQ',
        'name': 'Invesco QQQ Trust, Series 1',
        'asset': 'QQQ',
        'base': 'USD',
        'start_timestamp': 1504636860, 
    },
    {
        'ticker': 'SPY',
        'name': 'SPDR S&P 500 ETF Trust',
        'asset': 'SPY',
        'base': 'USD',
        'start_timestamp': 1504636860, 
    },
    {
        'ticker': 'VOO',
        'name': 'Vanguard S&P 500 ETF',
        'asset': 'VOO',
        'base': 'USD',
        'start_timestamp': 1504636860, 
    },
        {
        'ticker': 'DIA',
        'name': 'SPDR Dow Jones Industrial Average ETF Trust',
        'asset': 'DIA',
        'base': 'USD',
        'start_timestamp': 1504636860, 
    },
    {
        'ticker': 'VTWO',
        'name': 'Vanguard Russell 2000 ETF',
        'asset': 'VTWO',
        'base': 'USD',
        'start_timestamp': 1504637460, 
    },
    {
        'ticker': 'VTI',
        'name': 'Vanguard Total Stock Market ETF',
        'asset': 'VTI',
        'base': 'USD',
        'start_timestamp': 1504636800, 
    },
    
]

def hash(sourcedf,destinationdf,*column):
    destinationdf['id'] = pd.DataFrame(sourcedf[list(column)].values.sum(axis=1))[0].str.encode('utf-8').apply(lambda x: (hashlib.blake2b(x, digest_size=32).hexdigest().upper()))

def stringify_time(row):
   return str(row['timestamp'])

for ticker in stock_list:
    
    # print(aggs)
    # print(len(aggs))
    start_timestamp = ticker['start_timestamp']
    start_datetime = datetime.fromtimestamp(start_timestamp)
    active_datetime = start_datetime
    full_set = list()
    run = 1

    while active_datetime < datetime.now() + timedelta(minutes=-30):
        
        end_datetime = active_datetime + timedelta(minutes=50000)
        print(end_datetime.timestamp() * 1000)
        if end_datetime > datetime.now() + timedelta(minutes=-20):
            end_datetime = datetime.now() + timedelta(minutes=-20)
        
        aggs = client.get_aggs(ticker['ticker'], 1, "minute", int(active_datetime.timestamp() * 1000), int(end_datetime.timestamp() * 1000), limit=50000)
        if run == 1:
            df = pd.DataFrame(aggs)
        else:
            df = df.append(aggs, ignore_index=True, sort=False)
            
        active_datetime = end_datetime
        run += 1
            
    df = df.assign(ticker=ticker['ticker'])
    df = df.assign(updated=int(datetime.now().timestamp() * 1000))
    df = df.assign(asset=ticker['asset'])
    df = df.assign(base=ticker['base'])
    
    # df['time_string'] = df.apply(lambda row: stringify_time(row), axis=1)

    # hash(df,df,'time_string','asset','base')
    # df.set_index('id', inplace=True)
    # df = df.drop(columns=['time_string'])
    # show(df)
    with engine.connect() as connection:
        df.to_sql(con=connection, name='ohlcvt', if_exists='append', index=False)
