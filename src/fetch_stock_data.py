import pymysql
import os
import urllib.request
import json

from dotenv import load_dotenv
load_dotenv(verbose=True)

def get_connection():
    db = pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_DATABASE")
    )

    return db

"""
Returns a dictionary of stock data where the keys are dates and the value is a dict containing open, high, low, etc
"""
def get_data_from_api(symbol):
    api_key = os.getenv("API_KEY")
    base_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&apikey=' + api_key + '&symbol='

    url = base_url + symbol

    response = urllib.request.urlopen(url)
    data = json.loads(response.read())

    daily_data = data["Time Series (Daily)"]
    return daily_data

"""
Saves the stock data into the database
If there's already a value for this symbol on this date, update the row instead of inserting a new one
"""
def save_data(cursor, symbol, date, open, close, volume):

    query = '''
        INSERT INTO PRICES (`symbol`, `date`, `open`, `close`, `volume`)
        VALUES ('{symbol}', '{date}', '{open}', '{close}', '{volume}')
        ON DUPLICATE KEY UPDATE
            open='{open}',
            close='{close}',
            volume='{volume}'
    '''.format(
        symbol=symbol, date=date, open=open, close=close, volume=volume
    )
    cursor.execute(query)

def main():
    db = get_connection()
    cursor = db.cursor()

    symbol_list = ['MSFT']
    for symbol in symbol_list:
        daily_data = get_data_from_api(symbol)

        for date in daily_data:
            open = daily_data[date]['1. open']
            close = daily_data[date]['4. close']
            volume = daily_data[date]['5. volume']

            save_data(db, cursor, symbol, date, open, close, volume)

        db.commit()

    db.close()

if __name__ == "__main__":
    main()