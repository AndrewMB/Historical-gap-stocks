import pymysql
import os

from dotenv import load_dotenv
load_dotenv(verbose=True)

"""
Create a connection to the database
"""
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
Fetch all of the stock symbols that we want data for from the database
"""
def get_symbols(db, cursor):
    query = 'SELECT * FROM symbols'
    cursor.execute(query)
    results = cursor.fetchall()

    symbols = []
    for row in results:
        symbols.append(row[0])

    db.commit()
    return symbols

"""
Fetch all of the prices for a given stock
"""
def get_price_data(db, cursor, symbol):
    query = "SELECT * FROM prices where symbol = '{symbol}' order by date asc".format(symbol=symbol)
    cursor.execute(query)
    results = cursor.fetchall()
    db.commit()
    return results

"""
Calculate the gap based on yesterday's close and today's open
"""
def calculate_gap(yesterday, today):
    previous_close = yesterday[4]
    today_open = today[3]

    # Gap up
    if today_open > previous_close:
        gap = today_open / previous_close - 1
    else:
        gap = 1 - previous_close / today_open

    gap = gap * 100
    return gap

"""
Update the gap column in the database for a singe row
"""
def update_gap(db, cursor, id, gap):
    query = "UPDATE prices SET `gap` = '{gap}' where id = {id}".format(gap=gap, id=id)
    cursor.execute(query)
    db.commit()


def set_null_gaps_to_zero(db, cursor):
    query = "UPDATE prices SET `gap` = 0 where gap IS NULL"
    cursor.execute(query)
    db.commit()

def main():
    db = get_connection()
    cursor = db.cursor()

    # Get all of the symbols from the database that we want to execute on
    symbol_list = get_symbols(db, cursor)

    for symbol in symbol_list:

        # Get all of the price data by date
        price_data = get_price_data(db, cursor, symbol)

        # For each date, calculate what the gap is relative to yesterday's price and update that date's gap in the db
        for i in range(1, len(price_data)):
            today = price_data[i]
            yesterday = price_data[i-1]
            id = today[0]
            gap = calculate_gap(yesterday, today)
            update_gap(db, cursor, id, gap)

        print("Finished updating gap for symbol: " + symbol)

    # All of the very first days that we're tracking are still NULL, so set those to 0
    set_null_gaps_to_zero(db, cursor)
    db.close()

if __name__ == "__main__":
    main()