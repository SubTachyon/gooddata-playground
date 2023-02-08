# My thanks to Bee Guan Teo: 
# https://medium.com/the-handbook-of-coding-in-finance/building-financial-data-storage-with-postgresql-in-python-b981e38826fe
import pandas
import numpy
from fetch_stock_data import fetch_stock_data
from postgresql import create_table, insert_data, query_data

# Credentials for the postgresql db you are using
db_credentials = {
    "user": "deqtyrhl",
    "password": "BPKGBAHWah2a6PczLbAn6eauEPz1Nxkp",
    "host": "manny.db.elephantsql.com",
    "port": "5432",
    "database": "deqtyrhl"
}

### Create a table with my asset records ###

df = pandas.read_csv('assets.csv')
my_assets_data = df.values
assets_table_name = "assets"
create_table(
    db_credentials = db_credentials, 
    your_query = '''CREATE TABLE {} (
    Stock VARCHAR(255) NOT NULL, 
    Ticker VARCHAR(255) NOT NULL, 
    Date_of_purchase DATE NOT NULL, 
    Quantity FLOAT NOT NULL
    );'''.format(assets_table_name))
insert_data(
    db_credentials = db_credentials, 
    data = my_assets_data, 
    your_query = '''INSERT INTO {} (Stock, Ticker, Date_of_purchase, Quantity)
    VALUES (%s, %s, %s, %s)'''.format(assets_table_name))
print("Sample data from the 'assets' table:")
query_data(
    db_credentials = db_credentials, 
    your_query = "SELECT * from {} LIMIT 5".format(assets_table_name))

### Create a table with my stock records ###

apple_stock = fetch_stock_data(ticker = "AAPL", start_date = "2021-1-1", end_date = "2023-1-1")
microsoft_stock = fetch_stock_data(ticker = "MSFT", start_date = "2021-1-1", end_date = "2023-1-1")
pfizer_stock = fetch_stock_data(ticker = "PFE", start_date = "2021-1-1", end_date = "2023-1-1")
stocks_data = numpy.concatenate((apple_stock, microsoft_stock, pfizer_stock))

stocks_table_name = "stocks"
create_table(
    db_credentials = db_credentials, 
    your_query = '''CREATE TABLE {} (
    Date DATE NOT NULL, 
    Open FLOAT NOT NULL, 
    High FLOAT NOT NULL, 
    Low FLOAT NOT NULL, 
    Close FLOAT NOT NULL, 
    Adj_Close FLOAT NOT NULL, 
    Volume BIGINT NOT NULL, 
    Ticker VARCHAR(255) NOT NULL
    );'''.format(stocks_table_name))
insert_data(
    db_credentials = db_credentials, 
    data = stocks_data, 
    your_query = '''INSERT INTO {} (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''.format(stocks_table_name))
print("Sample data from the 'stocks' table:")
query_data(
    db_credentials = db_credentials, 
    your_query = "SELECT * from {} LIMIT 5".format(stocks_table_name))

