# My thanks to Bee Guan Teo: 
# https://medium.com/the-handbook-of-coding-in-finance/building-financial-data-storage-with-postgresql-in-python-b981e38826fe
import psycopg2
import numpy
import pandas
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(numpy.int64, psycopg2._psycopg.AsIs)
from fetch_stock_data import fetch_stock_data

db_credentials = {
    "user": "deqtyrhl",
    "password": "BPKGBAHWah2a6PczLbAn6eauEPz1Nxkp",
    "host": "manny.db.elephantsql.com",
    "port": "5432",
    "database": "deqtyrhl"
}

### Reusable DB connector
def connect_to_my_db(db_credentials):
    return psycopg2.connect(user=db_credentials["user"],
                            password=db_credentials["password"],
                            host=db_credentials["host"],
                            port=db_credentials["port"],
                            database=db_credentials["database"])

stocks_table_name = "stocks"

create_stocks_table_query = '''CREATE TABLE {} (
    Date DATE NOT NULL, 
    Open FLOAT NOT NULL, 
    High FLOAT NOT NULL, 
    Low FLOAT NOT NULL, 
    Close FLOAT NOT NULL, 
    Adj_Close FLOAT NOT NULL, 
    Volume BIGINT NOT NULL, 
    Ticker VARCHAR(255) NOT NULL
    );'''.format(stocks_table_name)

insert_to_stocks_table_query = '''INSERT INTO {} (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''.format(stocks_table_name)

fetch_sample_records_query = "SELECT * from {} LIMIT 5".format(stocks_table_name)

### CREATE TABLE IN POSTGRESQL
def create_stock_table(your_query):
    conn = connect_to_my_db(db_credentials)
    conn.autocommit = True
    cur = conn.cursor()
    query = your_query
    cur.execute(query)
    print("Table created successfully")
    conn.close()

### INSERT DATA INTO THE POSTGRESQL TABLE
def insert_stock_data(data, your_query):
    conn = connect_to_my_db(db_credentials)
    conn.autocommit = True
    cur = conn.cursor()
    query = your_query
    cur = conn.cursor()
    cur.executemany(query, data)
    conn.close()
    print("Data inserted successfully")

### QUERY SOME RECORDS FROM THE POSTGRESQL TABLE TO CONFIRM IT WORKED
def query_data(your_query):
    conn = connect_to_my_db(db_credentials)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(your_query)
    rows = cur.fetchall()
    for row in rows:
        print(row)

    print("Query done successfully");
    conn.close()

### Import some dummy asset data:
def give_me_my_assets_data():
    
    df = pandas.read_csv('assets.csv')

    print("Asset data:") 
    print(df.to_string()) 

    return df

##### EXECUTE #####

#give_me_my_assets_data()

table = "stocks"
appl_stock = fetch_stock_data(ticker = "AAPL", start_date = "2021-8-1", end_date = "2021-9-1")
create_stock_table(your_query = create_stocks_table_query)
insert_stock_data(data = appl_stock, your_query = insert_to_stocks_table_query)
query_data(your_query = fetch_sample_records_query)
