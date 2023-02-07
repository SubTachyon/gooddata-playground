# My thanks to Bee Guan Teo: 
# https://medium.com/the-handbook-of-coding-in-finance/building-financial-data-storage-with-postgresql-in-python-b981e38826fe
import yfinance
import psycopg2
import numpy
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(numpy.int64, psycopg2._psycopg.AsIs)

### YOUR DB CREDENTIALS
### Replace these with your own values
db_user = "deqtyrhl"
db_password = "BPKGBAHWah2a6PczLbAn6eauEPz1Nxkp"
db_host="manny.db.elephantsql.com"
db_port="5432"
db_database="deqtyrhl"

### REUSABLE DB CONNECTOR
def connect_to_my_db():
    return psycopg2.connect(user=db_user,
                            password=db_password,
                            host=db_host,
                            port=db_port,
                            database=db_database)

### FETCH STOCK DATA
def fetch_stock_data(ticker, start_date, end_date):
    stock_data = yfinance.download(ticker, start=start_date, end=end_date)
    # Bee Guan Teo's use of numpy.datetime_as_string did not work for me
    # so I elected to simply convert the Date values to string
    stock_data.index = stock_data.index.map(str)
    stock_data['Ticker'] = ticker
    stock_data = stock_data.rename(columns={"Adj Close": "Adj_Close"})
    records = stock_data.to_records(index=True)
    return records

### CREATE TABLE IN POSTGRESQL
def create_stock_table(db_table_name):
    conn = connect_to_my_db()
    conn.autocommit = True
    cur = conn.cursor()
    query = '''CREATE TABLE {} (
        Date DATE NOT NULL, 
        Open FLOAT NOT NULL, 
        High FLOAT NOT NULL, 
        Low FLOAT NOT NULL, 
        Close FLOAT NOT NULL, 
        Adj_Close FLOAT NOT NULL, 
        Volume BIGINT NOT NULL, 
        Ticker VARCHAR(255) NOT NULL
        );'''.format(db_table_name)
    cur.execute(query)
    print("Table created successfully")
    conn.close()

### INSERT DATA INTO THE POSTGRESQL TABLE
def insert_stock_data(data, db_table_name):
    conn = connect_to_my_db()
    conn.autocommit = True
    cur = conn.cursor()
    query = '''INSERT INTO {} (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''.format(db_table_name)
    cur = conn.cursor()
    cur.executemany(query, data)
    conn.close()
    print("Data inserted successfully")

### QUERY SOME RECORDS FROM THE POSTGRESQL TABLE TO CONFIRM IT WORKED
def query_data(db_table_name):
    conn = connect_to_my_db()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT * from {} LIMIT 5".format(db_table_name))
    rows = cur.fetchall()
    for row in rows:
        print(row)

    print("Query done successfully");
    conn.close()

##### EXECUTE #####

table = "apple_stock"
appl_stock = fetch_stock_data(ticker = "AAPL", start_date = "2021-8-1", end_date = "2021-9-1")
create_stock_table(db_table_name = table)
insert_stock_data(data = appl_stock, db_table_name = table)
query_data(db_table_name = table)
