import psycopg2
import numpy
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(numpy.int64, psycopg2._psycopg.AsIs)
from fetch_stock_data import fetch_stock_data

### Reusable DB connector
def connect_to_my_db(db_credentials):
    return psycopg2.connect(user=db_credentials["user"],
                            password=db_credentials["password"],
                            host=db_credentials["host"],
                            port=db_credentials["port"],
                            database=db_credentials["database"])

### CREATE TABLE IN POSTGRESQL
def create_table(db_credentials, your_query):
    conn = connect_to_my_db(db_credentials)
    conn.autocommit = True
    cur = conn.cursor()
    query = your_query
    cur.execute(query)
    print("Table created successfully")
    conn.close()

### INSERT DATA INTO THE POSTGRESQL TABLE
def insert_data(db_credentials, data, your_query):
    conn = connect_to_my_db(db_credentials)
    conn.autocommit = True
    cur = conn.cursor()
    query = your_query
    cur = conn.cursor()
    cur.executemany(query, data)
    conn.close()
    print("Data inserted successfully")

### QUERY SOME RECORDS FROM THE POSTGRESQL TABLE TO CONFIRM IT WORKED
def query_data(db_credentials, your_query):
    conn = connect_to_my_db(db_credentials)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(your_query)
    rows = cur.fetchall()
    for row in rows:
        print(row)

    print("Query done successfully");
    conn.close()