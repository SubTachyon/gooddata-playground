import yfinance

def fetch_stock_data(ticker, start_date, end_date):
    stock_data = yfinance.download(ticker, start=start_date, end=end_date)
    # numpy.datetime_as_string did not work for me
    # so I elected to simply convert the Date values to string
    stock_data.index = stock_data.index.map(str)
    stock_data['Ticker'] = ticker
    stock_data = stock_data.rename(columns={"Adj Close": "Adj_Close"})
    records = stock_data.to_records(index=True)
    return records