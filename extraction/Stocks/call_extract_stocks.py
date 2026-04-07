import extraction.Stocks.extract_stocks as extract_stocks
tickers = ["AAPL", "MSFT", "GOOG"]
extract_stocks.fetch_multiple_tickers(tickers)
extract_stocks.load_tickers(tickers)