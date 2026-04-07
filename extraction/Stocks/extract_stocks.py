from pickle import load

import yfinance as yf
import pandas as pd
import os
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

connection = sql.connect(
   server_hostname=os.getenv("DATABRICKS_HOST"),
   http_path=os.getenv("DATABRICKS_HTTP_PATH"),
   access_token=os.getenv("DATABRICKS_API_KEY") 
)

v_defaultdate = "2024-01-01"

with connection.cursor() as cursor:
 cursor.execute("CREATE catalog IF NOT EXISTS bronze")
 cursor.execute("CREATE schema IF NOT EXISTS bronze.stocks")

 try:
   cursor.execute("Select Coalesce(Max(etl_loaded_at), '"+v_defaultdate+"') from bronze.stocks.raw_stock_prices")
   query_result = cursor.fetchall()
 except Exception:
  query_result = [[None]]

v_datetoPass = query_result[0][0].date() + timedelta(days=1) if query_result[0][0] is not None else v_defaultdate



def fetch_multiple_tickers(ticker_list, start_date=v_datetoPass, end_date=datetime.now().date(), interval="1d", period=None):
    """
    Fetch historical data for multiple tickers using yfinance.

    :param ticker_list: List of ticker symbols (e.g., ["AAPL", "MSFT", "GOOG"])
    :param start_date: Start date in 'YYYY-MM-DD' format (optional)
    :param end_date: End date in 'YYYY-MM-DD' format (optional)
    :param interval: Data interval (e.g., "1d", "1h", "5m")
    :param period: yfinance `period` string (e.g., "1d", "5d", "1mo"). If set, `start_date`/`end_date` are ignored.
                   If `period` is None and both `start_date` and `end_date` are None, defaults to '1d' (latest).
    :return: Pandas DataFrame with historical data
    """
    if not ticker_list or not isinstance(ticker_list, (list, tuple)):
        raise ValueError("ticker_list must be a non-empty list or tuple of ticker symbols.")

    try:
        tickers_str = " ".join(ticker_list)  # yfinance accepts space-separated tickers

        # If no dates and no period provided, default to the most recent day.
        if period is None and start_date is None and end_date is None:
            period = "1d"

        if period:
            data = yf.download(
                tickers=tickers_str,
                period=period,
                interval=interval,
                group_by="ticker",
                auto_adjust=True,
                threads=True,
            )
        else:
            data = yf.download(
                tickers=tickers_str,
                start=start_date,
                end=end_date,
                interval=interval,
                group_by="ticker",  # Keeps data grouped by ticker
                auto_adjust=True,   # Adjusts for splits/dividends
                threads=True        # Parallel download

            )
        data = data.stack(level=0).reset_index()
        data.columns = ["date", "ticker", "close", "high", "low", "open", "volume"]
        data = data[["date", "ticker", "open", "high", "low", "close", "volume"]]
        data["etl_loaded_at"] = datetime.now(timezone.utc)
        return data


    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()
    
def load_tickers(tickers):
# Fetch the latest available pricing (omit dates -> defaults to latest day)

 df_latest = fetch_multiple_tickers(tickers)
 if not df_latest.empty:
  with connection.cursor() as cursor:
   cursor.execute("""
    CREATE OR REPLACE TABLE bronze.stocks.tmp_raw_stock_prices (
     date Date,
     ticker STRING,
     open DOUBLE,
     high DOUBLE,
     low DOUBLE,
     close DOUBLE,
     volume LONG,
     etl_loaded_at TIMESTAMP
    )
   """)
   # Build one big INSERT with all rows
   values_list = []
   for _, row in df_latest.iterrows():
    values_list.append(f"('{row['date']}', '{row['ticker']}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {int(row['volume'])}, '{row['etl_loaded_at']}')")

   values_str = ",\n".join(values_list)

   cursor.execute(f"""
    INSERT INTO bronze.stocks.tmp_raw_stock_prices 
    (date, ticker, open, high, low, close, volume, etl_loaded_at)
    VALUES {values_str}
   """)

# Example usage
if __name__ == "__main__":
 tickers = ["MSFT"]
 fetch_multiple_tickers(tickers)
 load_tickers(tickers)


 