import requests
import os
import pandas as pd
from datetime import timezone, datetime, timedelta
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

default_date = "2024-01-01"
series_ids = ["FEDFUNDS", "CPIAUCSL", "VIXCLS"]

connection = sql.connect(
    server_hostname=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    access_token=os.getenv("DATABRICKS_API_KEY")
)

with connection.cursor() as cursor:
    cursor.execute("CREATE CATALOG IF NOT EXISTS bronze")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze.FRED")

    try:
        cursor.execute("SELECT COALESCE(MAX(etl_loaded_at), '" + default_date + "') FROM bronze.FRED.raw_fred_data")
        query_result = cursor.fetchall()
    except Exception:
        query_result = [[None]]

datetoPass = query_result[0][0].date() + timedelta(days=1) if query_result[0][0] is not None else default_date


def fetch_fred_data(series_id_list, start_date=datetoPass, end_date=datetime.now().date()):
    if not series_id_list or not isinstance(series_id_list, (list, tuple)):
        raise ValueError("series_id_list must be a non-empty list or tuple of FRED series IDs.")

    all_data = []
    for series_id in series_id_list:
        response = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": series_id,
                "api_key": os.getenv("FRED_API_KEY"),
                "observation_start": start_date,
                "observation_end": end_date,
                "file_type": "json"
            }
        )
        data = response.json()
        print(data)
        df = pd.DataFrame(data['observations'])
        df["series_id"] = series_id
        df["etl_loaded_at"] = datetime.now(timezone.utc)
        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)
    cols = ["date", "series_id", "value", "etl_loaded_at"]
    final_df = final_df[cols]
    final_df['value'] = final_df['value'].replace('.', None)
    return final_df


def load_fred_data_to_databricks(df):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.FRED.tmp_fred_data (
                date DATE,
                series_id STRING,
                value FLOAT,
                etl_loaded_at TIMESTAMP
            )
        """)
        values_list = []
        for _, row in df.iterrows():
            val = 'NULL' if pd.isna(row['value']) else row['value']
            values_list.append(f"('{row['date']}', '{row['series_id']}', {val}, '{row['etl_loaded_at']}')")

        values_str = ",\n".join(values_list)

        cursor.execute(f"""
            INSERT INTO bronze.FRED.tmp_fred_data
            (date, series_id, value, etl_loaded_at)
            VALUES {values_str}
        """)


if __name__ == "__main__":
    final_df = fetch_fred_data(series_ids)
    if not final_df.empty:
        load_fred_data_to_databricks(final_df)
        print(f"Loaded {len(final_df)} rows to bronze.FRED.tmp3_fred_data")