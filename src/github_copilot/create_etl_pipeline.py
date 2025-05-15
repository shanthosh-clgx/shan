import os
import pandas as pd
import requests
import json
import google.auth.transport.requests
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Stock Price ETL Pipeline
# Extract daily stock price data from Alpha Vantage API
# Transform the data by adding calculated fields like daily change percentage
# Load the data into our BigQuery data warehouse

# Load environment variables from .env file
load_dotenv("src/github-copilot/.env")


def extract_stock_data(symbol, api_key):
    """Extract daily stock price data from Alpha Vantage API"""
    print(f"Extracting stock data for {symbol}...")

    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "full",
        "apikey": api_key,
    }

    response = requests.get(url, params=params, verify=False)
    data = response.json()

    if "Error Message" in data:
        raise Exception(f"API Error: {data['Error Message']}")

    if "Time Series (Daily)" not in data:
        raise Exception("No daily time series found in API response")

    # Convert the nested JSON to a pandas DataFrame
    time_series = data["Time Series (Daily)"]
    df = pd.DataFrame.from_dict(time_series, orient="index")

    # Rename columns
    df.rename(
        columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume",
        },
        inplace=True,
    )

    # Convert string values to appropriate types
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col])
    df["volume"] = pd.to_numeric(df["volume"], downcast="integer")

    # Add date column from index
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"])

    # Add symbol column
    df["symbol"] = symbol

    return df


def transform_stock_data(df):
    """
    Transforms stock market data by calculating various metrics for analysis.

    This function processes a DataFrame containing stock data by:
    1. Sorting the data by date in ascending order
    2. Calculating the absolute daily price change
    3. Computing the percentage of daily price change
    4. Determining the previous day's closing price
    5. Calculating the day-over-day percentage change
    6. Handling missing values for the first data point
    7. Adding an ETL timestamp

    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame containing stock data with at least 'date', 'open', and 'close' columns.

    Returns:
    --------
    pandas.DataFrame
        Transformed DataFrame with additional columns:
        - daily_change: Absolute difference between close and open prices
        - daily_change_pct: Percentage change between close and open prices
        - prev_close: Previous day's closing price
        - day_over_day_pct: Percentage change compared to previous day's close
        - etl_timestamp: Timestamp indicating when the transformation was performed

    Notes:
    ------
    - The function assumes input DataFrame has 'date', 'open', and 'close' columns
    - The first row's prev_close is filled with the open price of that day
    - The first row's day_over_day_pct is set to 0
    """
    """Transform the data by adding calculated fields like daily change percentage"""
    print("Transforming stock data...")

    # Sort by date in ascending order to properly calculate changes
    df = df.sort_values("date")

    # Calculate daily change (absolute)
    df["daily_change"] = df["close"] - df["open"]

    # Calculate daily change percentage
    df["daily_change_pct"] = (df["daily_change"] / df["open"]) * 100

    # Calculate previous day's close
    df["prev_close"] = df["close"].shift(1)

    # Calculate day-over-day change percentage
    df["day_over_day_pct"] = ((df["close"] - df["prev_close"]) / df["prev_close"]) * 100

    # Handle NaN values for the first row where prev_close doesn't exist
    df["prev_close"].fillna(df["open"], inplace=True)
    df["day_over_day_pct"].fillna(0, inplace=True)

    # Add extraction timestamp
    df["etl_timestamp"] = datetime.now()

    return df


def load_to_bigquery_rest_api(
    df, project_id, dataset_id, table_id, credentials_path=None
):
    """
    Load data into BigQuery using direct REST API calls (no jobs.create permission needed)
    """
    print(f"Loading data to BigQuery: {project_id}.{dataset_id}.{table_id}")

    # Set up credentials
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
    else:
        # Use default credentials
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
        )

    # Create an authenticated session
    session = requests.Session()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    session.headers.update(
        {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json",
        }
    )

    # Define the fully-qualified table name
    table_id_full = f"{project_id}.{dataset_id}.{table_id}"

    # Define the API endpoint for table data insertion
    # Using the tabledata.insertAll method which doesn't require jobs.create
    api_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}/insertAll"

    # Optimize batch size - larger batches are more efficient but insertAll has limits
    batch_size = 500
    total_rows = 0

    # Process in batches
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i : i + batch_size]

        # Format each row into the required format for the insertAll API
        rows = []
        for _, row in batch_df.iterrows():
            # Convert all values to appropriate JSON-serializable format
            formatted_row = {}
            for col, val in row.items():
                if pd.isna(val):
                    # None values are handled correctly by the JSON serializer
                    formatted_row[col] = None
                elif isinstance(val, datetime):
                    # Format datetime objects to strings
                    if col == "date":
                        formatted_row[col] = val.strftime("%Y-%m-%d")
                    else:
                        formatted_row[col] = val.strftime("%Y-%m-%d %H:%M:%S")
                elif isinstance(val, (int, float, str, bool)):
                    # Use primitive values directly
                    formatted_row[col] = val
                else:
                    # Convert any other types to strings
                    formatted_row[col] = str(val)

            # Add the row to the batch
            rows.append({"json": formatted_row})

        # Create the request payload
        payload = {
            "kind": "bigquery#tableDataInsertAllRequest",
            "skipInvalidRows": False,
            "ignoreUnknownValues": False,
            "rows": rows,
        }

        # Send the request
        response = session.post(api_url, json=payload)

        # Check for errors
        if response.status_code != 200:
            error_msg = response.json().get("error", {}).get("message", "Unknown error")
            raise Exception(f"Error inserting data: {error_msg}")

        # Check for insertion errors in the response
        result = response.json()
        if result.get("insertErrors"):
            error_details = json.dumps(result["insertErrors"], indent=2)
            raise Exception(f"Errors inserting rows: {error_details}")

        # Update progress
        total_rows += batch_df.shape[0]
        print(f"Inserted batch of {batch_df.shape[0]} rows")

    print(f"Total: Loaded {total_rows} rows to {table_id_full}")
    return total_rows


def run_stock_price_etl(
    symbols, api_key, project_id, dataset_id, table_id, credentials_path=None
):
    """Run the full ETL pipeline for stock prices"""
    all_data = []

    for symbol in symbols:
        # Extract
        df = extract_stock_data(symbol, api_key)

        # Transform
        df = transform_stock_data(df)

        all_data.append(df)

    # Combine all stock data
    combined_df = pd.concat(all_data, ignore_index=True)

    # Load
    load_to_bigquery_rest_api(
        combined_df, project_id, dataset_id, table_id, credentials_path
    )

    return combined_df


if __name__ == "__main__":
    # Configuration
    API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    DATASET_ID = os.environ.get("BQ_DATASET_ID")
    TABLE_ID = "stock_daily_prices"
    CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    # List of stock symbols to process
    SYMBOLS = ["MSFT"]

    # Run the ETL pipeline
    run_stock_price_etl(
        SYMBOLS, API_KEY, PROJECT_ID, DATASET_ID, TABLE_ID, CREDENTIALS_PATH
    )
