"""
Simple NYC Taxi Data Processing DAG
This DAG demonstrates basic ETL operations on NYC Taxi data using Airflow.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import os
import pandas as pd
import json
import pyarrow  # Required for parquet files

# Define file paths
DOWNLOAD_PATH = "/tmp/yellow_taxi_data.parquet"
RESULTS_PATH = "/tmp/taxi_summary.json"
REPORT_PATH = "/tmp/taxi_report.txt"

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Create the DAG
dag = DAG(
    "wacheng_simple_parquet_processing_pipeline",
    default_args=default_args,
    description="A simple pipeline for NYC Taxi data analysis",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=["wacheng", "challenge_1"],
)

# Task 1: Download the parquet file
download_data = BashOperator(
    task_id="download_data",
    bash_command=f"curl -k -o {DOWNLOAD_PATH} https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
    dag=dag,
)


# Task 2: Analyze the data
def analyze_taxi_data(**kwargs):
    """
    Read the parquet file and calculate basic statistics.
    """
    # Make sure the file exists
    if not os.path.exists(DOWNLOAD_PATH):
        raise FileNotFoundError(f"File not found: {DOWNLOAD_PATH}")

    # Read the parquet file
    print(f"Reading parquet file: {DOWNLOAD_PATH}")
    df = pd.read_parquet(DOWNLOAD_PATH)

    # Print basic info
    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")

    # Perform basic analysis
    analysis = {
        "row_count": len(df),
        "analysis_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "column_count": len(df.columns),
        "stats": {
            "passenger_count": {
                "mean": float(df["passenger_count"].mean()),
                "min": float(df["passenger_count"].min()),
                "max": float(df["passenger_count"].max()),
            },
            "trip_distance": {
                "mean": float(df["trip_distance"].mean()),
                "min": float(df["trip_distance"].min()),
                "max": float(df["trip_distance"].max()),
            },
            "fare_amount": {
                "mean": float(df["fare_amount"].mean()),
                "min": float(df["fare_amount"].min()),
                "max": float(df["fare_amount"].max()),
            },
        },
    }

    # Save results as JSON
    with open(RESULTS_PATH, "w") as f:
        json.dump(analysis, f, indent=2)

    print(f"Analysis results saved to: {RESULTS_PATH}")
    return analysis


analyze_data = PythonOperator(
    task_id="analyze_data",
    python_callable=analyze_taxi_data,
    dag=dag,
)


# Task 3: Generate a simple report
def generate_simple_report(**kwargs):
    """
    Create a simple text report from the analysis results.
    """
    # Read the analysis results
    with open(RESULTS_PATH, "r") as f:
        analysis = json.load(f)

    # Create a simple report
    report = f"""
NYC YELLOW TAXI DATA ANALYSIS
=============================
Generated on: {analysis['analysis_date']}

DATASET OVERVIEW
---------------
Total Records: {analysis['row_count']:,}
Total Columns: {analysis['column_count']}

KEY STATISTICS
-------------
Passenger Count:
  - Average: {analysis['stats']['passenger_count']['mean']:.2f}
  - Minimum: {analysis['stats']['passenger_count']['min']}
  - Maximum: {analysis['stats']['passenger_count']['max']}

Trip Distance (miles):
  - Average: {analysis['stats']['trip_distance']['mean']:.2f}
  - Minimum: {analysis['stats']['trip_distance']['min']:.2f}
  - Maximum: {analysis['stats']['trip_distance']['max']:.2f}

Fare Amount ($):
  - Average: {analysis['stats']['fare_amount']['mean']:.2f}
  - Minimum: {analysis['stats']['fare_amount']['min']:.2f}
  - Maximum: {analysis['stats']['fare_amount']['max']:.2f}

SUMMARY
-------
This report provides a basic overview of the NYC Yellow Taxi dataset.
"""

    # Save the report
    with open(REPORT_PATH, "w") as f:
        f.write(report)

    print(report)
    print(f"Report generated and saved to: {REPORT_PATH}")
    return True


generate_report = PythonOperator(
    task_id="generate_report",
    python_callable=generate_simple_report,
    dag=dag,
)

# Task 4: Clean up temp files
cleanup = BashOperator(
    task_id="cleanup",
    bash_command=f"rm -f {DOWNLOAD_PATH} {RESULTS_PATH} {REPORT_PATH}",
    dag=dag,
)

# Set task dependencies
download_data >> analyze_data >> generate_report >> cleanup
