"""
Advanced Parquet Processing DAG with Runtime Package Installation
This DAG demonstrates how to install dependencies at runtime in Airflow
and then use them for data processing tasks.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta
import os
import subprocess
import sys
import importlib.util

# Define file paths
DOWNLOAD_PATH = "/tmp/yellow_taxi_data.parquet"
SUMMARY_PATH = "/tmp/taxi_summary.json"
REPORT_PATH = "/tmp/taxi_report.md"
DB_PATH = "/tmp/taxi_analysis.db"
LOG_PATH = "/tmp/taxi_processing.log"

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Create the DAG
dag = DAG(
    "wacheng_advanced_parquet_processing_pipeline",
    default_args=default_args,
    description="Downloads, validates, and analyzes the NYC Taxi dataset",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 29),
    catchup=False,
    tags=["wacheng", "challenge_2"],
    max_active_runs=1,
)

# Task 1: Create directory if it doesn't exist
create_dirs = BashOperator(
    task_id="create_directories",
    bash_command="mkdir -p /tmp",
    dag=dag,
)


# Task 2: Install required packages if not already installed
def install_dependencies(**kwargs):
    # Write to log first
    with open(LOG_PATH, "w") as log_file:
        log_file.write(f"{datetime.now()}: Starting package installation check\n")

    # Function to check if a package is installed
    def is_package_installed(package_name):
        return importlib.util.find_spec(package_name) is not None

    # Function to install a package if it's not already installed
    def install_package(package_name):
        try:
            if not is_package_installed(package_name):
                print(f"Installing {package_name}...")
                subprocess.check_call(
                    [sys.executable, "-m", "pip", "install", package_name]
                )
                print(f"Successfully installed {package_name}")
                with open(LOG_PATH, "a") as log_file:
                    log_file.write(f"{datetime.now()}: Installed {package_name}\n")
                return True
            else:
                print(f"{package_name} is already installed")
                with open(LOG_PATH, "a") as log_file:
                    log_file.write(
                        f"{datetime.now()}: {package_name} is already installed\n"
                    )
                return False
        except Exception as e:
            print(f"Error installing {package_name}: {str(e)}")
            with open(LOG_PATH, "a") as log_file:
                log_file.write(
                    f"{datetime.now()}: Error installing {package_name}: {str(e)}\n"
                )
            raise

    # List of packages to install - added pyarrow for parquet support
    packages = ["pandas", "duckdb", "pyarrow"]

    # Install each package if needed
    for package in packages:
        install_package(package)

    # Log completion
    with open(LOG_PATH, "a") as log_file:
        log_file.write(f"{datetime.now()}: Dependency check completed\n")

    return True


install_deps = PythonOperator(
    task_id="install_dependencies",
    python_callable=install_dependencies,
    dag=dag,
)

# Task 3: Download the parquet file
download_parquet = BashOperator(
    task_id="download_parquet",
    bash_command=f"curl -k -o {DOWNLOAD_PATH} https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
    dag=dag,
)

# Task 4: Wait for the file to be available
check_file_exists = FileSensor(
    task_id="check_file_exists",
    filepath=DOWNLOAD_PATH,
    fs_conn_id="fs_default",
    poke_interval=5,
    timeout=60,
    mode="poke",
    dag=dag,
)


# Task 5: Create DuckDB database and table
def create_duckdb_table(**kwargs):
    try:
        # Now we can safely import DuckDB as it's been installed
        import duckdb

        # Connect to DuckDB (creates file if it doesn't exist)
        conn = duckdb.connect(DB_PATH)

        # Create table for taxi analysis results
        conn.execute(
            """
        CREATE TABLE IF NOT EXISTS taxi_analysis (
            id INTEGER PRIMARY KEY,
            analysis_date VARCHAR,
            row_count INTEGER,
            vendor_counts VARCHAR,
            avg_passenger_count FLOAT,
            avg_trip_distance FLOAT,
            avg_fare_amount FLOAT
        )
        """
        )

        # Create a sequence for auto-incrementing ID if it doesn't exist
        conn.execute(
            """
        CREATE SEQUENCE IF NOT EXISTS taxi_analysis_id_seq
        """
        )

        # Commit and close
        conn.close()

        # Log success
        with open(LOG_PATH, "a") as log_file:
            log_file.write(
                f"{datetime.now()}: DuckDB database and table created successfully\n"
            )

        return True

    except Exception as e:
        with open(LOG_PATH, "a") as log_file:
            log_file.write(
                f"{datetime.now()}: Failed to create DuckDB table. Error: {str(e)}\n"
            )
        raise


create_table = PythonOperator(
    task_id="create_duckdb_table",
    python_callable=create_duckdb_table,
    dag=dag,
)


# Task 6: Validate parquet structure
def validate_parquet_structure(**kwargs):
    try:
        # Import pandas and pyarrow (now we know they're installed)
        import pandas as pd
        import pyarrow.parquet as pq

        # Check if file exists and has content
        if not os.path.exists(DOWNLOAD_PATH) or os.path.getsize(DOWNLOAD_PATH) == 0:
            raise ValueError(f"File {DOWNLOAD_PATH} doesn't exist or is empty")

        # Read parquet file metadata first to check schema without loading all data
        parquet_file = pq.ParquetFile(DOWNLOAD_PATH)
        schema = parquet_file.schema.to_arrow_schema()
        column_names = schema.names

        # Log schema information
        with open(LOG_PATH, "a") as log_file:
            log_file.write(f"{datetime.now()}: Parquet file schema: {column_names}\n")

        # Check if the parquet file has the expected columns
        expected_columns = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
        ]

        missing_columns = [col for col in expected_columns if col not in column_names]
        if missing_columns:
            raise ValueError(f"Missing expected columns: {missing_columns}")

        # Read a small sample to check data quality
        df = pd.read_parquet(DOWNLOAD_PATH, columns=expected_columns, engine="pyarrow")

        # Check for missing values in key columns
        key_columns = ["VendorID", "passenger_count", "trip_distance", "fare_amount"]
        missing_counts = df[key_columns].isnull().sum()
        if missing_counts.sum() > 0:
            print(
                f"Warning: Found missing values in key columns: {missing_counts.to_dict()}"
            )

        # Log success
        with open(LOG_PATH, "a") as log_file:
            log_file.write(
                f"{datetime.now()}: Parquet validation successful. Shape: {df.shape}\n"
            )
            log_file.write(f"{datetime.now()}: Columns: {', '.join(df.columns)}\n")

        # Check if dataset is too small for analysis
        if len(df) < 10:
            with open(LOG_PATH, "a") as log_file:
                log_file.write(
                    f"{datetime.now()}: WARNING - Dataset too small with only {len(df)} rows\n"
                )
            return "skip_analysis"
        return "analyze_data"

    except Exception as e:
        with open(LOG_PATH, "a") as log_file:
            log_file.write(
                f"{datetime.now()}: Parquet validation failed. Error: {str(e)}\n"
            )
        raise


validate_parquet = BranchPythonOperator(
    task_id="validate_parquet",
    python_callable=validate_parquet_structure,
    provide_context=True,
    dag=dag,
)

# Define a dummy task for skipping analysis
skip_analysis = DummyOperator(
    task_id="skip_analysis",
    dag=dag,
)


# Task 7: Count rows and basic analysis
def analyze_parquet(**kwargs):
    try:
        # Import pandas and pyarrow (now we know they're installed)
        import pandas as pd
        import json

        # Read the parquet file - more efficient with parquet than CSV
        df = pd.read_parquet(DOWNLOAD_PATH, engine="pyarrow")

        # Count rows
        row_count = len(df)

        # Basic analysis
        vendor_counts = df["VendorID"].value_counts().to_dict()
        passenger_stats = {
            "mean": df["passenger_count"].mean(),
            "min": df["passenger_count"].min(),
            "max": df["passenger_count"].max(),
        }
        distance_stats = {
            "mean": df["trip_distance"].mean(),
            "min": df["trip_distance"].min(),
            "max": df["trip_distance"].max(),
        }
        fare_stats = {
            "mean": df["fare_amount"].mean(),
            "min": df["fare_amount"].min(),
            "max": df["fare_amount"].max(),
        }

        # Create a more comprehensive analysis
        analysis = {
            "row_count": row_count,
            "analysis_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "vendor_counts": vendor_counts,
            "passenger_stats": passenger_stats,
            "distance_stats": distance_stats,
            "fare_stats": fare_stats,
        }

        # Save results as JSON
        with open(SUMMARY_PATH, "w") as f:
            json.dump(analysis, f, indent=2)

        # Log count result
        with open(LOG_PATH, "a") as log_file:
            log_file.write(f"{datetime.now()}: Parquet file has {row_count} rows\n")
            log_file.write(
                f"{datetime.now()}: Analysis complete and saved to {SUMMARY_PATH}\n"
            )

        # Push analysis to XCom for use by other tasks
        kwargs["ti"].xcom_push(key="analysis", value=analysis)

        return analysis
    except Exception as e:
        with open(LOG_PATH, "a") as log_file:
            log_file.write(f"{datetime.now()}: Analysis failed. Error: {str(e)}\n")
        raise


analyze_data = PythonOperator(
    task_id="analyze_data",
    python_callable=analyze_parquet,
    provide_context=True,
    dag=dag,
)


# Task 8: Store results in DuckDB
def store_in_duckdb(**kwargs):
    try:
        import duckdb
        import json
        from datetime import datetime

        # Get analysis results from XCom
        ti = kwargs["ti"]
        analysis = ti.xcom_pull(task_ids="analyze_data", key="analysis")

        if not analysis:
            raise ValueError("No analysis data found from previous task")

        # Connect to DuckDB
        conn = duckdb.connect(DB_PATH)

        # Insert the analysis results into the table
        conn.execute(
            """
            INSERT INTO taxi_analysis (
                id, analysis_date, row_count, vendor_counts, 
                avg_passenger_count, avg_trip_distance, avg_fare_amount
            ) 
            VALUES (
                nextval('taxi_analysis_id_seq'), ?, ?, ?,
                ?, ?, ?
            )
            """,
            (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                analysis["row_count"],
                json.dumps(analysis["vendor_counts"]),
                analysis["passenger_stats"]["mean"],
                analysis["distance_stats"]["mean"],
                analysis["fare_stats"]["mean"],
            ),
        )

        # Verify the insertion
        result = conn.execute(
            "SELECT * FROM taxi_analysis ORDER BY id DESC LIMIT 1"
        ).fetchone()

        # Close the connection
        conn.close()

        # Log success
        with open(LOG_PATH, "a") as log_file:
            log_file.write(
                f"{datetime.now()}: Analysis results successfully stored in DuckDB\n"
            )

        return True
    except Exception as e:
        with open(LOG_PATH, "a") as log_file:
            log_file.write(
                f"{datetime.now()}: Failed to store results in DuckDB. Error: {str(e)}\n"
            )
        raise


store_results = PythonOperator(
    task_id="store_in_duckdb",
    python_callable=store_in_duckdb,
    provide_context=True,
    dag=dag,
)


# Task 9: Generate report
def generate_report(**kwargs):
    try:
        import json
        from datetime import datetime

        # Get analysis results from XCom
        ti = kwargs["ti"]
        analysis = ti.xcom_pull(task_ids="analyze_data", key="analysis")

        if not analysis:
            raise ValueError("No analysis data found from previous task")

        # Create a markdown report
        report = f"""# NYC Yellow Taxi Data Analysis Report
Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Dataset Overview
- Source: NYC Yellow Taxi Trip Data
- Format: Parquet
- Total Records: {analysis["row_count"]:,}

## Key Statistics

### Vendor Distribution
```
{json.dumps(analysis["vendor_counts"], indent=2)}
```

### Passenger Count
- Average: {analysis["passenger_stats"]["mean"]:.2f}
- Minimum: {analysis["passenger_stats"]["min"]}
- Maximum: {analysis["passenger_stats"]["max"]}

### Trip Distance (miles)
- Average: {analysis["distance_stats"]["mean"]:.2f}
- Minimum: {analysis["distance_stats"]["min"]:.2f}
- Maximum: {analysis["distance_stats"]["max"]:.2f}

### Fare Amount ($)
- Average: {analysis["fare_stats"]["mean"]:.2f}
- Minimum: {analysis["fare_stats"]["min"]:.2f}
- Maximum: {analysis["fare_stats"]["max"]:.2f}

## Analysis Summary
This report provides a basic analysis of the NYC Yellow Taxi dataset.
The dataset contains information about taxi trips including pickup and dropoff times,
passenger counts, trip distances, and fare amounts.

## Next Steps
- Consider further analysis by time of day or geographical region
- Explore correlations between trip distance, fare amount, and passenger count
- Visualize the data with charts and maps
"""

        # Save the report
        with open(REPORT_PATH, "w") as f:
            f.write(report)

        # Log success
        with open(LOG_PATH, "a") as log_file:
            log_file.write(
                f"{datetime.now()}: Report generated and saved to {REPORT_PATH}\n"
            )

        return True
    except Exception as e:
        with open(LOG_PATH, "a") as log_file:
            log_file.write(
                f"{datetime.now()}: Failed to generate report. Error: {str(e)}\n"
            )
        raise


generate_report_task = PythonOperator(
    task_id="generate_report",
    python_callable=generate_report,
    provide_context=True,
    dag=dag,
)


# Task: Cleanup temporary files
def cleanup_temp_files(**kwargs):
    try:
        # List of files to delete
        files_to_delete = [DOWNLOAD_PATH, SUMMARY_PATH, REPORT_PATH, DB_PATH, LOG_PATH]

        # Delete each file if it exists
        for file_path in files_to_delete:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Deleted {file_path}")

        # Log cleanup success
        with open(LOG_PATH, "a") as log_file:
            log_file.write(
                f"{datetime.now()}: Temporary files cleaned up successfully\n"
            )

        return True
    except Exception as e:
        print(f"Error during cleanup: {str(e)}")
        raise


cleanup_files = PythonOperator(
    task_id="cleanup_files",
    python_callable=cleanup_temp_files,
    dag=dag,
)

# Set task dependencies
(
    create_dirs
    >> install_deps
    >> download_parquet
    >> check_file_exists
    >> create_table
    >> validate_parquet
)

# Handle the branch for small datasets
validate_parquet >> [analyze_data, skip_analysis]

# Continue with analysis flow
analyze_data >> store_results >> generate_report_task

# Both paths lead to cleanup
generate_report_task >> cleanup_files
skip_analysis >> cleanup_files
