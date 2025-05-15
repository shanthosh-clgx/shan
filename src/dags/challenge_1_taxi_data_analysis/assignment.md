# NYC Taxi Data Processing - Airflow Assignment

## Overview
This assignment will help you develop practical skills with Apache Airflow by building a data pipeline from scratch that processes NYC Yellow Taxi trip data. You'll create a complete DAG that demonstrates common ETL operations including data download, analysis, reporting, and cleanup.

## Directory Setup and Naming Conventions
### File Location
Store your work in: idap_data_pipelines_aus-refdatapipeline-skilltraining/src/dags/{your_isc_account_name}/

### File Naming
For the first assignment, prefix your Python file with challenge_1_
Example: idap_data_pipelines_aus-refdatapipeline-skilltraining/src/dags/wacheng/challenge_1_taxi_data_analysis.py

### DAG Configuration
Use your ISC account name in the DAG definition as shown below:

```python
pythondag = DAG(
    "{your_isc_account_name}_simple_parquet_processing_pipeline",
    default_args=default_args,
    description="A simple pipeline for NYC Taxi data analysis",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=["{your_isc_account_name}", "challenge_1"],
)
```

## Learning Objectives
- Create a complete Airflow DAG from scratch
- Implement both Bash and Python operators
- Apply data analysis techniques within Airflow
- Configure task dependencies in a pipeline
- Use proper error handling in Airflow tasks
- Gain hands-on experience with a realistic data engineering workflow

## Prerequisites
- Basic understanding of Python
- Familiarity with pandas and data manipulation
- Access to an Airflow environment (local or shared development environment)
- Basic understanding of ETL concepts

## Assignment Description

You will build an Airflow DAG that:
1. Downloads NYC Yellow Taxi data in parquet format
2. Performs basic analysis on the dataset
3. Generates a simple text report with the analysis results
4. Cleans up temporary files

## Requirements

Your DAG should include the following components:

### 1. DAG Configuration
- Create a DAG with appropriate configuration:
  - Set a meaningful DAG ID (using your name or username for identification)
  - Configure it to run daily
  - Set appropriate default arguments
  - Add descriptive tags
  - Disable catchup
  - Include a description

### 2. Task 1: Download Data
- Implement a BashOperator that:
  - Downloads the NYC Yellow Taxi data for January 2023 in parquet format
  - Saves it to a temporary location (/tmp/yellow_taxi_data.parquet)
  - Uses the URL: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

### 3. Task 2: Analyze Data
- Implement a PythonOperator that:
  - Reads the downloaded parquet file using pandas
  - Validates that the file exists before processing
  - Calculates basic statistics for:
    - Passenger count (mean, min, max)
    - Trip distance (mean, min, max)
    - Fare amount (mean, min, max)
  - Saves the analysis results as a JSON file (/tmp/taxi_summary.json)
  - Returns the analysis results

### 4. Task 3: Generate Report
- Implement a PythonOperator that:
  - Reads the analysis results from the JSON file
  - Creates a formatted text report with:
    - A title and timestamp
    - Dataset overview (row count, column count)
    - Formatted statistics for passenger count, trip distance, and fare amount
    - A brief summary
  - Saves the report to a text file (/tmp/taxi_report.txt)
  - Prints the report to the Airflow logs

### 5. Task 4: Cleanup
- Implement a BashOperator that:
  - Removes all temporary files created during the pipeline execution
  - Executes only after all other tasks have completed successfully

### 6. Task Dependencies
- Configure the proper task dependencies to ensure:
  - Data is downloaded before analysis
  - Analysis is completed before report generation
  - Cleanup occurs only after the report is generated

## Submission Guidelines

Your submission should include:
1. The complete Python file(named challenge_1_taxi_data_analysis.py) containing your DAG, placed under folder src/dags/{your_isc_account},
please define the DAG with following attributes 
dag = DAG(
    "{your_isc_account}_simple_parquet_processing_pipeline",
    default_args=default_args,
    description="A simple pipeline for NYC Taxi data analysis",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=["{your_isc_account}", "challenge_1"],
)

## Evaluation Criteria

Your assignment will be evaluated based on:
- Correct implementation of all required tasks
- Code quality and adherence to Airflow best practices
- Proper error handling
- Clear documentation and comments
- Successful execution of the full pipeline

## Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

## Due Date

Please complete and submit this assignment by 15th June.

Good luck, and happy coding!