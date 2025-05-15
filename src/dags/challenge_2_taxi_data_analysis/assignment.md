# Data Engineering Challenge: NYC Taxi Data Pipeline

## Assignment Overview

In this assignment, you will build and optimize an Apache Airflow data pipeline that processes New York City taxi trip data. This challenge simulates real-world data engineering tasks where you need to handle large datasets efficiently, implement proper validation, perform analysis, and store results in a duckdb database.

## Learning Objectives

By completing this assignment, you will:
- Gain hands-on experience with Apache Airflow for workflow orchestration
- Learn how to dynamically install dependencies in a production environment
- Work with Parquet files for efficient data processing
- Implement proper data validation and error handling
- Perform basic data analysis and reporting
- Store and retrieve data using DuckDB, a lightweight analytical database
- Apply best practices for data pipeline development

## Problem Statement

The NYC Taxi and Limousine Commission (TLC) publishes trip record data for taxi rides in New York City. This data is valuable for analyzing transportation patterns, economic trends, and urban mobility. However, the raw data needs to be processed, validated, and analyzed before it can provide useful insights.

Your task is to build a data pipeline that:
1. Downloads the latest NYC Yellow Taxi trip data in Parquet format
2. Validates the data structure and quality
3. Performs basic statistical analysis
4. Stores the results in a database
5. Generates a formatted report

## Requirements

### Technical Requirements

1. **Environment Setup**:
   - Your pipeline must dynamically check for and install required dependencies
   - Required packages: pandas, pyarrow, duckdb

2. **Data Processing**:
   - The pipeline must handle Parquet files efficiently
   - Implement schema validation to ensure expected columns are present
   - Check for data quality issues (missing values, invalid data types)

3. **Analysis**:
   - Calculate basic statistics on passenger counts, trip distances, and fare amounts
   - Identify distribution of trips by vendor
   - Store analysis results in a structured format (JSON)

4. **Data Storage**:
   - Create and use a DuckDB database for storing analysis results
   - Implement proper table creation and data insertion

5. **Reporting**:
   - Generate a well-formatted Markdown report with key findings
   - Include summary statistics and interpretation

6. **Error Handling and Logging**:
   - Implement comprehensive error handling throughout the pipeline
   - Maintain detailed logs of all operations
   - Branch workflow based on data validation results

### Pipeline Structure

Your DAG should include the following tasks in a logical sequence:
1. Create necessary directories
2. Install required dependencies
3. Download the taxi data Parquet file
4. Wait for file availability
5. Create database structure
6. Validate Parquet structure and data quality
7. Analyze data (only if validation passes)
8. Store results in DuckDB
9. Generate a report
10. Clean up temporary files

## Evaluation Criteria

Your solution will be evaluated based on:

1. **Functionality**: Does the pipeline execute successfully from start to finish?
2. **Code Quality**: Is the code well-organized, properly documented, and following best practices?
3. **Error Handling**: How robustly does the pipeline handle potential errors?
4. **Efficiency**: Are there optimizations to handle large data volumes efficiently?
5. **Documentation**: Is the code well-commented and easy to understand?

## Deliverables

1. A complete Airflow DAG Python file with all required functions and operators
2. A brief document explaining your approach, challenges faced, and potential improvements

## Resources

- NYC TLC Trip Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Apache Airflow Documentation: https://airflow.apache.org/docs/
- Parquet Format: https://parquet.apache.org/
- DuckDB Documentation: https://duckdb.org/docs/

## Extension Challenges (Optional)

If you complete the primary assignment, consider these extensions:

1. Implement data visualization of key metrics using a library compatible with Airflow
2. Add geographic analysis using pickup/dropoff locations
3. Create a parameterized pipeline that can process data for different time periods
4. Implement incremental processing to handle only new data since the last run
5. Add data quality tests with threshold-based alerting

Good luck with your data engineering challenge!