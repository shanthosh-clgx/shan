# Taxi Data Analysis - Challenge 1

## Overview
This DAG performs analysis on taxi ride data as part of Challenge 1 for the skill training pipeline.

## Purpose
The purpose of this DAG is to demonstrate data processing techniques on taxi trip data, likely including:
- Data extraction from source
- Data cleaning and transformation
- Analysis of trip patterns, fares, and metrics
- Generation of insights or reports

## DAG Structure
The DAG consists of several tasks that move data through a processing pipeline:

1. **Extract** - Pull raw taxi data from source
2. **Transform** - Clean and process the data
3. **Analyze** - Perform statistical analysis on the data
4. **Report** - Generate visualizations or reports

## Schedule
The DAG runs on a defined schedule (refer to the Python file for specific timing).

## Dependencies
- Apache Airflow
- Required Python libraries for data processing
- Access to the taxi dataset

## Expected Outputs
- Processed taxi data
- Analysis results
- Possible visualizations or reports

## Execution
The DAG can be triggered manually from the Airflow UI or will run according to its schedule.

## Example Output

Below is a terminal session showing the output files generated by this DAG:

```bash
idap-data-pipelines-aus-refdatapipeline-skilltraining-py3.11wacheng@CANL-H5SQ034:~/Development/Repo_Github/refdatapipeline/idap_data_pipelines_aus-refdatapipeline-skilltraining/src/skill-training-local$ docker exec -it 4a8 /bin/bash
airflow@4a8311bd7427:/opt/airflow$ ls
airflow-worker.pid  airflow_init.sh  connections.yaml  dags           logs     requirements.txt  webserver_config.py
airflow.cfg         config           cred              entrypoint.sh  plugins  variables.json    working
airflow@4a8311bd7427:/opt/airflow$ cd /tmp
airflow@4a8311bd7427:/tmp$ ls
pymp-bgg__96i  taxi_analysis.db  taxi_processing.log  taxi_report.md  taxi_summary.json  yellow_taxi_data.parquet
airflow@4a8311bd7427:/tmp$ cat taxi_report.md 
# NYC Yellow Taxi Data Analysis Report
Generated on: 2025-04-29 08:00:59

## Dataset Overview
- Source: NYC Yellow Taxi Trip Data
- Format: Parquet
- Total Records: 3,475,226

## Key Statistics

### Vendor Distribution
```
{
  "2": 2719860,
  "1": 753671,
  "7": 1206,
  "6": 489
}
```

## Notes
This challenge demonstrates skills in:
- ETL pipeline design
- Data analysis techniques
- Airflow DAG construction
- Working with real-world datasets