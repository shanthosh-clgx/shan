#!/bin/bash
# entrypoint.sh

export PYTHONPATH="/opt/airflow/:$PYTHONPATH"

# Import pre-defined Airflow variables and connections
echo "Importing Airflow variables"
airflow variables import ./variables.json
echo "Importing Airflow connections"
airflow connections import ./connections.yaml

# Execute the passed command
echo "Executing command: $@"
if [ "$1" = 'webserver' ]; then
    exec airflow webserver
elif [ "$1" = 'scheduler' ]; then
    exec airflow scheduler
elif [ "$1" = 'celery_worker' ]; then
    exec airflow celery worker
elif [ "$1" = 'triggerer' ]; then
    exec airflow triggerer  
elif [ "$1" = 'airflow_init' ]; then
    exec /opt/airflow/airflow_init.sh        
else
    echo "Command not recognized"
    exit 1
fi
