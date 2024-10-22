#!/bin/bash

# Initialize the Airflow database
airflow db upgrade

# Start a simple HTTP server to satisfy Cloud Run's health check
if [[ -z "$PORT" ]]; then
  export PORT=8080
fi
nohup python -m http.server $PORT &

# Start the scheduler
exec airflow scheduler
echo "Executor schedule: $AIRFLOW__CORE__EXECUTOR"
echo "SQL Alchemy Conn: $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
