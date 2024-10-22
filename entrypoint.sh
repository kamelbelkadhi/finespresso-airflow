#!/bin/bash

# Initialize the Airflow database
airflow db upgrade

# Create an admin user if not exist
airflow users create \
    --username admin \
    --password admin \
    --firstname ${AIRFLOW_FIRSTNAME:-Admin} \
    --lastname ${AIRFLOW_LASTNAME:-User} \
    --role ${AIRFLOW_ROLE:-Admin} \
    --email ${AIRFLOW_EMAIL:-admin@example.com} || true

# Start the webserver
exec airflow webserver
echo "Executor: $AIRFLOW__CORE__EXECUTOR"
echo "SQL Alchemy Conn: $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
