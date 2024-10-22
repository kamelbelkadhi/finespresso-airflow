#!/bin/bash

# Initialize the Airflow database
airflow db upgrade

# Create an admin user if not exist
airflow users create \
    --username "$AIRFLOW_USERNAME" \
    --password "$AIRFLOW_PASSWORD" \
    --firstname "${AIRFLOW_FIRSTNAME:-Admin}" \
    --lastname "${AIRFLOW_LASTNAME:-User}" \
    --role "${AIRFLOW_ROLE:-Admin}" \
    --email "${AIRFLOW_EMAIL:-admin@example.com}" || true

# Start the scheduler in the background
airflow scheduler &

# Start the webserver in the foreground (this keeps the container running)
exec airflow webserver --port 8080
