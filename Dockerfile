FROM apache/airflow:2.10.2-python3.8

ENV AIRFLOW__WEBSERVER__SECRET_KEY='2385fea04030a18a1f8ff5876ad8a327'

# Install any additional packages
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
