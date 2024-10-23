FROM apache/airflow:2.10.2-python3.8

# Switch to airflow user for installing Python packages
USER airflow

# Set the working directory
WORKDIR /opt/airflow

# Copy requirements.txt and ensure airflow user ownership
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Install any additional packages as airflow user
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir apache-airflow-providers-google

# Switch to root to copy entrypoint scripts and adjust permissions
USER root

# Copy dags and schemas directories to the Docker image
COPY dags /opt/airflow/dags
COPY schemas /opt/airflow/schemas

# Adjust ownership and permissions
RUN chown -R airflow: /opt/airflow/dags
RUN chown -R airflow: /opt/airflow/schemas

# Copy entrypoint script and set permissions
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN chown airflow: /entrypoint.sh

# Switch back to airflow user
USER airflow

# Set the default entrypoint
ENTRYPOINT ["/entrypoint.sh"]
