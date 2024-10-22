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

COPY dags /opt/airflow/dags
RUN chown -R airflow: /opt/airflow/dags

# Copy entrypoint script and set permissions
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN chown airflow: /entrypoint.sh

# Copy scheduler entrypoint script and set permissions
COPY scheduler_entrypoint.sh /scheduler_entrypoint.sh
RUN chmod +x /scheduler_entrypoint.sh
RUN chown airflow: /scheduler_entrypoint.sh

# Switch back to airflow user
USER airflow

# Set the default entrypoint
ENTRYPOINT ["/entrypoint.sh"]
