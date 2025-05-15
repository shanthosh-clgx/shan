FROM apache/airflow:2.7.3-python3.10

WORKDIR /opt/airflow

ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/cred/clgx-gcp.json
ENV GCP_PROJECT_ID=clgx-refdatapipe-app-dev-f12e
ENV AWS_LOCAL_CREDENTIALS=/opt/airflow/cred/aws-dev.json

# First copy the requirements file
COPY skill-training-local/requirements.txt /opt/airflow/requirements.txt

# Then install dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/* && \
    chown -R airflow:users /opt/airflow/logs && \
    chmod -R 700 /opt/airflow/logs && \
    chmod -R 755 /opt/airflow/dags

RUN apt-get install -y ca-certificates --no-install-recommends && update-ca-certificates
# Switch to airflow user for pip installation
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# Copy other files after dependencies are installed
COPY --chown=airflow:root --chmod=755 skill-training-local/entrypoint.sh skill-training-local/airflow_init.sh skill-training-local/variables.json skill-training-local/connections.yaml /opt/airflow/

# Set entrypoint
ENTRYPOINT ["./entrypoint.sh"]