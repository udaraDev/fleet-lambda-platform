FROM platform AS platform-runtime
FROM apache/airflow:2.10.5-python3.11
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
USER airflow
# Airflow is already pinned by the base image; only add project dependencies.
RUN pip install --timeout 120 --retries 10 --no-cache-dir 'pyarrow==19.0.1' 'psycopg2-binary==2.9.10' 's3fs==2025.2.0'
USER root
COPY --from=platform-runtime --chown=airflow:root /usr/local/lib/python3.11/site-packages/pyspark /home/airflow/.local/lib/python3.11/site-packages/pyspark
COPY --from=platform-runtime --chown=airflow:root /usr/local/lib/python3.11/site-packages/pyspark-3.5.6.dist-info /home/airflow/.local/lib/python3.11/site-packages/pyspark-3.5.6.dist-info
COPY --from=platform-runtime --chown=airflow:root /usr/local/lib/python3.11/site-packages/py4j /home/airflow/.local/lib/python3.11/site-packages/py4j
COPY --from=platform-runtime --chown=airflow:root /usr/local/lib/python3.11/site-packages/py4j-0.10.9.7.dist-info /home/airflow/.local/lib/python3.11/site-packages/py4j-0.10.9.7.dist-info
USER airflow
COPY --chown=airflow:root common /app/common
COPY --chown=airflow:root batch /app/batch
COPY --chown=airflow:root config /app/config
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
