FROM apache/airflow:2.10.5-python3.11
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --timeout 120 --retries 10 --no-cache-dir 'apache-airflow==2.10.5' 'pyarrow==19.0.1' 'psycopg2-binary==2.9.10' 'pyspark==3.5.6'
COPY --chown=airflow:root common /app/common
COPY --chown=airflow:root batch /app/batch
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
