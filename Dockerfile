FROM python:3.11-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app HOME=/home/fleet
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java \
    && useradd --uid 50000 --gid 0 --create-home fleet \
    && mkdir -p /data && chown -R 50000:0 /home/fleet /data
ENV JAVA_HOME=/opt/java
WORKDIR /app
COPY requirements.txt .
RUN pip install --timeout 120 --retries 10 --no-cache-dir -r requirements.txt
COPY scripts/check_spark.py scripts/check_spark.py
USER 50000:0
# Resolve the Kafka connector at build time; normal launches need no Maven download.
RUN SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[1]' \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 scripts/check_spark.py
COPY . .
CMD ["python", "-m", "simulators.producer_stream"]
