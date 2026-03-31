FROM apache/airflow:2.9.3-python3.11

USER root

RUN mkdir -p /opt/airflow/data && \
    chown -R airflow /opt/airflow/data


# 1. Install OpenJDK 11 (Better compatibility for Spark 3.5)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Set JAVA_HOME to version 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64




# 4. Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# 5. Sync your Python dependencies (pyspark, polars, etc.)


USER airflow
RUN pip install pyspark polars google-cloud-storage google-cloud-bigquery requests gcsfs




