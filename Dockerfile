FROM apache/airflow:2.8.1

USER root

# 1. Install OpenJDK 11 (Better compatibility for Spark 3.5)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Set JAVA_HOME to version 11
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN export JAVA_HOME

# 3. Copy the GCS Connector JAR into the image 
# This ensures Spark can "see" GCS (gs://)
COPY gcs-connector-hadoop3-latest.jar /opt/airflow/plugins/gcs-connector.jar

# 4. Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER airflow

# 5. Sync your Python dependencies (pyspark, polars, etc.)
COPY pyproject.toml uv.lock ./
RUN uv pip install --system -r pyproject.toml