FROM apache/airflow:2.8.1-python3.10

USER root

# Install OpenJDK-17 (required for Spark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install PySpark and Providers
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY scripts /opt/airflow/scripts
USER root
RUN chmod +x /opt/airflow/scripts/entrypoint.sh
USER airflow
