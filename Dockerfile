FROM apache/airflow:2.9.0-python3.12

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64



RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark delta-spark polars