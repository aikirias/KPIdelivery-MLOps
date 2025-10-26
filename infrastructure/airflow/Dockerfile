FROM apache/airflow:2.7.1

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential openjdk-17-jdk python3-tk && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
