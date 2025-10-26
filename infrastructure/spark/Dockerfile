FROM apache/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir boto3==1.34.30
ENV PYSPARK_PYTHON=python3
USER spark
