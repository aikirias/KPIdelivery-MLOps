FROM jupyter/scipy-notebook:2023-10-20

USER root
RUN pip install --no-cache-dir \
    mlflow==2.10.0 \
    boto3==1.34.30 \
    pyspark==3.5.0
USER ${NB_UID}

ENV JUPYTER_TOKEN=mlops \
    MLFLOW_TRACKING_URI=http://mlflow:5000 \
    MLFLOW_S3_ENDPOINT_URL=http://minio:9000

WORKDIR /home/jovyan/work
