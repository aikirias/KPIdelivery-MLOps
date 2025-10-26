FROM python:3.10-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    mlflow==2.10.0 \
    boto3==1.34.30 \
    psycopg2-binary==2.9.9

WORKDIR /mlflow
EXPOSE 5000

ENTRYPOINT ["mlflow", "server"]
CMD ["--host", "0.0.0.0", "--port", "5000", "--backend-store-uri", "postgresql+psycopg2://airflow:airflow@postgres:5432/mlflow_db", "--default-artifact-root", "s3://mlflow-artifacts", "--serve-artifacts"]
