"""Model training / evaluation helpers using Spark + MLflow."""
from __future__ import annotations

import json
import time
from typing import Any, Dict

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

from churn import config


def _build_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.master(config.SPARK_MASTER_URL)
        .appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    return builder.getOrCreate()


def train_with_spark(**context: Any) -> Dict[str, Any]:
    """Train a logistic regression model with Spark ML."""
    spark = _build_spark_session("churn_training")
    try:
        df = spark.read.parquet(str(config.FEATURES_PATH))
        df = df.fillna(0)
        feature_cols = config.FEATURE_COLUMNS
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=30)
        pipeline = Pipeline(stages=[assembler, lr])

        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)

        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        auc = evaluator.evaluate(predictions)

        mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
        mlflow.set_experiment("churn_retention")
        with mlflow.start_run(run_name=f"churn_{context['logical_date'].date()}") as run:
            mlflow.log_metric(config.METRIC_KEY, float(auc))
            mlflow.log_param("feature_columns", ",".join(feature_cols))
            mlflow.log_param("algorithm", "logistic_regression")
            mlflow.log_artifact(str(config.FEATURES_PATH), artifact_path="dataset")
            mlflow.spark.log_model(model, artifact_path="model")
            run_id = run.info.run_id

        payload = {"run_id": run_id, "metric": float(auc)}
        artifact_summary = config.ARTIFACT_DIR / "last_training.json"
        artifact_summary.write_text(json.dumps(payload, indent=2))
        return payload
    finally:
        spark.stop()


def evaluate_candidate(**context: Any) -> Dict[str, Any]:
    """Compare candidate run vs the latest Production model."""
    ti = context["ti"]
    training_result = ti.xcom_pull(task_ids="train_spark_model")
    if not training_result:
        raise ValueError("No training metadata available for evaluation.")
    run_id = training_result["run_id"]
    candidate_metric = training_result["metric"]

    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    client = MlflowClient()

    model_uri = f"runs:/{run_id}/model"
    registration = mlflow.register_model(model_uri, config.MODEL_NAME)

    # Wait for the model version to be ready
    status = registration.status
    version = registration.version
    while status == "PENDING_REGISTRATION":
        time.sleep(2)
        status = client.get_model_version(config.MODEL_NAME, version).status

    production_metric = None
    for mv in client.search_model_versions(f"name='{config.MODEL_NAME}'"):
        if mv.current_stage == "Production":
            prod_run = client.get_run(mv.run_id)
            production_metric = prod_run.data.metrics.get(config.METRIC_KEY)
            break

    promote = production_metric is None or candidate_metric >= production_metric
    evaluation = {
        "candidate_metric": candidate_metric,
        "production_metric": production_metric,
        "promote": promote,
        "candidate_version": version,
        "run_id": run_id,
    }
    return evaluation


def decide_next_step(**context: Any) -> str:
    """Branch between promoting or skipping the candidate model."""
    evaluation = context["ti"].xcom_pull(task_ids="evaluate_candidate")
    if evaluation and evaluation.get("promote"):
        return "promote_model"
    return "skip_promotion"


def promote_model(**context: Any) -> None:
    """Transition the candidate to Production stage."""
    evaluation = context["ti"].xcom_pull(task_ids="evaluate_candidate")
    if not evaluation:
        raise ValueError("No evaluation result found.")
    client = MlflowClient()
    version = evaluation["candidate_version"]
    client.transition_model_version_stage(
        name=config.MODEL_NAME,
        version=version,
        stage="Production",
        archive_existing_versions=True,
    )
    client.set_registered_model_tag(config.MODEL_NAME, "last_promotion_run", evaluation["run_id"])


def skip_model(**context: Any) -> None:
    evaluation = context["ti"].xcom_pull(task_ids="evaluate_candidate")
    if evaluation:
        print(
            "Modelo candidato no supera al Production actual. "
            f"Candidato={evaluation['candidate_metric']:.4f} vs Prod={evaluation['production_metric']}"
        )
