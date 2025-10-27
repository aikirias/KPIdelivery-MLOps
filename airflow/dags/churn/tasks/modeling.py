"""Model training / evaluation helpers using Spark + MLflow."""
from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict

import mlflow
import mlflow.spark
import numpy as np
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe
from mlflow.tracking import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

from churn import config
from churn.tasks import drift

HPO_SPACE = {
    "regParam": hp.loguniform("regParam", math.log(1e-4), math.log(1.0)),
    "elasticNetParam": hp.uniform("elasticNetParam", 0.0, 1.0),
    "maxIter": hp.quniform("maxIter", 30, 120, 10),
}


def _build_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.master(config.SPARK_MASTER_URL)
        .appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.s3.impl",
            "org.apache.hadoop.fs.s3a.S3A",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    if config.SPARK_JARS_PACKAGES:
        builder = builder.config("spark.jars.packages", config.SPARK_JARS_PACKAGES)
    return builder.getOrCreate()


def train_with_spark(**context: Any) -> Dict[str, Any]:
    """Train logistic regression with simple HPO, log SHAP artifacts to MLflow."""
    spark = _build_spark_session("churn_training")
    try:
        df = spark.read.parquet(str(config.FEATURES_PATH)).fillna(0)
        feature_cols = config.FEATURE_COLUMNS
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        evaluator = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
        )
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

        mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
        mlflow.set_experiment("churn_retention")

        best: Dict[str, Any] = {"metric": -1.0, "model": None, "run_id": None, "params": None}
        trial_counter = {"value": 0}

        def objective(param_sample: Dict[str, float]) -> Dict[str, Any]:
            trial_counter["value"] += 1
            max_iter = int(param_sample["maxIter"])
            reg_param = float(param_sample["regParam"])
            elastic_net = float(param_sample["elasticNetParam"])

            lr = LogisticRegression(
                featuresCol="features",
                labelCol="label",
                maxIter=max_iter,
                regParam=reg_param,
                elasticNetParam=elastic_net,
            )
            pipeline = Pipeline(stages=[assembler, lr])
            model = pipeline.fit(train_df)
            auc = evaluator.evaluate(model.transform(test_df))

            run_name = (
                f"churn_{context['logical_date'].date()}_hpo_{trial_counter['value']}"
            )
            with mlflow.start_run(run_name=run_name) as run:
                mlflow.log_params(
                    {
                        "regParam": reg_param,
                        "elasticNetParam": elastic_net,
                        "maxIter": max_iter,
                        "feature_columns": ",".join(feature_cols),
                        "algorithm": "spark_logistic_regression",
                        "hpo_algorithm": "tpe",
                    }
                )
                mlflow.log_metric(config.METRIC_KEY, float(auc))
                mlflow.spark.log_model(model, artifact_path="model")
                run_id = run.info.run_id

            if auc > best["metric"]:
                best.update(
                    {
                        "metric": float(auc),
                        "model": model,
                        "run_id": run_id,
                        "params": {
                            "regParam": reg_param,
                            "elasticNetParam": elastic_net,
                            "maxIter": max_iter,
                        },
                    }
                )

            return {"loss": 1 - auc, "status": STATUS_OK}

        trials = Trials()
        fmin(
            fn=objective,
            space=HPO_SPACE,
            algo=tpe.suggest,
            max_evals=config.HPO_MAX_EVALS,
            trials=trials,
            rstate=np.random.default_rng(config.HPO_SEED),
        )

        if not best["model"]:
            raise RuntimeError("No Spark model was trained successfully.")

        shap_path, mean_abs = _log_explainability(best["model"], best["run_id"])
        _log_dataset(best["run_id"])
        _log_mean_abs_shap(best["run_id"], mean_abs, shap_path)
        drift_stats = drift.calculate_recent_drift()
        if drift_stats:
            with mlflow.start_run(run_id=best["run_id"]):
                mlflow.log_metric("recent_drift_share", drift_stats["drift_share"])
                mlflow.log_metric(
                    "recent_drift_detected", int(drift_stats["drift_detected"])
                )

        payload = {"run_id": best["run_id"], "metric": best["metric"]}
        (config.ARTIFACT_DIR / "last_training.json").write_text(json.dumps(payload, indent=2))
        return payload
    finally:
        spark.stop()


def evaluate_candidate(**context: Any) -> Dict[str, Any]:
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
    margin = config.PROMOTION_MIN_DELTA
    promote = False
    if production_metric is None:
        promote = True
    elif candidate_metric - production_metric >= margin:
        promote = True
    return {
        "candidate_metric": candidate_metric,
        "production_metric": production_metric,
        "promotion_margin": margin,
        "promote": promote,
        "candidate_version": version,
        "run_id": run_id,
    }


def decide_next_step(**context: Any) -> str:
    evaluation = context["ti"].xcom_pull(task_ids="evaluate_candidate")
    if evaluation and evaluation.get("promote"):
        return "promote_model"
    return "skip_promotion"


def promote_model(**context: Any) -> None:
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
    client.set_registered_model_tag(
        config.MODEL_NAME, "last_promotion_run", evaluation["run_id"]
    )


def skip_model(**context: Any) -> None:
    evaluation = context["ti"].xcom_pull(task_ids="evaluate_candidate")
    if evaluation:
        print(
            "Modelo candidato no supera al Production actual con el margen configurado. "
            f"Candidato={evaluation['candidate_metric']:.4f} vs Prod={evaluation['production_metric']} "
            f"(mínimo requerido: +{evaluation['promotion_margin']:.4f})"
        )


def _log_dataset(run_id: str) -> None:
    with mlflow.start_run(run_id=run_id):
        mlflow.log_artifact(str(config.FEATURES_PATH), artifact_path="dataset")


def _log_mean_abs_shap(run_id: str, importance: Dict[str, float], shap_path: Path) -> None:
    mean_path = config.ARTIFACT_DIR / "mean_abs_shap.json"
    mean_path.write_text(json.dumps(importance, indent=2))
    with mlflow.start_run(run_id=run_id):
        mlflow.log_artifact(str(mean_path), artifact_path="explainability")
        mlflow.log_artifact(str(shap_path), artifact_path="explainability")


def _log_explainability(model, run_id: str) -> tuple[Path, Dict[str, float]]:
    import pandas as pd
    import shap
    from matplotlib import pyplot as plt
    from sklearn.linear_model import LogisticRegression as SklearnLogReg

    plt.switch_backend("Agg")

    pdf = pd.read_parquet(config.FEATURES_PATH)
    feature_df = pdf[config.FEATURE_COLUMNS].fillna(0)
    background = (
        feature_df.sample(n=min(200, len(feature_df)), random_state=42)
        if len(feature_df) > 0
        else feature_df
    )
    lr_stage = next(
        stage for stage in model.stages if isinstance(stage, LogisticRegressionModel)
    )
    sk_model = SklearnLogReg()
    sk_model.classes_ = np.array([0, 1])
    sk_model.coef_ = lr_stage.coefficients.toArray().reshape(1, -1)
    sk_model.intercept_ = np.array([lr_stage.intercept])
    sk_model.n_features_in_ = sk_model.coef_.shape[1]
    sk_model.feature_names_in_ = np.array(config.FEATURE_COLUMNS)

    explainer = shap.LinearExplainer(
        sk_model, background, feature_perturbation="interventional"
    )
    shap_values = explainer(background)
    shap_path = config.ARTIFACT_DIR / "shap_summary.png"
    plt.figure()
    shap.summary_plot(shap_values, background, show=False)
    plt.tight_layout()
    plt.savefig(shap_path, bbox_inches="tight")
    plt.close()

    values = shap_values.values
    if values.ndim == 3:
        values = values[:, 1, :]
    mean_abs = dict(
        zip(config.FEATURE_COLUMNS, np.abs(values).mean(axis=0).astype(float).tolist())
    )

    return shap_path, mean_abs
