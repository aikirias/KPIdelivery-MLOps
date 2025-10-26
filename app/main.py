import json
import os
from typing import Dict, Optional

import mlflow
import numpy as np
import pandas as pd
import streamlit as st
from mlflow import artifacts
from mlflow.tracking import MlflowClient


FEATURES = [
    "payments_last_60d",
    "payments_sum",
    "avg_discount",
    "mau_mp_1",
    "mau_ml_1",
    "mau_ml_2",
    "saldo_mes_actual",
    "saldo_mes_previo",
    "spent_ml",
    "frequency_ml",
    "has_investment",
]
MODEL_NAME = os.getenv("CHURN_MODEL_NAME", "churn-model")


@st.cache_resource(show_spinner=False)
def load_model():
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    production_version = None
    try:
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        for mv in versions:
            if mv.current_stage == "Production":
                production_version = mv
                break
    except Exception:
        production_version = None
    try:
        model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/Production")
        st.success("Modelo Production cargado desde MLflow.")
        return model, production_version
    except Exception as exc:
        st.warning(
            "No se pudo cargar el modelo Production. "
            "Revisá que exista una versión publicada en MLflow.\n\n"
            f"Detalle: {exc}"
        )
        return None, production_version


def predict(model, payload: Dict[str, float]) -> Dict[str, float]:
    df = pd.DataFrame([payload])
    if model is None:
        # fallback heurístico
        prob = min(0.95, max(0.05, (payload["payments_sum"] / 1000.0) + payload["has_investment"] * 0.1))
        return {"probability": prob, "explanation": "Heurística local (sin modelo Production)."}
    preds = model.predict(df)
    if isinstance(preds, np.ndarray) and preds.ndim > 1:
        prob = float(preds[0][1])
    else:
        prob = float(preds[0])
    return {"probability": prob, "explanation": "Inferencia usando modelo Production."}


def _render_shap_section(production_meta: Optional[object]) -> None:
    if production_meta is None:
        st.info("No hay un modelo en Production para mostrar SHAP.")
        return
    try:
        shap_png = artifacts.download_artifacts(
            run_id=production_meta.run_id,
            artifact_path="explainability/shap_summary.png",
        )
        mean_json = artifacts.download_artifacts(
            run_id=production_meta.run_id,
            artifact_path="explainability/mean_abs_shap.json",
        )
    except Exception as exc:
        st.warning(f"No se encontraron artefactos de explainability: {exc}")
        return

    st.image(shap_png, caption="SHAP summary plot", use_column_width=True)
    try:
        with open(mean_json) as handle:
            data = json.load(handle)
        imp_df = (
            pd.DataFrame(sorted(data.items(), key=lambda x: x[1], reverse=True), columns=["feature", "mean_abs_shap"])
            .reset_index(drop=True)
        )
        st.dataframe(imp_df, use_container_width=True)
    except Exception as exc:
        st.warning(f"No se pudo cargar la tabla de importancias: {exc}")


def main():
    st.set_page_config(page_title="Churn Predictor", page_icon="🤖", layout="centered")
    st.title("Churn Predictor - Mercado Pago (Demo)")
    st.write(
        "Ingresá las features del usuario y consultá la probabilidad de que vuelva a operar "
        "en los próximos 60 días. La app consulta el modelo registrado en MLflow."
    )

    with st.sidebar:
        st.header("Credenciales / Config")
        st.code(
            json.dumps(
                {
                    "MLFLOW_TRACKING_URI": os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
                    "MLFLOW_S3_ENDPOINT_URL": os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000"),
                },
                indent=2,
            )
        )

    model, production_meta = load_model()

    col1, col2 = st.columns(2)

    inputs: Dict[str, float] = {}
    with col1:
        inputs["payments_last_60d"] = st.number_input("Transacciones últimos 60d", min_value=0, value=3)
        inputs["payments_sum"] = st.number_input("Monto gastado últimos 6m (USD)", min_value=0.0, value=250.0)
        inputs["avg_discount"] = st.slider("Descuento promedio", 0.0, 1.0, 0.05, 0.01)
        inputs["mau_mp_1"] = st.number_input("Accesos MP último mes", min_value=0, value=5)
        inputs["mau_ml_1"] = st.number_input("Accesos ML último mes", min_value=0, value=4)
    with col2:
        inputs["mau_ml_2"] = st.number_input("Accesos ML penúltimo mes", min_value=0, value=3)
        inputs["saldo_mes_actual"] = st.number_input("Saldo máximo actual (USD)", min_value=0.0, value=500.0)
        inputs["saldo_mes_previo"] = st.number_input("Saldo máximo previo (USD)", min_value=0.0, value=450.0)
        inputs["spent_ml"] = st.number_input("Gasto ML últimos 6m (USD)", min_value=0.0, value=150.0)
        inputs["frequency_ml"] = st.slider("Frecuencia compras ML (días)", 0, 30, 10, 1)
        inputs["has_investment"] = st.selectbox("Estado inversión (1 si activa)", [0, 1], index=1)

    if st.button("Calcular probabilidad", type="primary"):
        result = predict(model, inputs)
        st.metric("Probabilidad estimada de retener al usuario", f"{result['probability']*100:.2f}%")
        st.info(result["explanation"])

    st.caption("Los parámetros se calibran en los DAGs de entrenamiento y monitoreo.")

    st.divider()
    st.subheader("Explainability (SHAP)")
    _render_shap_section(production_meta)


if __name__ == "__main__":
    main()
