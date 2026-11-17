"""Sidebar controls and on-demand scoring form."""

import requests
import streamlit as st

from dashboard.client import StreamGuardAPI


def render_sidebar(api: StreamGuardAPI) -> tuple[str, int]:
    with st.sidebar:
        st.header("Controls")
        st.caption("The dashboard refreshes live from the FastAPI service.")
        selected_sensor = st.selectbox(
            "Sensor", ["All sensors", *[f"sensor-{index}" for index in range(1, 6)]]
        )
        history_size = st.slider("Readings displayed", 100, 1000, 500, 100)
        st.divider()
        _render_prediction_form(api)
    return selected_sensor, history_size


def _render_prediction_form(api: StreamGuardAPI) -> None:
    st.subheader("Score a reading")
    with st.form("prediction"):
        sensor_id = st.text_input("Sensor ID", "sensor-1")
        temperature = st.number_input("Temperature (°C)", -50.0, 150.0, 25.0, 0.5)
        humidity = st.number_input("Humidity (%)", 0.0, 100.0, 50.0, 1.0)
        vibration = st.number_input("Vibration (mm/s)", 0.0, 100.0, 0.5, 0.1)
        submitted = st.form_submit_button("Run prediction", use_container_width=True)
    if not submitted:
        return

    try:
        result = api.predict(
            {
                "sensor_id": sensor_id,
                "temperature": temperature,
                "humidity": humidity,
                "vibration": vibration,
            }
        )
        if result["is_anomaly"]:
            st.error(f"Anomaly detected · score {result['anomaly_score']:.3f}")
        else:
            st.success(f"Normal reading · score {result['anomaly_score']:.3f}")
        st.caption(
            f"Threshold {result['threshold']:.3f} · model {result['model_version']} · "
            f"history used {result['history_readings_used']}"
        )
    except requests.RequestException as exc:
        st.error(f"Prediction failed: {exc}")
