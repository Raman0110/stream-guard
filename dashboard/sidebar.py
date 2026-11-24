"""Sidebar controls and on-demand scoring form."""

import requests
import streamlit as st

from dashboard.client import StreamGuardAPI


def render_sidebar(api: StreamGuardAPI) -> tuple[str, int]:
    with st.sidebar:
        st.markdown(
            """
            <div class="sg-brand">
                <div class="sg-mark">SG</div>
                <div>
                    <div class="sg-brand-name">StreamGuard</div>
                    <div class="sg-brand-meta">Sensor operations</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.subheader("View")
        selected_sensor = st.selectbox(
            "Sensor",
            [*[f"sensor-{index}" for index in range(1, 6)], "All sensors"],
            help="Choose one sensor for a focused timeline or compare all sensors.",
        )
        history_size = st.slider(
            "History window",
            100,
            1000,
            500,
            100,
            help="Number of recent readings requested from the API.",
        )
        st.caption(f"Showing up to {history_size:,} recent readings · auto-refresh enabled")
        st.divider()
        _render_prediction_form(api)
    return selected_sensor, history_size


def _render_prediction_form(api: StreamGuardAPI) -> None:
    st.subheader("Manual evaluation")
    st.caption("Check one reading against the active model and recent sensor history.")
    with st.form("prediction", border=False):
        sensor_id = st.text_input("Sensor ID", "sensor-1")
        temperature = st.number_input("Temperature (°C)", -50.0, 150.0, 25.0, 0.5)
        humidity = st.number_input("Humidity (%)", 0.0, 100.0, 50.0, 1.0)
        vibration = st.number_input("Vibration (mm/s)", 0.0, 100.0, 0.5, 0.1)
        submitted = st.form_submit_button("Evaluate reading", use_container_width=True)
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
            st.error(f"Anomalous pattern · score {result['anomaly_score']:.3f}")
        else:
            st.success(f"Within baseline · score {result['anomaly_score']:.3f}")
        st.caption(
            f"Threshold {result['threshold']:.3f} · model {result['model_version']} · "
            f"history used {result['history_readings_used']}"
        )
    except requests.RequestException as exc:
        st.error(f"Prediction failed: {exc}")
