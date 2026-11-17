"""Streamlit dashboard for the live StreamGuard pipeline."""

import os
from datetime import UTC, datetime
from typing import cast

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from plotly.subplots import make_subplots

from dashboard.client import DriftResponse, ReadingResponse, StreamGuardAPI

API_URL = os.getenv("API_URL", "http://localhost:8000")
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "2"))

st.set_page_config(
    page_title="StreamGuard Control Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.5rem; padding-bottom: 2rem;}
    [data-testid="stMetric"] {
        background: linear-gradient(145deg, #111827, #172033);
        border: 1px solid #263349;
        border-radius: 12px;
        padding: 14px 16px;
    }
    [data-testid="stMetricValue"] {color: #5eead4;}
    .status-ok, .status-warn {
        display: inline-block; padding: 0.3rem 0.7rem; border-radius: 999px;
        font-weight: 700; font-size: 0.82rem; letter-spacing: 0.02em;
    }
    .status-ok {background: #123c35; color: #5eead4; border: 1px solid #216e61;}
    .status-warn {background: #451a1a; color: #fca5a5; border: 1px solid #7f1d1d;}
    </style>
    """,
    unsafe_allow_html=True,
)

api = StreamGuardAPI(API_URL)


def format_latency(value: float | None) -> str:
    if value is None:
        return "—"
    seconds = float(value)
    return f"{seconds * 1000:.0f} ms" if seconds < 1 else f"{seconds:.2f} s"


def readings_frame(rows: list[ReadingResponse]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    return frame.sort_values("ts")


def telemetry_figure(frame: pd.DataFrame) -> go.Figure:
    fields = [
        ("temperature", "Temperature", "°C", "#38bdf8"),
        ("humidity", "Humidity", "%", "#a78bfa"),
        ("vibration", "Vibration", "mm/s", "#34d399"),
    ]
    figure = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.08)
    anomalies = frame[frame["is_anomaly"]]
    for row, (field, label, unit, color) in enumerate(fields, start=1):
        figure.add_trace(
            go.Scatter(
                x=frame["ts"],
                y=frame[field],
                mode="lines",
                name=label,
                line={"color": color, "width": 2},
                hovertemplate=f"%{{y:.3f}} {unit}<extra></extra>",
            ),
            row=row,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=anomalies["ts"],
                y=anomalies[field],
                mode="markers",
                name=f"{label} alert",
                marker={"color": "#fb7185", "size": 9, "symbol": "diamond"},
                hovertemplate=f"Anomaly: %{{y:.3f}} {unit}<extra></extra>",
                showlegend=row == 1,
            ),
            row=row,
            col=1,
        )
        figure.update_yaxes(title_text=unit, row=row, col=1, gridcolor="#243047")
    figure.update_layout(
        height=590,
        margin={"l": 20, "r": 20, "t": 25, "b": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#cbd5e1"},
        hovermode="x unified",
        legend={"orientation": "h"},
    )
    figure.update_xaxes(gridcolor="#243047")
    return figure


def render_drift(drift: DriftResponse) -> None:
    status = drift["status"]
    if status == "no_data":
        st.info(
            "Drift monitoring is the next pipeline milestone; "
            "no drift window has been computed yet."
        )
        return
    badge = "status-warn" if status == "drift" else "status-ok"
    label = "DRIFT DETECTED" if status == "drift" else "BASELINE STABLE"
    st.markdown(f'<span class="{badge}">{label}</span>', unsafe_allow_html=True)
    features = pd.DataFrame(drift["features"])
    if not features.empty:
        st.dataframe(
            features[["feature", "ks_statistic", "ks_pvalue", "psi", "drift_detected"]],
            hide_index=True,
            use_container_width=True,
        )


st.title("🛡️ StreamGuard Control Center")
st.caption("Real-time IoT telemetry · streaming validation · ML anomaly detection")

with st.sidebar:
    st.header("Controls")
    st.caption("The dashboard refreshes live from the FastAPI service.")
    selected_sensor = st.selectbox("Sensor", ["All sensors", *[f"sensor-{i}" for i in range(1, 6)]])
    history_size = st.slider("Readings displayed", 100, 1000, 500, 100)
    st.divider()
    st.subheader("Score a reading")
    with st.form("prediction"):
        prediction_sensor = st.text_input("Sensor ID", "sensor-1")
        temperature = st.number_input("Temperature (°C)", -50.0, 150.0, 25.0, 0.5)
        humidity = st.number_input("Humidity (%)", 0.0, 100.0, 50.0, 1.0)
        vibration = st.number_input("Vibration (mm/s)", 0.0, 100.0, 0.5, 0.1)
        submitted = st.form_submit_button("Run prediction", use_container_width=True)
    if submitted:
        try:
            result = api.predict(
                {
                    "sensor_id": prediction_sensor,
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


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def live_dashboard() -> None:
    try:
        health = api.health()
        metrics = api.metrics()
        frame = readings_frame(api.readings(history_size))
        drift = api.drift()
    except requests.RequestException as exc:
        st.error(f"The StreamGuard API is unavailable at {API_URL}: {exc}")
        return

    header_left, header_right = st.columns([4, 1])
    with header_left:
        st.markdown('<span class="status-ok">● PIPELINE ONLINE</span>', unsafe_allow_html=True)
    with header_right:
        st.caption(f"Updated {datetime.now(UTC).strftime('%H:%M:%S')} UTC")

    cols = st.columns(6)
    cols[0].metric("Throughput", f"{metrics['throughput_per_sec']:.1f}/s")
    cols[1].metric("Readings", f"{metrics['readings_total']:,}")
    cols[2].metric("Alerts (60s)", f"{metrics['anomalies_in_window']:,}")
    cols[3].metric("P50 latency", format_latency(metrics["latency_p50_s"]))
    cols[4].metric("P95 latency", format_latency(metrics["latency_p95_s"]))
    cols[5].metric("Dead letters", f"{metrics['dlq_total']:,}")

    st.caption(f"Model {health['model_version']} · live 60-second processing window")
    st.divider()

    chart_col, alert_col = st.columns([2.2, 1])
    with chart_col:
        st.subheader("Live telemetry")
        chart_frame = frame
        if selected_sensor != "All sensors" and not frame.empty:
            chart_frame = cast(
                pd.DataFrame,
                frame.loc[frame["sensor_id"] == selected_sensor],
            )
        if chart_frame.empty:
            st.info("Waiting for sensor readings…")
        else:
            st.plotly_chart(telemetry_figure(chart_frame), use_container_width=True)

    with alert_col:
        st.subheader("Recent anomaly alerts")
        alert_frame = cast(pd.DataFrame, frame.loc[frame["is_anomaly"]]).sort_values(
            by="ts", ascending=False
        )
        if alert_frame.empty:
            st.success("No anomalies in the displayed readings.")
        else:
            display = (
                cast(
                    pd.DataFrame,
                    alert_frame.loc[
                        :,
                        [
                            "ts",
                            "sensor_id",
                            "anomaly_score",
                            "temperature",
                            "humidity",
                            "vibration",
                        ],
                    ],
                )
                .head(20)
                .copy()
            )
            timestamp_series = cast(pd.Series, display["ts"])
            display["ts"] = timestamp_series.dt.strftime("%H:%M:%S")
            st.dataframe(display, hide_index=True, use_container_width=True, height=530)

    st.subheader("Distribution drift")
    render_drift(drift)


live_dashboard()
