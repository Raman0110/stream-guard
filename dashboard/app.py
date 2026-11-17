"""Streamlit composition root for the live StreamGuard dashboard."""

import os
from datetime import UTC, datetime
from typing import cast

import pandas as pd
import requests
import streamlit as st

from dashboard.client import StreamGuardAPI
from dashboard.presentation import (
    format_latency,
    readings_frame,
    render_drift,
    telemetry_figure,
)
from dashboard.sidebar import render_sidebar
from dashboard.theme import configure_page

API_URL = os.getenv("API_URL", "http://localhost:8000")
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "2"))

configure_page()
api = StreamGuardAPI(API_URL)

st.title("🛡️ StreamGuard Control Center")
st.caption("Real-time IoT telemetry · streaming validation · ML anomaly detection")
selected_sensor, history_size = render_sidebar(api)


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

    columns = st.columns(6)
    columns[0].metric("Throughput", f"{metrics['throughput_per_sec']:.1f}/s")
    columns[1].metric("Readings", f"{metrics['readings_total']:,}")
    columns[2].metric("Alerts (60s)", f"{metrics['anomalies_in_window']:,}")
    columns[3].metric("P50 latency", format_latency(metrics["latency_p50_s"]))
    columns[4].metric("P95 latency", format_latency(metrics["latency_p95_s"]))
    columns[5].metric("Dead letters", f"{metrics['dlq_total']:,}")

    st.caption(f"Model {health['model_version']} · live 60-second processing window")
    st.divider()

    chart_column, alert_column = st.columns([2.2, 1])
    with chart_column:
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

    with alert_column:
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
