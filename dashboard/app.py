"""Streamlit composition root for the live StreamGuard dashboard."""

import os
from datetime import UTC, datetime
from html import escape
from pathlib import Path
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


def load_api_key() -> str:
    direct = os.getenv("API_KEY", "").strip()
    key_file = os.getenv("API_KEY_FILE", "").strip()
    if key_file:
        return Path(key_file).read_text(encoding="utf-8").strip()
    return direct


configure_page()
api = StreamGuardAPI(API_URL, api_key=load_api_key())

st.markdown(
    """
    <div class="sg-page-head">
        <div class="sg-kicker">Live condition monitoring</div>
        <h1>Sensor operations</h1>
        <p>Telemetry, anomaly review and model health in one working view.</p>
    </div>
    """,
    unsafe_allow_html=True,
)
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

    updated_at = datetime.now(UTC).strftime("%H:%M:%S UTC")
    st.markdown(
        f"""
        <div class="sg-runtime">
            <div class="sg-runtime-left">
                <span class="sg-dot"></span>
                <span class="sg-runtime-label">Pipeline operational</span>
            </div>
            <div class="sg-runtime-meta">Updated {updated_at}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    columns = st.columns(4)
    columns[0].metric("Throughput", f"{metrics['throughput_per_sec']:.1f}/s")
    columns[1].metric("P95 end-to-end", format_latency(metrics["latency_p95_s"]))
    columns[2].metric("Anomalies · 60s", f"{metrics['anomalies_in_window']:,}")
    columns[3].metric("Dead letters", f"{metrics['dlq_total']:,}")

    st.markdown(
        f"""
        <div class="sg-context-line">
            <strong>{metrics['readings_total']:,}</strong> readings processed &nbsp;·&nbsp;
            median latency <strong>{format_latency(metrics['latency_p50_s'])}</strong> &nbsp;·&nbsp;
            model <strong>{escape(health['model_version'])}</strong> &nbsp;·&nbsp;
            60-second metrics window
        </div>
        """,
        unsafe_allow_html=True,
    )

    chart_column, alert_column = st.columns([2.2, 1])
    with chart_column:
        with st.container(border=True):
            scope_label = "All sensors" if selected_sensor == "All sensors" else selected_sensor
            st.markdown(
                f"""
                <div class="sg-panel-head">
                    <div>
                        <div class="sg-panel-title">Telemetry</div>
                        <div class="sg-panel-note">{scope_label} · latest available readings</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            chart_frame = frame
            if selected_sensor != "All sensors" and not frame.empty:
                chart_frame = cast(
                    pd.DataFrame,
                    frame.loc[frame["sensor_id"] == selected_sensor],
                )
            if chart_frame.empty:
                st.info("Waiting for sensor readings.")
            else:
                st.plotly_chart(
                    telemetry_figure(chart_frame),
                    use_container_width=True,
                    config={"displayModeBar": False, "scrollZoom": False},
                )

    with alert_column:
        with st.container(border=True):
            st.markdown(
                """
                <div class="sg-panel-head">
                    <div>
                        <div class="sg-panel-title">Anomaly review</div>
                        <div class="sg-panel-note">Most recent flagged readings in this view</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            alert_frame = cast(pd.DataFrame, frame.loc[frame["is_anomaly"]]).sort_values(
                by="ts", ascending=False
            )
            if alert_frame.empty:
                st.markdown(
                    """
                    <div class="sg-empty">
                        <div class="sg-empty-title">No anomalies in the current window</div>
                        <div class="sg-empty-copy">
                            New model flags will appear here automatically.
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
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
                st.dataframe(
                    display,
                    hide_index=True,
                    use_container_width=True,
                    height=510,
                    column_config={
                        "ts": st.column_config.TextColumn("Time", width="small"),
                        "sensor_id": st.column_config.TextColumn("Sensor", width="small"),
                        "anomaly_score": st.column_config.NumberColumn("Score", format="%.3f"),
                        "temperature": st.column_config.NumberColumn("Temp", format="%.1f °C"),
                        "humidity": st.column_config.NumberColumn("Humidity", format="%.1f%%"),
                        "vibration": st.column_config.NumberColumn("Vibration", format="%.2f"),
                    },
                )

    st.markdown("<br>", unsafe_allow_html=True)
    with st.container(border=True):
        st.markdown(
            """
            <div class="sg-panel-head">
                <div>
                    <div class="sg-panel-title">Model drift</div>
                    <div class="sg-panel-note">
                        Live feature distributions compared with training
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        render_drift(drift)


live_dashboard()
