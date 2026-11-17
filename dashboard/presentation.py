"""Data preparation and reusable visual components for the dashboard."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from dashboard.client import DriftResponse, ReadingResponse


def format_latency(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 1000:.0f} ms" if value < 1 else f"{value:.2f} s"


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
        st.info("No drift window has been computed yet.")
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
