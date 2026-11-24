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
        ("temperature", "Temperature", "°C", "#d9a35f"),
        ("humidity", "Humidity", "%", "#67a9cf"),
        ("vibration", "Vibration", "mm/s", "#65b891"),
    ]
    sensor_colors = ["#67a9cf", "#65b891", "#d9a35f", "#a78bca", "#cf7f7f"]
    sensors = sorted(frame["sensor_id"].unique().tolist())
    comparing_sensors = len(sensors) > 1
    figure = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.09,
        subplot_titles=[field[1] for field in fields],
    )

    for row, (field, _label, unit, field_color) in enumerate(fields, start=1):
        for sensor_index, sensor_id in enumerate(sensors):
            sensor_frame = frame.loc[frame["sensor_id"] == sensor_id]
            anomalies = sensor_frame.loc[sensor_frame["is_anomaly"]]
            line_color = (
                sensor_colors[sensor_index % len(sensor_colors)]
                if comparing_sensors
                else field_color
            )
            figure.add_trace(
                go.Scatter(
                    x=sensor_frame["ts"],
                    y=sensor_frame[field],
                    mode="lines",
                    name=sensor_id,
                    legendgroup=sensor_id,
                    line={"color": line_color, "width": 1.8},
                    hovertemplate=(
                        f"{sensor_id}<br>%{{y:.3f}} {unit}<br>%{{x|%H:%M:%S}}<extra></extra>"
                    ),
                    showlegend=comparing_sensors and row == 1,
                ),
                row=row,
                col=1,
            )
            figure.add_trace(
                go.Scatter(
                    x=anomalies["ts"],
                    y=anomalies[field],
                    mode="markers",
                    name="Anomaly",
                    legendgroup="anomaly",
                    marker={
                        "color": "#e16b6b",
                        "line": {"color": "#f4b0b0", "width": 1},
                        "size": 7,
                        "symbol": "circle",
                    },
                    hovertemplate=(
                        f"{sensor_id} anomaly<br>%{{y:.3f}} {unit}<br>"
                        "%{x|%H:%M:%S}<extra></extra>"
                    ),
                    showlegend=False,
                ),
                row=row,
                col=1,
            )
        figure.update_yaxes(title_text=unit, row=row, col=1, gridcolor="#252d36")

    figure.update_layout(
        height=570,
        margin={"l": 16, "r": 12, "t": 38, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#9aa6b2", "family": "Inter, Segoe UI, sans-serif", "size": 12},
        hovermode="x unified",
        hoverlabel={"bgcolor": "#171e27", "bordercolor": "#34414d", "font_color": "#edf1f4"},
        legend={
            "orientation": "h",
            "x": 0,
            "y": 1.1,
            "font": {"size": 11},
            "title": {"text": ""},
        },
        uirevision="|".join(sensors),
    )
    figure.update_annotations(font={"color": "#d7dde3", "size": 12}, x=0, xanchor="left")
    figure.update_xaxes(
        gridcolor="#252d36",
        linecolor="#303a45",
        showspikes=True,
        spikecolor="#687582",
        spikethickness=1,
        tickformat="%H:%M:%S",
    )
    return figure


def render_drift(drift: DriftResponse) -> None:
    status = drift["status"]
    if status == "no_data":
        st.markdown(
            """
            <div class="sg-state">
                <div class="sg-state-mark"></div>
                <div>
                    <div class="sg-state-title">Waiting for a drift window</div>
                    <div class="sg-state-copy">
                        The monitoring worker needs enough recent feature vectors before comparison.
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    features = pd.DataFrame(drift["features"])
    drifted_count = int(features["drift_detected"].sum()) if not features.empty else 0
    state_class = "sg-state is-warning" if status == "drift" else "sg-state"
    state_title = (
        "Distribution shift detected" if status == "drift" else "Training baseline is stable"
    )
    state_copy = (
        f"{drifted_count} of {len(features)} monitored features exceed the drift limits."
        if status == "drift"
        else f"All {len(features)} monitored features remain within the configured limits."
    )
    computed_at = drift["computed_at"]
    if computed_at:
        timestamp = pd.to_datetime(computed_at, utc=True).strftime("%Y-%m-%d %H:%M UTC")
        state_copy = f"{state_copy} Last computed {timestamp}."
    st.markdown(
        f"""
        <div class="{state_class}">
            <div class="sg-state-mark"></div>
            <div>
                <div class="sg-state-title">{state_title}</div>
                <div class="sg-state-copy">{state_copy}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not features.empty:
        features = features.copy()
        features["feature"] = features["feature"].str.replace("_", " ").str.title()
        features["status"] = features["drift_detected"].map({True: "Review", False: "Stable"})
        features = features.sort_values(["drift_detected", "psi"], ascending=[False, False])
        st.dataframe(
            features[["feature", "status", "ks_statistic", "ks_pvalue", "psi"]],
            hide_index=True,
            use_container_width=True,
            height=min(360, 38 * len(features) + 40),
            column_config={
                "feature": st.column_config.TextColumn("Feature", width="medium"),
                "status": st.column_config.TextColumn("Status", width="small"),
                "ks_statistic": st.column_config.NumberColumn("KS statistic", format="%.3f"),
                "ks_pvalue": st.column_config.NumberColumn("KS p-value", format="%.4f"),
                "psi": st.column_config.NumberColumn("PSI", format="%.3f"),
            },
        )
