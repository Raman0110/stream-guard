"""Page configuration and visual styling for the Streamlit dashboard."""

import streamlit as st


def configure_page() -> None:
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
