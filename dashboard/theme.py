"""Page configuration and visual styling for the Streamlit dashboard."""

import streamlit as st


def configure_page() -> None:
    st.set_page_config(
        page_title="StreamGuard | Sensor Operations",
        page_icon="S",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(
        """
        <style>
        :root {
            --sg-bg: #0c1015;
            --sg-surface: #121820;
            --sg-surface-raised: #171e27;
            --sg-border: #27313c;
            --sg-border-soft: #202832;
            --sg-text: #edf1f4;
            --sg-muted: #8f9ba8;
            --sg-accent: #42b99f;
            --sg-danger: #e16b6b;
            --sg-warning: #d6a552;
        }

        html, body, [class*="css"] {
            font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont,
                "Segoe UI", sans-serif;
        }

        .stApp {background: var(--sg-bg);}
        .block-container {
            max-width: 1480px;
            padding-top: 2.25rem;
            padding-bottom: 3rem;
        }

        header[data-testid="stHeader"] {background: transparent;}
        footer {visibility: hidden;}

        section[data-testid="stSidebar"] {
            background: #0f141a;
            border-right: 1px solid var(--sg-border-soft);
        }
        section[data-testid="stSidebar"] [data-testid="stSidebarContent"] {
            padding-top: 1.25rem;
        }

        .sg-brand {
            display: flex;
            align-items: center;
            gap: 0.7rem;
            margin: 0 0 1.7rem;
        }
        .sg-mark {
            display: grid;
            place-items: center;
            width: 2rem;
            height: 2rem;
            border: 1px solid #397e70;
            border-radius: 6px;
            color: #8ad7c6;
            font-size: 0.73rem;
            font-weight: 750;
            letter-spacing: 0.05em;
        }
        .sg-brand-name {color: var(--sg-text); font-size: 0.96rem; font-weight: 650;}
        .sg-brand-meta {color: var(--sg-muted); font-size: 0.76rem; margin-top: 0.05rem;}

        .sg-page-head {margin-bottom: 1.35rem;}
        .sg-kicker {
            color: #79cbbb;
            font-size: 0.76rem;
            font-weight: 650;
            letter-spacing: 0.1em;
            margin-bottom: 0.55rem;
            text-transform: uppercase;
        }
        .sg-page-head h1 {
            color: var(--sg-text);
            font-size: clamp(2rem, 3vw, 2.75rem);
            font-weight: 620;
            letter-spacing: -0.035em;
            line-height: 1.05;
            margin: 0;
        }
        .sg-page-head p {
            color: var(--sg-muted);
            font-size: 1rem;
            margin: 0.7rem 0 0;
        }

        .sg-runtime {
            align-items: center;
            border-bottom: 1px solid var(--sg-border-soft);
            display: flex;
            justify-content: space-between;
            margin-bottom: 1.15rem;
            padding-bottom: 0.8rem;
        }
        .sg-runtime-left {align-items: center; display: flex; gap: 0.55rem;}
        .sg-dot {
            background: var(--sg-accent);
            border-radius: 999px;
            box-shadow: 0 0 0 3px rgba(66, 185, 159, 0.12);
            display: inline-block;
            height: 0.48rem;
            width: 0.48rem;
        }
        .sg-runtime-label {color: #cbd4dc; font-size: 0.84rem; font-weight: 560;}
        .sg-runtime-meta {color: var(--sg-muted); font-size: 0.79rem;}

        [data-testid="stMetric"] {
            background: var(--sg-surface);
            border: 1px solid var(--sg-border);
            border-radius: 8px;
            min-height: 108px;
            padding: 1rem 1.05rem;
        }
        [data-testid="stMetricLabel"] {
            color: var(--sg-muted);
            font-size: 0.8rem;
        }
        [data-testid="stMetricValue"] {
            color: var(--sg-text);
            font-size: 1.65rem;
            font-weight: 620;
            letter-spacing: -0.025em;
        }
        [data-testid="stMetricDelta"] {font-size: 0.76rem;}

        .sg-context-line {
            color: var(--sg-muted);
            font-size: 0.8rem;
            margin: 0.7rem 0 1.45rem;
        }
        .sg-context-line strong {color: #c8d0d8; font-weight: 560;}

        [data-testid="stVerticalBlockBorderWrapper"] {
            background: var(--sg-surface);
            border-color: var(--sg-border) !important;
            border-radius: 9px;
        }
        [data-testid="stVerticalBlockBorderWrapper"] > div {
            padding: 1.1rem 1.15rem 1rem;
        }

        .sg-panel-head {
            align-items: start;
            display: flex;
            justify-content: space-between;
            margin-bottom: 0.85rem;
        }
        .sg-panel-title {color: var(--sg-text); font-size: 1rem; font-weight: 610;}
        .sg-panel-note {color: var(--sg-muted); font-size: 0.78rem; margin-top: 0.2rem;}

        .sg-empty {
            align-items: center;
            border: 1px dashed #303b47;
            border-radius: 7px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            min-height: 390px;
            padding: 2rem;
            text-align: center;
        }
        .sg-empty-title {color: #d2d9df; font-size: 0.92rem; font-weight: 580;}
        .sg-empty-copy {color: var(--sg-muted); font-size: 0.8rem; margin-top: 0.35rem;}

        .sg-state {
            align-items: flex-start;
            display: flex;
            gap: 0.7rem;
            padding: 0.15rem 0 0.7rem;
        }
        .sg-state-mark {
            background: var(--sg-accent);
            border-radius: 2px;
            height: 2.25rem;
            margin-top: 0.15rem;
            width: 3px;
        }
        .sg-state.is-warning .sg-state-mark {background: var(--sg-warning);}
        .sg-state-title {color: var(--sg-text); font-size: 0.95rem; font-weight: 610;}
        .sg-state-copy {color: var(--sg-muted); font-size: 0.8rem; margin-top: 0.18rem;}

        div[data-testid="stDataFrame"] {
            border: 1px solid var(--sg-border-soft);
            border-radius: 6px;
        }
        div[data-testid="stForm"] {
            background: transparent;
            border: 0;
            padding: 0;
        }
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div,
        [data-testid="stNumberInputContainer"] {
            border-color: var(--sg-border) !important;
            border-radius: 6px !important;
        }
        .stButton > button, [data-testid="stFormSubmitButton"] > button {
            background: #277f6e;
            border: 1px solid #349a85;
            border-radius: 6px;
            color: white;
            font-weight: 600;
        }
        .stButton > button:hover, [data-testid="stFormSubmitButton"] > button:hover {
            background: #2d907c;
            border-color: #49ad98;
        }
        hr {border-color: var(--sg-border-soft) !important;}

        @media (max-width: 900px) {
            .block-container {padding-top: 1.35rem;}
            .sg-runtime {align-items: flex-start; flex-direction: column; gap: 0.45rem;}
            .sg-empty {min-height: 180px;}
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
