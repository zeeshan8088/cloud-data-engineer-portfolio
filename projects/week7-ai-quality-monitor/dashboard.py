# dashboard.py
# ----------------------------------------------------------
# Purpose: Live Streamlit dashboard for the AI Quality
#          Monitor. Reads directly from BigQuery and
#          displays anomaly trends, breakdowns, and
#          Gemini explanations in a beautiful dark UI.
#
# Run with: streamlit run dashboard.py
# ----------------------------------------------------------

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── Page config ───────────────────────────────────────────
# Must be the first Streamlit call in the script
st.set_page_config(
    page_title="AI Data Quality Monitor",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ────────────────────────────────────────────
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background-color: #0e1117;
        color: #ffffff;
    }

    /* Metric cards */
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #1e2130, #252a3d);
        border: 1px solid #2d3250;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }

    div[data-testid="metric-container"] label {
        color: #8b92a5 !important;
        font-size: 13px !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.8px;
    }

    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-size: 2.2rem !important;
        font-weight: 700 !important;
    }

    /* Section headers */
    .section-header {
        font-size: 1.1rem;
        font-weight: 700;
        color: #8b92a5;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin: 24px 0 12px 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #2d3250;
    }

    /* Anomaly cards */
    .anomaly-card {
        background: linear-gradient(135deg, #1a1f2e, #1e2436);
        border: 1px solid #2d3250;
        border-left: 4px solid #ff4b6e;
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 12px;
        transition: all 0.2s ease;
    }

    .anomaly-card:hover {
        border-left-color: #7c83fd;
        box-shadow: 0 4px 20px rgba(124, 131, 253, 0.15);
    }

    .anomaly-type-badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 20px;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.5px;
        margin-bottom: 8px;
    }

    .badge-negative    { background: #ff4b6e22; color: #ff4b6e; border: 1px solid #ff4b6e44; }
    .badge-zero        { background: #ffa94d22; color: #ffa94d; border: 1px solid #ffa94d44; }
    .badge-future      { background: #7c83fd22; color: #7c83fd; border: 1px solid #7c83fd44; }
    .badge-missing     { background: #51cf6622; color: #51cf66; border: 1px solid #51cf6644; }
    .badge-duplicate   { background: #ff6b6b22; color: #ff6b6b; border: 1px solid #ff6b6b44; }

    .order-id {
        font-size: 1rem;
        font-weight: 700;
        color: #ffffff;
        margin-bottom: 4px;
    }

    .issue-text {
        font-size: 0.82rem;
        color: #8b92a5;
        margin-bottom: 8px;
        line-height: 1.5;
    }

    .gemini-label {
        font-size: 11px;
        font-weight: 700;
        color: #7c83fd;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        margin-bottom: 4px;
    }

    .gemini-text {
        font-size: 0.85rem;
        color: #c9d1d9;
        line-height: 1.6;
        font-style: italic;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: #131720 !important;
        border-right: 1px solid #2d3250;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Dataframe */
    .dataframe { background: #1e2130 !important; }

    /* Run ID pill */
    .run-pill {
        display: inline-block;
        background: #1e2436;
        border: 1px solid #2d3250;
        border-radius: 20px;
        padding: 4px 14px;
        font-size: 12px;
        color: #7c83fd;
        margin: 3px;
        font-family: monospace;
    }
</style>
""", unsafe_allow_html=True)

# ── BigQuery client ───────────────────────────────────────
PROJECT_ID = "intricate-ward-459513-e1"
DATASET    = "ecommerce_quality_monitor"
TABLE      = "anomaly_reports"

@st.cache_data(ttl=60)  # cache for 60 seconds — auto refreshes
def load_data() -> pd.DataFrame:
    """
    Load all anomaly data from BigQuery.
    Cached for 60 seconds — dashboard auto-refreshes
    without hammering BigQuery on every interaction.
    """
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            run_id,
            order_id,
            anomaly_type,
            description,
            gemini_explanation,
            detected_at
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        ORDER BY detected_at DESC
    """
    df = client.query(query).to_dataframe()
    df["detected_at"] = pd.to_datetime(df["detected_at"])
    return df


# ── Colour map ────────────────────────────────────────────
ANOMALY_COLORS = {
    "NEGATIVE_AMOUNT":      "#ff4b6e",
    "ZERO_AMOUNT_HIGH_QTY": "#ffa94d",
    "FUTURE_TIMESTAMP":     "#7c83fd",
    "MISSING_CUSTOMER_ID":  "#51cf66",
    "DUPLICATE_ORDER_ID":   "#ff6b6b",
}

BADGE_CLASS = {
    "NEGATIVE_AMOUNT":      "badge-negative",
    "ZERO_AMOUNT_HIGH_QTY": "badge-zero",
    "FUTURE_TIMESTAMP":     "badge-future",
    "MISSING_CUSTOMER_ID":  "badge-missing",
    "DUPLICATE_ORDER_ID":   "badge-duplicate",
}


# ── Sidebar ───────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔍 AI Quality Monitor")
    st.markdown("---")

    st.markdown("**Data Source**")
    st.markdown(f"`{PROJECT_ID}`")
    st.markdown(f"`{DATASET}.{TABLE}`")
    st.markdown("---")

    # Load data here so sidebar can use it
    with st.spinner("Loading from BigQuery..."):
        df = load_data()

    # Run selector
    run_ids = ["All Runs"] + sorted(df["run_id"].unique().tolist(), reverse=True)
    selected_run = st.selectbox("Filter by Pipeline Run", run_ids)

    st.markdown("---")

    # Anomaly type filter
    all_types = df["anomaly_type"].unique().tolist()
    selected_types = st.multiselect(
        "Filter by Anomaly Type",
        all_types,
        default=all_types
    )

    st.markdown("---")
    st.markdown(f"**Last refreshed**")
    st.markdown(f"`{datetime.now().strftime('%H:%M:%S')}`")

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ── Apply filters ─────────────────────────────────────────
filtered = df.copy()
if selected_run != "All Runs":
    filtered = filtered[filtered["run_id"] == selected_run]
if selected_types:
    filtered = filtered[filtered["anomaly_type"].isin(selected_types)]


# ── Header ────────────────────────────────────────────────
st.markdown("""
<div style='padding: 24px 0 8px 0;'>
    <h1 style='color: #ffffff; font-size: 2rem; font-weight: 800; margin: 0;'>
        🛒 E-commerce Data Quality Monitor
    </h1>
    <p style='color: #8b92a5; font-size: 1rem; margin: 6px 0 0 0;'>
        AI-powered anomaly detection — Gemini 2.5 explanations — Live BigQuery data
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("---")


# ── KPI Metrics ───────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Anomalies", len(filtered))
with col2:
    st.metric("Pipeline Runs", filtered["run_id"].nunique())
with col3:
    st.metric("Anomaly Types", filtered["anomaly_type"].nunique())
with col4:
    most_common = filtered["anomaly_type"].value_counts().idxmax() if len(filtered) > 0 else "—"
    st.metric("Most Common", most_common.replace("_", " ").title()[:15])
with col5:
    latest = filtered["detected_at"].max().strftime("%b %d %H:%M") if len(filtered) > 0 else "—"
    st.metric("Latest Run", latest)


st.markdown("<br>", unsafe_allow_html=True)


# ── Charts row ────────────────────────────────────────────
chart_col1, chart_col2 = st.columns([1, 1])

with chart_col1:
    st.markdown('<div class="section-header">Anomaly Breakdown</div>', unsafe_allow_html=True)

    type_counts = filtered["anomaly_type"].value_counts().reset_index()
    type_counts.columns = ["anomaly_type", "count"]

    fig_donut = go.Figure(data=[go.Pie(
        labels=type_counts["anomaly_type"],
        values=type_counts["count"],
        hole=0.6,
        marker=dict(
            colors=[ANOMALY_COLORS.get(t, "#7c83fd") for t in type_counts["anomaly_type"]],
            line=dict(color="#0e1117", width=3)
        ),
        textinfo="label+percent",
        textfont=dict(color="#ffffff", size=12),
        hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Share: %{percent}<extra></extra>"
    )])

    fig_donut.update_layout(
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        font=dict(color="#ffffff"),
        showlegend=False,
        margin=dict(t=20, b=20, l=20, r=20),
        height=320,
        annotations=[dict(
            text=f"<b>{len(filtered)}</b><br>total",
            x=0.5, y=0.5,
            font=dict(size=18, color="#ffffff"),
            showarrow=False
        )]
    )
    st.plotly_chart(fig_donut, use_container_width=True)

with chart_col2:
    st.markdown('<div class="section-header">Anomalies Per Run</div>', unsafe_allow_html=True)

    run_counts = filtered.groupby(
        [filtered["detected_at"].dt.strftime("%m/%d %H:%M"), "anomaly_type"]
    ).size().reset_index(name="count")
    run_counts.columns = ["run_time", "anomaly_type", "count"]

    fig_bar = px.bar(
        run_counts,
        x="run_time",
        y="count",
        color="anomaly_type",
        color_discrete_map=ANOMALY_COLORS,
        barmode="stack",
    )
    fig_bar.update_layout(
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        font=dict(color="#ffffff"),
        xaxis=dict(
            gridcolor="#2d3250",
            tickfont=dict(color="#8b92a5"),
            title=""
        ),
        yaxis=dict(
            gridcolor="#2d3250",
            tickfont=dict(color="#8b92a5"),
            title="Anomalies"
        ),
        legend=dict(
            font=dict(color="#8b92a5", size=11),
            bgcolor="#1e2130",
            bordercolor="#2d3250"
        ),
        margin=dict(t=20, b=20, l=20, r=20),
        height=320,
    )
    st.plotly_chart(fig_bar, use_container_width=True)


# ── Pipeline run IDs ──────────────────────────────────────
st.markdown('<div class="section-header">Pipeline Runs</div>', unsafe_allow_html=True)
run_html = "".join([f'<span class="run-pill">{r}</span>' for r in sorted(df["run_id"].unique(), reverse=True)])
st.markdown(run_html, unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)


# ── Anomaly cards ─────────────────────────────────────────
st.markdown('<div class="section-header">Anomaly Details — Gemini Analysis</div>', unsafe_allow_html=True)

if len(filtered) == 0:
    st.info("No anomalies match your current filters.")
else:
    # Two column card layout
    left_col, right_col = st.columns(2)

    for i, (_, row) in enumerate(filtered.iterrows()):
        badge_class = BADGE_CLASS.get(row["anomaly_type"], "badge-future")
        label = row["anomaly_type"].replace("_", " ")
        explanation = row["gemini_explanation"] or "No explanation available."
        # Trim explanation to 3 sentences max for cards
        sentences = explanation.split(". ")
        short_explanation = ". ".join(sentences[:2]) + ("." if len(sentences) > 1 else "")

        card_html = f"""
        <div class="anomaly-card">
            <span class="anomaly-type-badge {badge_class}">{label}</span>
            <div class="order-id">{row['order_id']}</div>
            <div class="issue-text">{row['description']}</div>
            <div class="gemini-label">🤖 Gemini Analysis</div>
            <div class="gemini-text">{short_explanation}</div>
        </div>
        """

        if i % 2 == 0:
            left_col.markdown(card_html, unsafe_allow_html=True)
        else:
            right_col.markdown(card_html, unsafe_allow_html=True)