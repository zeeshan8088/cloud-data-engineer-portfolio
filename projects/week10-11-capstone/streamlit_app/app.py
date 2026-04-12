"""
============================================================
RetailFlow - Streamlit Analytics Dashboard
============================================================
Multi-page dashboard with 4 views:
  1. Revenue Overview
  2. Customer Analytics
  3. Funnel Analysis
  4. Demand Forecast

Data source: Google BigQuery (retailflow_gold + retailflow_predictions)
Charts: Plotly
============================================================
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
import os

# ── Configuration ────────────────────────────────────────────
PROJECT_ID = os.environ.get("GCP_PROJECT", "intricate-ward-459513-e1")
LOCATION = "asia-south1"

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="RetailFlow Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* Dark-themed KPI cards */
div[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.3);
}

div[data-testid="stMetric"] label {
    color: #a0aec0 !important;
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #e2e8f0 !important;
    font-size: 1.8rem !important;
    font-weight: 700 !important;
}

/* Sidebar styling */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
}

section[data-testid="stSidebar"] .stRadio label {
    color: #e2e8f0 !important;
}

/* Header gradient text */
.main-header {
    background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-size: 2.2rem;
    font-weight: 700;
    margin-bottom: 0.5rem;
}

.sub-header {
    color: #718096;
    font-size: 1rem;
    margin-bottom: 2rem;
}

/* Plotly chart containers */
div[data-testid="stPlotlyChart"] {
    border-radius: 12px;
    overflow: hidden;
}

/* Table styling */
div[data-testid="stDataFrame"] {
    border-radius: 12px;
    overflow: hidden;
}
</style>
""", unsafe_allow_html=True)


# ── BigQuery Client ──────────────────────────────────────────
@st.cache_resource
def get_bq_client():
    """Create a cached BigQuery client."""
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    """Run a BigQuery query and return as DataFrame (cached 5 min)."""
    client = get_bq_client()
    return client.query(query).result().to_dataframe()


# ── Color Palette ────────────────────────────────────────────
COLORS = {
    "primary": "#667eea",
    "secondary": "#764ba2",
    "accent": "#f093fb",
    "success": "#48bb78",
    "warning": "#ed8936",
    "danger": "#fc8181",
    "info": "#63b3ed",
    "bg_dark": "#1a1a2e",
    "bg_card": "#16213e",
    "text": "#e2e8f0",
    "text_muted": "#a0aec0",
}

CATEGORY_COLORS = px.colors.qualitative.Set2
PLOTLY_TEMPLATE = "plotly_dark"


# ── Sidebar Navigation ──────────────────────────────────────
with st.sidebar:
    st.markdown("## RetailFlow")
    st.markdown("**Analytics Dashboard**")
    st.markdown("---")

    page = st.radio(
        "Navigate",
        [
            "Revenue Overview",
            "Customer Analytics",
            "Funnel Analysis",
            "Demand Forecast",
        ],
        index=0,
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown(
        "<small style='color:#718096;'>Powered by BigQuery + Vertex AI</small>",
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════
# PAGE 1: Revenue Overview
# ══════════════════════════════════════════════════════════════
def page_revenue_overview():
    st.markdown('<div class="main-header">Revenue Overview</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Daily revenue trends and key performance indicators</div>',
        unsafe_allow_html=True,
    )

    # ── KPI Queries ──────────────────────────────────────────
    kpi_query = f"""
    SELECT
        ROUND(SUM(total_revenue), 2) AS total_revenue,
        SUM(order_count) AS total_orders,
        ROUND(AVG(avg_order_value), 2) AS avg_order_value,
        SUM(units_sold) AS total_units_sold,
        COUNT(DISTINCT order_date) AS active_days
    FROM `{PROJECT_ID}.retailflow_gold.mart_sales_daily`
    """
    kpi_df = run_query(kpi_query)

    if kpi_df.empty:
        st.warning("No revenue data available.")
        return

    row = kpi_df.iloc[0]

    # ── KPI Cards ────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric(
            label="Total Revenue",
            value=f"Rs {row['total_revenue']:,.0f}",
        )
    with c2:
        st.metric(
            label="Total Orders",
            value=f"{row['total_orders']:,}",
        )
    with c3:
        st.metric(
            label="Avg Order Value",
            value=f"Rs {row['avg_order_value']:,.0f}",
        )
    with c4:
        st.metric(
            label="Total Units Sold",
            value=f"{row['total_units_sold']:,}",
        )

    st.markdown("")

    # ── Daily Revenue Line Chart ─────────────────────────────
    daily_query = f"""
    SELECT
        order_date,
        total_revenue,
        order_count,
        avg_order_value,
        top_category
    FROM `{PROJECT_ID}.retailflow_gold.mart_sales_daily`
    ORDER BY order_date
    """
    daily_df = run_query(daily_query)

    fig = px.line(
        daily_df,
        x="order_date",
        y="total_revenue",
        title="Daily Revenue Trend",
        labels={"order_date": "Date", "total_revenue": "Revenue (Rs)"},
        template=PLOTLY_TEMPLATE,
    )
    fig.update_traces(
        line=dict(color=COLORS["primary"], width=2.5),
        fill="tozeroy",
        fillcolor="rgba(102,126,234,0.15)",
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=COLORS["text"]),
        height=420,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
    )
    st.plotly_chart(fig, use_container_width=True, key="revenue_line_chart")

    # ── Daily Orders + AOV ───────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        fig2 = px.bar(
            daily_df,
            x="order_date",
            y="order_count",
            title="Daily Order Volume",
            labels={"order_date": "Date", "order_count": "Orders"},
            template=PLOTLY_TEMPLATE,
            color_discrete_sequence=[COLORS["info"]],
        )
        fig2.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color=COLORS["text"]),
            height=350,
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        )
        st.plotly_chart(fig2, use_container_width=True, key="orders_bar_chart")

    with col2:
        fig3 = px.line(
            daily_df,
            x="order_date",
            y="avg_order_value",
            title="Average Order Value Trend",
            labels={"order_date": "Date", "avg_order_value": "AOV (Rs)"},
            template=PLOTLY_TEMPLATE,
        )
        fig3.update_traces(line=dict(color=COLORS["warning"], width=2))
        fig3.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color=COLORS["text"]),
            height=350,
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        )
        st.plotly_chart(fig3, use_container_width=True, key="aov_line_chart")


# ══════════════════════════════════════════════════════════════
# PAGE 2: Customer Analytics
# ══════════════════════════════════════════════════════════════
def page_customer_analytics():
    st.markdown('<div class="main-header">Customer Analytics</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Lifetime value segmentation and top customer insights</div>',
        unsafe_allow_html=True,
    )

    # ── LTV Segment Distribution ─────────────────────────────
    segment_query = f"""
    SELECT
        ltv_segment,
        COUNT(*) AS customer_count,
        ROUND(AVG(total_spend), 2) AS avg_spend,
        ROUND(SUM(total_spend), 2) AS total_spend
    FROM `{PROJECT_ID}.retailflow_gold.mart_customer_ltv`
    GROUP BY ltv_segment
    ORDER BY total_spend DESC
    """
    seg_df = run_query(segment_query)

    # Segment KPIs
    total_customers = seg_df["customer_count"].sum()
    total_ltv_revenue = seg_df["total_spend"].sum()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total Customers", f"{total_customers:,}")
    with c2:
        st.metric("Total Customer Spend", f"Rs {total_ltv_revenue:,.0f}")
    with c3:
        high_val = seg_df[seg_df["ltv_segment"] == "High"]
        hv_count = int(high_val["customer_count"].values[0]) if not high_val.empty else 0
        st.metric("High-Value Customers", f"{hv_count}")

    st.markdown("")

    col1, col2 = st.columns(2)

    with col1:
        # Custom color mapping for segments
        segment_color_map = {
            "High": "#48bb78",
            "Medium": "#667eea",
            "Low": "#ed8936",
            "New": "#a0aec0",
        }
        fig = px.bar(
            seg_df,
            x="ltv_segment",
            y="customer_count",
            color="ltv_segment",
            color_discrete_map=segment_color_map,
            title="Customers by LTV Segment",
            labels={"ltv_segment": "LTV Segment", "customer_count": "Customers"},
            template=PLOTLY_TEMPLATE,
            text="customer_count",
        )
        fig.update_traces(textposition="outside", textfont_size=14)
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color=COLORS["text"]),
            height=400,
            showlegend=False,
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        )
        st.plotly_chart(fig, use_container_width=True, key="ltv_segment_bar")

    with col2:
        fig2 = px.pie(
            seg_df,
            names="ltv_segment",
            values="total_spend",
            title="Revenue Share by LTV Segment",
            color="ltv_segment",
            color_discrete_map=segment_color_map,
            template=PLOTLY_TEMPLATE,
            hole=0.45,
        )
        fig2.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color=COLORS["text"]),
            height=400,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        st.plotly_chart(fig2, use_container_width=True, key="ltv_revenue_pie")

    # ── Top 10 Customers by Spend ────────────────────────────
    st.markdown("### Top 10 Customers by Total Spend")
    top_query = f"""
    SELECT
        full_name,
        email,
        city,
        ltv_segment,
        total_spend,
        order_count,
        avg_order_value,
        churn_risk,
        favorite_category
    FROM `{PROJECT_ID}.retailflow_gold.mart_customer_ltv`
    ORDER BY total_spend DESC
    LIMIT 10
    """
    top_df = run_query(top_query)

    # Style the dataframe
    st.dataframe(
        top_df.style.format({
            "total_spend": "Rs {:,.0f}",
            "avg_order_value": "Rs {:,.0f}",
            "order_count": "{:,}",
        }),
        use_container_width=True,
        height=420,
    )


# ══════════════════════════════════════════════════════════════
# PAGE 3: Funnel Analysis
# ══════════════════════════════════════════════════════════════
def page_funnel_analysis():
    st.markdown('<div class="main-header">Funnel Analysis</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">Website conversion funnel from views to purchases</div>',
        unsafe_allow_html=True,
    )

    # ── Aggregated Funnel Data ───────────────────────────────
    funnel_query = f"""
    SELECT
        SUM(page_views) AS page_views,
        SUM(add_to_cart) AS add_to_cart,
        SUM(checkout_started) AS checkout_started,
        SUM(confirmed_orders) AS orders,
        ROUND(SAFE_DIVIDE(SUM(add_to_cart), SUM(page_views)) * 100, 2) AS views_to_cart_rate,
        ROUND(SAFE_DIVIDE(SUM(checkout_started), SUM(add_to_cart)) * 100, 2) AS cart_to_checkout_rate,
        ROUND(SAFE_DIVIDE(SUM(confirmed_orders), SUM(checkout_started)) * 100, 2) AS checkout_to_order_rate,
        ROUND(SAFE_DIVIDE(SUM(confirmed_orders), SUM(page_views)) * 100, 2) AS overall_conversion_rate
    FROM `{PROJECT_ID}.retailflow_gold.mart_funnel`
    """
    funnel_df = run_query(funnel_query)

    if funnel_df.empty:
        st.warning("No funnel data available.")
        return

    row = funnel_df.iloc[0]

    # ── KPI Cards ────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Page Views", f"{row['page_views']:,.0f}")
    with c2:
        st.metric("Add to Cart", f"{row['add_to_cart']:,.0f}")
    with c3:
        st.metric("Checkouts", f"{row['checkout_started']:,.0f}")
    with c4:
        st.metric("Orders", f"{row['orders']:,.0f}")

    st.markdown("")

    # ── Funnel Chart ─────────────────────────────────────────
    funnel_stages = ["Page Views", "Add to Cart", "Checkout", "Orders"]
    funnel_values = [
        int(row["page_views"]),
        int(row["add_to_cart"]),
        int(row["checkout_started"]),
        int(row["orders"]),
    ]

    fig = go.Figure(go.Funnel(
        y=funnel_stages,
        x=funnel_values,
        textinfo="value+percent initial",
        textfont=dict(size=14, family="Inter"),
        marker=dict(
            color=["#667eea", "#764ba2", "#f093fb", "#48bb78"],
            line=dict(width=1, color="rgba(255,255,255,0.2)"),
        ),
        connector=dict(line=dict(color="rgba(255,255,255,0.1)", width=1)),
    ))
    fig.update_layout(
        title="Conversion Funnel",
        template=PLOTLY_TEMPLATE,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=COLORS["text"]),
        height=450,
        margin=dict(l=20, r=20, t=50, b=20),
    )
    st.plotly_chart(fig, use_container_width=True, key="funnel_chart")

    # ── Conversion Rates ─────────────────────────────────────
    st.markdown("### Stage-to-Stage Conversion Rates")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Views -> Cart", f"{row['views_to_cart_rate']}%")
    with col2:
        st.metric("Cart -> Checkout", f"{row['cart_to_checkout_rate']}%")
    with col3:
        st.metric("Checkout -> Order", f"{row['checkout_to_order_rate']}%")
    with col4:
        st.metric("Overall Conversion", f"{row['overall_conversion_rate']}%")

    # ── Daily Funnel Trend ───────────────────────────────────
    st.markdown("### Daily Funnel Trends")
    daily_funnel_query = f"""
    SELECT
        event_date,
        page_views,
        add_to_cart,
        checkout_started,
        confirmed_orders
    FROM `{PROJECT_ID}.retailflow_gold.mart_funnel`
    ORDER BY event_date
    """
    daily_funnel_df = run_query(daily_funnel_query)

    fig2 = px.line(
        daily_funnel_df,
        x="event_date",
        y=["page_views", "add_to_cart", "checkout_started", "confirmed_orders"],
        title="Daily Funnel Stage Volumes",
        labels={"event_date": "Date", "value": "Count", "variable": "Stage"},
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=["#667eea", "#764ba2", "#f093fb", "#48bb78"],
    )
    fig2.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=COLORS["text"]),
        height=380,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig2, use_container_width=True, key="daily_funnel_trend")


# ══════════════════════════════════════════════════════════════
# PAGE 4: Demand Forecast
# ══════════════════════════════════════════════════════════════
def page_demand_forecast():
    st.markdown('<div class="main-header">Demand Forecast</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="sub-header">AI-powered demand predictions vs. current sales performance</div>',
        unsafe_allow_html=True,
    )

    # ── Fetch Predictions ────────────────────────────────────
    forecast_query = f"""
    SELECT
        product_id,
        product_name,
        category,
        current_units_sold,
        predicted_next_week_units_sold,
        prediction_confidence,
        prediction_date
    FROM `{PROJECT_ID}.retailflow_predictions.demand_forecast`
    ORDER BY predicted_next_week_units_sold DESC
    """
    forecast_df = run_query(forecast_query)

    if forecast_df.empty:
        st.warning("No forecast data available. Run generate_mock_predictions.py first.")
        return

    # ── KPI Cards ────────────────────────────────────────────
    avg_confidence = forecast_df["prediction_confidence"].mean()
    total_predicted = forecast_df["predicted_next_week_units_sold"].sum()
    total_current = forecast_df["current_units_sold"].sum()
    growth_pct = ((total_predicted - total_current) / total_current * 100) if total_current else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Products Forecasted", f"{len(forecast_df)}")
    with c2:
        st.metric("Avg Confidence", f"{avg_confidence:.1%}")
    with c3:
        st.metric("Predicted Total Units", f"{total_predicted:,}")
    with c4:
        st.metric(
            "Predicted Growth",
            f"{growth_pct:+.1f}%",
            delta=f"{total_predicted - total_current:+,} units",
        )

    st.markdown("")

    # ── Grouped Bar Chart: Current vs Predicted ──────────────
    # Reshape for plotly grouped bar
    bar_data = forecast_df[["product_name", "category", "current_units_sold", "predicted_next_week_units_sold"]].copy()
    bar_data = bar_data.sort_values("predicted_next_week_units_sold", ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=bar_data["product_name"],
        x=bar_data["current_units_sold"],
        name="Current Units Sold",
        orientation="h",
        marker_color="#667eea",
        text=bar_data["current_units_sold"],
        textposition="auto",
    ))
    fig.add_trace(go.Bar(
        y=bar_data["product_name"],
        x=bar_data["predicted_next_week_units_sold"],
        name="Predicted Next Week",
        orientation="h",
        marker_color="#48bb78",
        text=bar_data["predicted_next_week_units_sold"],
        textposition="auto",
    ))
    fig.update_layout(
        title="Current vs Predicted Units Sold",
        barmode="group",
        template=PLOTLY_TEMPLATE,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=COLORS["text"]),
        height=max(500, len(bar_data) * 40),
        margin=dict(l=200, r=20, t=50, b=20),
        xaxis=dict(title="Units", gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(title=""),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True, key="forecast_comparison_bar")

    # ── By Category ──────────────────────────────────────────
    st.markdown("### Forecast by Category")
    cat_df = forecast_df.groupby("category").agg(
        current=("current_units_sold", "sum"),
        predicted=("predicted_next_week_units_sold", "sum"),
        avg_confidence=("prediction_confidence", "mean"),
        products=("product_id", "count"),
    ).reset_index()
    cat_df["growth_pct"] = ((cat_df["predicted"] - cat_df["current"]) / cat_df["current"] * 100).round(1)

    fig2 = px.bar(
        cat_df,
        x="category",
        y=["current", "predicted"],
        barmode="group",
        title="Category-Level: Current vs Predicted Units",
        labels={"category": "Category", "value": "Units", "variable": "Metric"},
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=["#667eea", "#48bb78"],
    )
    fig2.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=COLORS["text"]),
        height=400,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig2, use_container_width=True, key="category_forecast_bar")

    # ── Confidence Heatmap ───────────────────────────────────
    st.markdown("### Prediction Confidence by Product")

    fig3 = px.bar(
        forecast_df.sort_values("prediction_confidence", ascending=True),
        x="prediction_confidence",
        y="product_name",
        orientation="h",
        color="prediction_confidence",
        color_continuous_scale=["#fc8181", "#ed8936", "#48bb78"],
        title="Model Confidence per Product",
        labels={"prediction_confidence": "Confidence", "product_name": "Product"},
        template=PLOTLY_TEMPLATE,
    )
    fig3.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=COLORS["text"]),
        height=max(400, len(forecast_df) * 30),
        margin=dict(l=200, r=20, t=50, b=20),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", tickformat=".0%"),
        coloraxis_colorbar=dict(title="Confidence", tickformat=".0%"),
    )
    st.plotly_chart(fig3, use_container_width=True, key="confidence_bar")

    # ── Raw Data Table ───────────────────────────────────────
    st.markdown("### Detailed Forecast Data")
    st.dataframe(
        forecast_df.style.format({
            "current_units_sold": "{:,}",
            "predicted_next_week_units_sold": "{:,}",
            "prediction_confidence": "{:.2%}",
        }),
        use_container_width=True,
        height=400,
    )


# ══════════════════════════════════════════════════════════════
# ROUTER
# ══════════════════════════════════════════════════════════════
if page == "Revenue Overview":
    page_revenue_overview()
elif page == "Customer Analytics":
    page_customer_analytics()
elif page == "Funnel Analysis":
    page_funnel_analysis()
elif page == "Demand Forecast":
    page_demand_forecast()
