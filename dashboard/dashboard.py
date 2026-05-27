import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from datetime import datetime

# ---------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------

st.set_page_config(
    page_title="YouTube Real-Time Analytics",
    page_icon="🚀",
    layout="wide"
)

# ---------------------------------------------------------
# AUTO REFRESH
# ---------------------------------------------------------

st_autorefresh(interval=30000, key="refresh")

# ---------------------------------------------------------
# CUSTOM CSS
# ---------------------------------------------------------

st.markdown("""
<style>

html, body, [class*="css"] {
    font-family: 'Segoe UI', sans-serif;
}

.main {
    background: linear-gradient(180deg, #050816 0%, #0b1120 100%);
    color: white;
}

.block-container {
    padding-top: 1rem;
    padding-bottom: 1rem;
    max-width: 98%;
}

section[data-testid="stSidebar"] {
    background: #0f172a;
    border-right: 1px solid #1e293b;
}

.metric-card {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.08);
    padding: 22px;
    border-radius: 18px;
    backdrop-filter: blur(10px);
    box-shadow: 0px 0px 20px rgba(0,0,0,0.2);
}

.metric-title {
    font-size: 15px;
    color: #94a3b8;
}

.metric-value {
    font-size: 34px;
    font-weight: 700;
    color: white;
}

.live-badge {
    padding: 8px 14px;
    border-radius: 12px;
    background: #16a34a;
    color: white;
    font-weight: bold;
    display: inline-block;
}

.chart-card {
    background: rgba(255,255,255,0.04);
    border-radius: 20px;
    padding: 18px;
    border: 1px solid rgba(255,255,255,0.08);
}

div[data-testid="metric-container"] {
    background-color: transparent;
    border: none;
}

footer {
    visibility: hidden;
}

header {
    visibility: hidden;
}

</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port="5432"
    )

conn = get_connection()

# ---------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------

query = """
SELECT *
FROM public.youtube_trending_videos
LIMIT 500
"""

df = pd.read_sql(query, conn)

if df.empty:
    st.warning("No streaming data available")
    st.stop()

# ---------------------------------------------------------
# CLEAN DATA
# ---------------------------------------------------------

numeric_cols = ["views", "likes", "comments"]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ---------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------

st.sidebar.title("🎛 Dashboard Controls")

channels = st.sidebar.multiselect(
    "Filter Channels",
    options=df["channel_title"].unique(),
    default=df["channel_title"].unique()
)

df = df[df["channel_title"].isin(channels)]

top_n = st.sidebar.slider(
    "Top Videos Count",
    5,
    20,
    10
)

st.sidebar.markdown("---")

st.sidebar.success("✅ Real-Time Stream Active")

# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------

left, right = st.columns([5,1])

with left:
    st.title("🚀 YouTube Real-Time Analytics")

    st.markdown(
        "Streaming Analytics using Kafka • Spark • PostgreSQL • Airflow"
    )

with right:
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class='live-badge'>
        🔴 LIVE
        </div>
        """,
        unsafe_allow_html=True
    )

st.markdown("---")

# ---------------------------------------------------------
# KPI CARDS
# ---------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

def metric_card(title, value):

    st.markdown(
        f"""
        <div class='metric-card'>
            <div class='metric-title'>{title}</div>
            <div class='metric-value'>{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

with col1:
    metric_card("📹 Total Videos", f"{len(df):,}")

with col2:
    metric_card("👀 Total Views", f"{df['views'].sum():,}")

with col3:
    metric_card("👍 Total Likes", f"{df['likes'].sum():,}")

with col4:
    metric_card("💬 Total Comments", f"{df['comments'].sum():,}")

st.markdown("<br>", unsafe_allow_html=True)

# ---------------------------------------------------------
# TABS
# ---------------------------------------------------------

tab1, tab2, tab3 = st.tabs([
    "📊 Analytics",
    "🔥 Trending",
    "📋 Live Stream"
])

# ---------------------------------------------------------
# ANALYTICS TAB
# ---------------------------------------------------------

with tab1:

    c1, c2 = st.columns(2)

    with c1:

        st.markdown("<div class='chart-card'>", unsafe_allow_html=True)

        st.subheader("🔥 Top Trending Videos")

        top_videos = (
            df.sort_values(by="views", ascending=False)
            .head(top_n)
        )

        fig = px.bar(
            top_videos,
            x="views",
            y="title",
            orientation="h",
            color="views",
            height=500
        )

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=20, b=10),
            yaxis_title="",
            xaxis_title="Views"
        )

        st.plotly_chart(fig, use_container_width=True)

        st.markdown("</div>", unsafe_allow_html=True)

    with c2:

        st.markdown("<div class='chart-card'>", unsafe_allow_html=True)

        st.subheader("📺 Top Channels")

        channel_df = (
            df.groupby("channel_title")["views"]
            .sum()
            .reset_index()
            .sort_values(by="views", ascending=False)
            .head(8)
        )

        fig2 = px.treemap(
            channel_df,
            path=["channel_title"],
            values="views",
            color="views",
            height=500
        )

        fig2.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)"
        )

        st.plotly_chart(fig2, use_container_width=True)

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    c3, c4 = st.columns(2)

    with c3:

        st.markdown("<div class='chart-card'>", unsafe_allow_html=True)

        st.subheader("👍 Likes vs 💬 Comments")

        fig3 = px.scatter(
            df,
            x="likes",
            y="comments",
            size="views",
            hover_name="title",
            color="views",
            height=450
        )

        fig3.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)"
        )

        st.plotly_chart(fig3, use_container_width=True)

        st.markdown("</div>", unsafe_allow_html=True)

    with c4:

        st.markdown("<div class='chart-card'>", unsafe_allow_html=True)

        st.subheader("📈 Views Distribution")

        fig4 = px.histogram(
            df,
            x="views",
            nbins=20,
            height=450
        )

        fig4.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)"
        )

        st.plotly_chart(fig4, use_container_width=True)

        st.markdown("</div>", unsafe_allow_html=True)

# ---------------------------------------------------------
# TRENDING TAB
# ---------------------------------------------------------

with tab2:

    trending_df = (
        df.sort_values(by="views", ascending=False)
        [["title", "channel_title", "views", "likes", "comments"]]
        .head(20)
    )

    st.subheader("🔥 Top Trending Videos")

    st.dataframe(
        trending_df,
        use_container_width=True,
        height=600
    )

# ---------------------------------------------------------
# LIVE STREAM TAB
# ---------------------------------------------------------

with tab3:

    st.subheader("📡 Live Kafka Stream Data")

    live_df = df[
        ["title", "channel_title", "views", "likes", "comments"]
    ]

    st.dataframe(
        live_df,
        use_container_width=True,
        height=650
    )

# ---------------------------------------------------------
# FOOTER
# ---------------------------------------------------------

st.markdown("---")

current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

st.caption(
    f"Last Refreshed: {current_time} | Real-Time Streaming Pipeline"
)