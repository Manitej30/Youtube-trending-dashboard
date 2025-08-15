# dashboard.py
import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime

# ----------------------------
# Postgres Connection Settings
# ----------------------------
POSTGRES_HOST = "postgres"  # Windows + Docker trick
POSTGRES_PORT = 5432
POSTGRES_DB = "airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"

# ----------------------------
# Function to get trending videos
# ----------------------------
def get_trending_videos(limit=20):
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT video_id, title, channel_title, published_at, views, likes, comments
            FROM youtube_trending_videos
            ORDER BY published_at DESC
            LIMIT {limit};
        """)
        rows = cursor.fetchall()
        columns = ["Video ID", "Title", "Channel", "Published At", "Views", "Likes", "Comments"]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error connecting to Postgres: {e}")
        return pd.DataFrame()

# ----------------------------
# Streamlit Layout
# ----------------------------
st.set_page_config(page_title="YouTube Trending Videos Dashboard", layout="wide")

st.title("📈 YouTube Trending Videos Dashboard")
st.write("Showing the latest trending videos from your Spark + Postgres pipeline.")

# Fetch data
df = get_trending_videos(limit=20)

# Show data
if not df.empty:
    df["Published At"] = pd.to_datetime(df["Published At"])
    st.dataframe(df, use_container_width=True)

    # Optional charts
    st.subheader("Views & Likes Distribution")
    st.bar_chart(df.set_index("Title")[["Views", "Likes"]])
else:
    st.info("No data available yet. Make sure your Spark streaming pipeline is running.")
