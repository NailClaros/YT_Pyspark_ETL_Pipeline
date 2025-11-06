import streamlit as st
import boto3
import pandas as pd
import re
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os
load_dotenv()

def get_db_connection():
    """Connect to database."""
    return psycopg2.connect(os.getenv("DB_URL"))


# -------- Helper functions -------- #
def get_latest_week_folder(bucket: str, prefix: str) -> str:
    """Return the latest week_X folder path from S3."""
    s3 = boto3.client("s3",
        aws_access_key_id=os.getenv("AWS_READER_KEY"),
        aws_secret_access_key=os.getenv("AWS_READER_ACCESS"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    week_pattern = re.compile(r"week_(\d{4}_\d{2}_\d{2})")

    weeks = []
    for obj in response.get("Contents", []):
        match = week_pattern.search(obj["Key"])
        if match:
            weeks.append(match.group(0))
    if not weeks:
        raise Exception("No week folders found in S3.")
    latest_week = sorted(set(weeks))[-1]
    return f"s3://{bucket}/{prefix}/{latest_week}/"

@st.cache_data(ttl=3600)
def load_latest_data():
    """Read the latest Parquet folder from S3 (cached for 1 hour)."""
    bucket = "yt-pyspark"
    prefix = "pipeline/prod"
    latest_path = get_latest_week_folder(bucket, prefix)
    return pd.read_parquet(latest_path)

@st.cache_data(ttl=1800)
def get_top_videos_from_db(limit=10):
    """Fetch latest trending videos from your database."""
    query = """
    SELECT video_id, title, channel_title, thumbnail_link, category_id
    FROM yt_data.youtube_videos_p 
    ORDER BY recorded_at DESC
    LIMIT %s;
    """
    with get_db_connection() as conn:
        return pd.read_sql(query, conn, params=(limit,))

@st.cache_data(ttl=86400)
def get_category_mapping():
    """Return a dictionary mapping category_id -> category_name from Postgres."""
    query = "SELECT id, category_name FROM yt_data.categorical_data;"
    with get_db_connection() as conn:
        df = pd.read_sql(query, conn)
    # Ensure keys are strings to match Spark parquet schema
    return dict(zip(df["id"].astype(str), df["category_name"]))


# -------- Streamlit App -------- #

st.title("📊 YouTube Trending Dashboard (Live Hourly)")
st.caption("Auto-refreshes every hour from S3 Parquet data")



try:
    df = load_latest_data()
    st.success("✅ Data loaded successfully")
    # st.metric("Total Records", len(df))
    # st.dataframe(df.head(10))

    trending_df = load_latest_data()
    video_meta_df = get_top_videos_from_db()
    top_10_videos = trending_df.sort_values("recorded_at", ascending=False).head(10)


    sort_column = st.selectbox(
        "Sort Top 10 by:",
        options=["views", "engagement_rate"],
        index=0,  # default sort by views
        help="Choose how to rank trending videos"
    )


    col1, col2 = st.columns([3, 2])


    with col1:
        merged_df = pd.merge(video_meta_df, trending_df, on="video_id", how="left")

        merged_df = merged_df.sort_values(["recorded_at", sort_column], ascending=[False, False]).head(10)

        st.subheader("🔥 Top 10 Trending Videos Right Now")

        for idx, row in merged_df.iterrows():
            views = int(row["views"]) if not pd.isna(row["views"]) else 0
            engagement = row["engagement_rate"] if not pd.isna(row["engagement_rate"]) else 0.0

            # Highlight the top video
            if idx == merged_df.index[0]:
                border_style = "3px solid orange"
                img_width = 160
            else:
                border_style = "1px solid #ccc"
                img_width = 120

            st.markdown(
                f"""
                <div style='display:flex; align-items:center; margin-bottom:12px; border:{border_style}; border-radius:12px; padding:4px'>
                    <img src='{row["thumbnail_link"]}' width='{img_width}' style='border-radius:8px; margin-right:12px'>
                    <div>
                        <b>{row["title"]}</b><br>
                        <small>{row["channel_title"]}</small><br>
                        {views:,} views | Engagement: {engagement:.2f}%
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

    with col2:
        category_counts = (
            top_10_videos["category_name"]
            .value_counts()
            .reset_index()
            .rename(columns={"index": "category_name", "category_name": "video_count"})
        )


        st.subheader("📊 Top Categories (Top 10 Trending)")
        import random
        
        warm_colors = [
            "#FF6B6B",  # red
            "#FF8C42",  # orange
            "#FFD93D",  # yellow
            "#6BCB77",  # green
            "#4D96FF",  # blue
            "#FF4D6D",  # pink
            "#FFA07A",  # salmon
        ]

        for _, row in category_counts.iterrows():
            category = row["video_count"]
            count = row["count"]
            color = random.choice(warm_colors)
            st.markdown(f"<span style='color:{color}; font-size:20px; font-weight:bold'>{category} {count}</span>", unsafe_allow_html=True)






    # --- Layout Row 2: Engagement Timeline ---
    st.subheader("📊 Engagement Over Time")

    show_top10 = st.toggle("Show only top 10 videos", value=False)
    if show_top10:
        trending_df = trending_df[trending_df["video_id"].isin(video_meta_df["video_id"])]

    metric_to_plot = st.selectbox(
        "Select metric to plot:",
        ["views", "likes", "comments", "engagement_rate"],
        index=0
    )

    fig_line = px.line(
        trending_df.sort_values("recorded_at"),
        x="recorded_at",
        y=metric_to_plot,
        color="video_id",
        title=f"{metric_to_plot.capitalize()} over Time",
    )
    st.plotly_chart(fig_line, use_container_width=True)


except Exception as e:
    st.error(f"Failed to load data: {e}")
