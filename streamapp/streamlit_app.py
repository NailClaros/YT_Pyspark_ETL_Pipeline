import streamlit as st
import boto3
import pandas as pd
import re
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from streamlit_plotly_events import plotly_events
import os
load_dotenv()

st.set_page_config(layout="wide", page_title="YouTube Trending Dashboard", page_icon="📊")

if "selected_video" not in st.session_state:
        st.session_state.selected_video = None
if "last_data_hash" not in st.session_state:
    st.session_state.last_data_hash = None

def get_db_connection():
    """Connect to database."""
    if os.getenv("ENV", "prod") == "test":
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
    else:
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
    prefix = "pipeline/prod" if os.getenv("ENV", "prod") == "prod" else "pipeline/test"
    latest_path = get_latest_week_folder(bucket, prefix)
    return pd.read_parquet(latest_path)

@st.cache_data(ttl=1800)
def get_top_videos_from_db(limit=10):
    """Fetch latest trending videos from your database."""
    if os.getenv("ENV", "test") == "test":
        query = """
        SELECT video_id, title, channel_title, thumbnail_link, category_id, tags
        FROM aq_test_local.youtube_videos_p 
        ORDER BY recorded_at DESC
        LIMIT %s;
        """
    else:
        query = """
        SELECT video_id, title, channel_title, thumbnail_link, category_id, tags
        FROM yt_data.youtube_videos_p 
        ORDER BY recorded_at DESC
        LIMIT %s;
        """
    with get_db_connection() as conn:
        return pd.read_sql(query, conn, params=(limit,))

@st.cache_data(ttl=1800)
def get_top_channels_from_db(min_videos: int = 2):
    """
    Return channels with more than `min_videos` currently trending this week.
    Looks at the youtube_videos_p table and counts how many times
    each channel appears between this Monday and next Monday.
    """

    today = datetime.now().date()
    monday = today - timedelta(days=today.weekday())
    next_monday = monday + timedelta(days=7)

    if os.getenv("ENV", "prod") == "test":
        schema = "aq_test_local"
    else:
        schema = "yt_data"

    query = f"""
        SELECT channel_title, COUNT(*) AS video_count
        FROM {schema}.youtube_videos_p
        WHERE DATE(recorded_at) >= '{monday}'
          AND DATE(recorded_at) < '{next_monday}'
        GROUP BY channel_title
        HAVING COUNT(*) > {min_videos}
        ORDER BY video_count DESC;
    """

    with get_db_connection() as conn:
        return pd.read_sql(query, conn)
    

def parse_tags(tags_str: str) -> list:
    """
    Convert a raw tags string like:
    {bizarrap,biza,"bzrp music sessions",trap} 
    into a clean list of tags: ['bizarrap', 'biza', 'bzrp music sessions', 'trap']
    Handles both quoted and unquoted tags.
    """
    if not tags_str or tags_str == "{}":
        return []

    # Remove the surrounding braces
    tags_str = tags_str.strip("{}")

    # Regex to match quoted or unquoted tags
    pattern = r'"([^"]+)"|([^,]+)'

    tags = re.findall(pattern, tags_str)

    # Flatten regex tuples, strip whitespace, and remove empty strings
    clean_tags = [t[0] if t[0] else t[1] for t in tags]
    clean_tags = [t.strip() for t in clean_tags if t.strip()]
    return clean_tags

from collections import Counter
def get_top_tags(top_videos_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate tags from top videos and count frequency.
    Returns a dataframe with columns: 'tag', 'count'
    """
    all_tags = []
    for _, row in top_videos_df.iterrows():
        if "tags" in row and row["tags"]:
            tags = parse_tags(row["tags"])
            all_tags.extend(tags)
    
    tag_counts = Counter(all_tags)
    
    tag_counts = {tag: count for tag, count in tag_counts.items() if count > 1}
    
    if not tag_counts:
        return None
    
    return pd.DataFrame(tag_counts.items(), columns=["tag", "count"]).sort_values("count", ascending=False)

@st.cache_data()
def get_category_mapping():
    """Return a dictionary mapping category_id -> category_name from Postgres."""
    if os.getenv("ENV", "prod") == "test":
        query = 'SELECT id, category_name FROM aq_test_local.categorical_data;'
    else:
        query = 'SELECT id, category_name FROM yt_data.categorical_data;'

    with get_db_connection() as conn:
        df = pd.read_sql(query, conn)
    # Ensure keys are strings to match Spark parquet schema
    return dict(zip(df["id"].astype(str), df["category_name"]))


# -------- Streamlit App -------- #
st.title("📊 YouTube Trending Dashboard (Live Hourly)")
st.caption("Auto-refreshes every hour from S3 Parquet data")



try:
    metric_map = {
    "views": "Views",
    "likes": "Likes",
    "comment_count": "Comment Count",
    "engagement_rate": "Engagement Rate"
    }

    trending_df = load_latest_data() ## Load latest data from S3
    unique_videos = trending_df["video_id"].nunique()
    video_meta_df = get_top_videos_from_db(limit=None) ## gets video metadata from Postgres
    ## This gets all the videos in the videos table to merge with trending data to get the top 10

    long_data = pd.merge(trending_df, video_meta_df, on="video_id", how="left").drop(["category_id"], axis=1).sort_values(["recorded_at", "title"], ascending=False)

    last_10_df = trending_df.sort_values("recorded_at", ascending=False).drop_duplicates("video_id").head(10)

    st.success("✅ Data loaded successfully")
    col1, col2 = st.columns([.5,.5], gap="large")


    with col1:
        sort_display = st.selectbox(
        "Sort Top 10 by:",
        options=["Views", "Engagement Rate"],
        index=0,
        help="Choose how to rank trending videos"
        )

        sort_column_map = {
        "Views": "views",
        "Engagement Rate": "engagement_rate"
        }

        sort_column = sort_column_map[sort_display]
        # Here we merge video metadata with trending stats for the last 10 videos
        merged_df = pd.merge(video_meta_df, last_10_df, on="video_id", how="inner")
        merged_df = merged_df.drop(["category_id"], axis=1)
        merged_df = merged_df.sort_values(["recorded_at", sort_column], ascending=[False, False]).drop_duplicates().head(10)

        st.subheader("🔥 Top 10 Trending Videos Right Now")

        for rank, (_, row) in enumerate(merged_df.iterrows(), start=1):
            youtube_link = f"https://www.youtube.com/watch?v={row['video_id']}"
            views = int(row["views"]) if not pd.isna(row["views"]) else 0
            engagement = row["engagement_rate"] if not pd.isna(row["engagement_rate"]) else 0.0

            # Highlight top video visually
            if rank == 1:
                border_style = "2px solid orange"
                img_width = 120
                badge_color = "#FF8C42"  # orange for top video
            else:
                border_style = "1px solid #333"
                img_width = 90
                badge_color = "#6BCB77" if rank <= 3 else "#4D96FF"  # top 3 green, rest blue

            st.markdown(
                f"""
                <div style='
                    display:flex;
                    align-items:center;
                    margin-bottom:10px;
                    border:{border_style};
                    border-radius:10px;
                    padding:3px 10px;
                    background-color:rgba(255,255,255,0.05);
                    box-shadow:0 1px 3px rgba(0,0,0,0.08);
                '>
                    <div style='
                        width:30px; height:20px;
                        border-radius:50%;
                        background-color:{badge_color};
                        color:white;
                        display:flex;
                        align-items:center;
                        justify-content:center;
                        font-weight:bold;
                        margin-right:10px;
                        flex-shrink:0;
                    '>
                        {rank}
                    </div>
                    <a href="{youtube_link}" target="_blank">
                        <img src='{row["thumbnail_link"]}' width='{img_width}' style='border-radius:6px; margin-right:10px'>
                    </a>
                    <div style='flex:1'>
                        <a href="{youtube_link}" target="_blank" style='text-decoration:none; color:white; font-weight:bold'>
                            {row["title"]}
                        </a><br>
                        <small style='color:#cccccc'>{row["channel_title"]}</small><br>
                        <span style='font-size:13px; color:#cccccc'>
                            {views:,} views | Engagement: {engagement:.2f}%
                        </span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.subheader("🔥This weeks longest trending video and category insights")
        trending_counts = trending_df.groupby("video_id")["recorded_at"].nunique()

        # Find the maximum number of trending days
        max_trending_h = trending_counts.max()

        # Filter videos that have the maximum trending days
        top_videos = trending_counts[trending_counts == max_trending_h]

        if top_videos.empty:
            st.info("No trending videos found for this week.")
        else:
            top_videos_list = list(top_videos.items())

            # Display first 3 videos directly
            for video_id, days in top_videos_list[:3]:
                video_info = merged_df[merged_df["video_id"] == video_id].iloc[0]
                title = video_info["title"]
                channel = video_info["channel_title"]
                category = video_info.get("category_name", "Unknown")
                thumbnail = video_info.get("thumbnail_link", "")
                youtube_link = f"https://www.youtube.com/watch?v={video_id}"

                st.markdown(f"""
                <div style="
                    display:flex;
                    align-items:center;
                    margin-bottom:6px;
                    padding:4px 6px;
                    border-radius:6px;
                    background-color: rgba(255,255,255,0.05);
                ">
                    <a href="{youtube_link}" target="_blank">
                        <img src="{thumbnail}" width="60" style="border-radius:4px; margin-right:6px;">
                    </a>
                    <div style="flex:1; line-height:1.2;">
                        <a href="{youtube_link}" target="_blank" style="text-decoration:none; color:white; font-weight:bold; font-size:12px;">
                            {title}
                        </a><br>
                        <small style="color:#cccccc; font-size:10px;">{channel}</small><br>
                        <span style="color:#888; font-size:10px;">Category: {category} | {days} hours trending</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
            # If more than 3 videos, put the rest in an expander
            over_3 = len(top_videos_list) - 3
            if len(top_videos_list) > 3:
                with st.expander(f"Show {over_3} more {'video' if over_3 == 1 else 'videos'}"):
                    for video_id, days in top_videos_list[3:]:
                        video_info = merged_df[merged_df["video_id"] == video_id].iloc[0]
                        title = video_info["title"]
                        channel = video_info["channel_title"]
                        category = video_info.get("category_name", "Unknown")
                        thumbnail = video_info.get("thumbnail_link", "")
                        youtube_link = f"https://www.youtube.com/watch?v={video_id}"

                        st.markdown(f"""
                        <div style="
                            display:flex;
                            align-items:center;
                            margin-bottom:6px;
                            padding:4px 6px;
                            border-radius:6px;
                            background-color: rgba(255,255,255,0.05);
                        ">
                            <a href="{youtube_link}" target="_blank">
                                <img src="{thumbnail}" width="60" style="border-radius:4px; margin-right:6px;">
                            </a>
                            <div style="flex:1; line-height:1.2;">
                                <a href="{youtube_link}" target="_blank" style="text-decoration:none; color:white; font-weight:bold; font-size:12px;">
                                    {title}
                                </a><br>
                                <small style="color:#cccccc; font-size:10px;">{channel}</small><br>
                                <span style="color:#888; font-size:10px;">Category: {category} | {days} hours trending</span>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

    with col2:
        def get_rank_color(rank):
            if rank == 1:  # gold
                return "#FFD700"
            elif rank == 2:  # silver
                return "#C0C0C0"
            elif rank == 3:  # bronze
                return "#CD7F32"
            else:
                return "#4D96FF"  # blue for rest
        # --- Top Categories ---
        st.subheader("📊 Top Categories (Top 10 Trending)")

        category_counts = dict(
            merged_df["category_name"]
            .value_counts()
        )

        cat_counts = (
            pd.DataFrame(list(category_counts.items()), columns=["category", "count"])
            .sort_values("count", ascending=False)
        )

        cat_counts["rank"] = cat_counts["count"].rank(
            method="dense", ascending=False
        ).astype(int)    
      
        for _, row in cat_counts.iterrows():
            rank = int(row["rank"])
            category = row["category"]
            count = int(row["count"])
            color = get_rank_color(rank)

            col1, col2, col3 = st.columns([.5, 5, 1])
            with col1:
                st.markdown(f"<span style='color:{color}'>**#{rank}**</span>", unsafe_allow_html=True)
            with col2:
                st.markdown(f"<span style='color:{color}'>{category}</span>", unsafe_allow_html=True)
            with col3:
                st.markdown(f"**{count} {'videos' if count > 1 else 'video'}**", unsafe_allow_html=True)


        ## Current Top Video Lifespan Charts based on views or engagement
        st.subheader("🏆 Current Top Video lifespan over all metrics")
        st.markdown(f"<span style='margin-bottom:2;'>**Video: {merged_df.iloc[0]['title']}**</span>", unsafe_allow_html=True)
        st.markdown(f"🔥This video has been trending for {long_data[long_data['video_id'] == merged_df.iloc[0]['video_id']]['recorded_at'].nunique()} hours.")
        num_1_history = long_data[long_data["video_id"] == merged_df.iloc[0]["video_id"]]
        import plotly.graph_objects as go

        fig1 = go.Figure()

        # Views on primary y-axis
        fig1.add_trace(
            go.Scatter(
                x=num_1_history["recorded_at"],
                y=num_1_history["views"],
                mode="lines+markers",
                name="Views",
                yaxis="y1"
            )
        )

        # Likes on secondary y-axis
        fig1.add_trace(
            go.Scatter(
                x=num_1_history["recorded_at"],
                y=num_1_history["likes"],
                mode="lines+markers",
                name="Likes",
                yaxis="y2"
            )
        )

        # Layout for dual y-axis
        fig1.update_layout(
            title="Views vs Likes",
            xaxis_title="Date",
            yaxis=dict(
                title="Views",
                side="left",
                showgrid=True,
                zeroline=False
            ),
            yaxis2=dict(
                title="Likes",
                side="right",
                overlaying="y",
                showgrid=True,
                zeroline=False
            ),
            template="plotly_dark",
            hovermode="x unified",
            legend=dict(title="Metrics",
                       orientation="h",
                       yanchor="bottom",
                       y=1.02,
                       xanchor="right",
                       x=1),
            height=250,
            margin=dict(l=0, r=0, t=0, b=0)
        )

        

        # --- Chart 2: Comment Count vs Engagement Rate ---
        fig2 = go.Figure()

        # Comment count on primary y-axis
        fig2.add_trace(
            go.Scatter(
                x=num_1_history["recorded_at"],
                y=num_1_history["comment_count"],
                mode="lines+markers",
                name="Comment Count",
                yaxis="y1"
            )
        )

        # Engagement rate on secondary y-axis
        fig2.add_trace(
            go.Scatter(
                x=num_1_history["recorded_at"],
                y=num_1_history["engagement_rate"],
                mode="lines+markers",
                name="Engagement Rate",
                yaxis="y2"
            )
        )

        # Layout for dual y-axis
        fig2.update_layout(
            title="Comment Count vs Engagement Rate",
            xaxis_title="Date",
            yaxis=dict(
                title="Comment Count",
                side="left",
                showgrid=True,
                zeroline=False
            ),
            yaxis2=dict(
                title="Engagement Rate",
                side="right",
                overlaying="y",
                showgrid=True,
                zeroline=False
            ),
            template="plotly_dark",
            hovermode="x unified",
            legend=dict(title="Metrics",
                       orientation="h",
                       yanchor="bottom",
                       y=1.02,
                       xanchor="right",
                       x=1),
            height=250,
            margin=dict(l=50, r=50, t=0, b=30)
        )
        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)

        ## Top Channels Section
        st.subheader("🎥 Top Channels in Trending This Week")
        top_channels_df = get_top_channels_from_db(min_videos=1)

        if top_channels_df.empty:
            st.write("No channels with more than 2 trending videos this week...")
        else:
            for _, row in top_channels_df.iterrows():
                color = get_rank_color(_+1)
                st.markdown(
                    f"""
                    <span style='color:{color}; font-size:20px; font-weight:bold'>
                        <a href='https://www.youtube.com/@{row["channel_title"].replace(" ", "")}' 
                        target='_blank' style='color:{color}; text-decoration:none'>
                        #{_+1} {row['channel_title']}
                        </a>: {row['video_count']} videos
                    </span>
                    """,
                    unsafe_allow_html=True
                )   



        # Frequent Tags Section (aggregated from top 10)
        st.subheader("🏷️ Most Frequent Tags (within Top 10 Trending)")
        st.markdown("Tags used by multiple videos in the top 10 trending this week:")
        top_tags_df = get_top_tags(merged_df)
        if top_tags_df is None or top_tags_df.empty:
            st.info("No videos use common frequent tags this week.")
        else:
            top_tags_df = top_tags_df.sort_values("count", ascending=False).reset_index(drop=True)

            num_cols = 3
            cols = st.columns(num_cols)
            col_items = [top_tags_df.iloc[i::num_cols] for i in range(num_cols)]

            for i, col in enumerate(cols):
                for _, row in col_items[i].iterrows():
                    color = get_rank_color(row["count"])
                    col.markdown(
                        f"<span style='color:{color}; font-weight:bold; font-size:16px'>{row['tag']}: {row['count']}</span>",
                        unsafe_allow_html=True
                    )




        st.subheader(f"🏷️ Tags of the #1 video based on {sort_display}")
        st.markdown(f"**Video: {merged_df.iloc[0]['title']}**")
        top = merged_df.iloc[0]["tags"]
        if not top or top == "{}":
            st.info("No tags for this video.")
        else:
            tags_list = parse_tags(top)
            st.markdown(", ".join(tags_list))

    
    # --- Layout Row 2: Engagement Timeline ---
    st.subheader(f"📊 Engagement Over Time for {unique_videos} videos this week")
    
    

    metric_to_plot = st.selectbox(
        "Select metric to plot:",
        list(metric_map.keys()),
        format_func=lambda x: metric_map[x],
        index=0
    )

    show_top10 = st.toggle("Show only top 10 videos", value=False)
    if show_top10:
        long_data = long_data[long_data["video_id"].isin(last_10_df["video_id"])]

        # Clear selection if it's not part of filtered data
        if (
            st.session_state.selected_video
            and st.session_state.selected_video["title"] not in long_data["title"].values
        ):
            st.session_state.selected_video = None

    customdata_cols = ["title", "channel_title", "thumbnail_link", metric_to_plot]

    # Conditional layout
    if st.session_state.selected_video is None:
        # Full width
        fig_col = st.container()
        detail_col = None
    else:
        # Split layout
        fig_col, detail_col = st.columns([3.8, 1.2], gap="large")


    with fig_col:
        fig_line = px.line( long_data.sort_values("recorded_at"), 
                           x="recorded_at", 
                           y=metric_to_plot,
                            color="title", 
                            labels={ "recorded_at": "Recorded At",
                                     metric_to_plot: metric_map[metric_to_plot], 
                                     "title": "Video Title", 
                                     "channel_title": "Channel" }, 
                            title=f"{metric_map[metric_to_plot]} Over Time", 
                            hover_data=customdata_cols, 
                            markers=True, )

        config = {"displayModeBar": False, 
                  "scrollZoom": True}
        

        fig_line.update_traces(marker=dict(size=12),
                               hovertemplate =
                                "<b>%{customdata[0]}</b><br>" +
                                "<b>%{customdata[1]}</b><br>" +
                                "%{customdata[2]} <br>" +
                                f"<b>{metric_map[metric_to_plot]}: </b>" + "%{y}<br>" +
                                "<extra></extra>",)
        fig_line.update_layout(showlegend=show_top10,
                                legend=dict(title="Videos"),)
        
        fig_line.add_layout_image(
            dict(
                source='https://streamlit.io/images/brand/streamlit-mark-color.png',
                xref="paper", yref="paper",
                x=1.05, y=1,
                sizex=0.3, sizey=0.3,
                xanchor="left", yanchor="top"
            )
        )
        event = st.plotly_chart(fig_line, config=config, width="stretch", on_select="rerun")

    # Handle selection
    if event and "selection" in event and event["selection"]["points"]:
        selected_point = event["selection"]["points"][0]
        customdata = selected_point.get("customdata", [])
        if customdata:
            st.session_state.selected_video = {
                "title": customdata[0],
                "channel": customdata[1],
                "thumbnail": customdata[2],
                "metric": selected_point.get("y", 0),
            }

    # --- Keep previous selection if no new event ---
    elif (
        not event
        or not event.get("selection", {}).get("points")
    ) and st.session_state.selected_video:
        pass  


    if st.session_state.selected_video and detail_col:
        with detail_col:
            v = st.session_state.selected_video
            st.markdown("### 🎬 Video Details")
            st.image(v["thumbnail"], width="stretch")
            st.markdown(
                f"**{v['title']}**  \n"
                f"Channel: {v['channel']}  \n"
                f"{metric_map[metric_to_plot]}: {v['metric']:,}"
            )
            if st.button("Close"):
                st.session_state.selected_video = None
                st.rerun()


except Exception as e:
    st.error(f"Failed to load data: {e}")
