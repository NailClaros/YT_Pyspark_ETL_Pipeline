import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

def get_db_connection(env=os.getenv("ENV", "prod")):
    """
    Lazily establishes and returns a PostgreSQL connection.
    The connection is only made when this function is called.
    Use env="test" for local testing; defaults to production.
    - env: "prod" or "test" to determine connection type.
    """
    if env == "test":
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
    else:
        return psycopg2.connect(os.getenv("DB_URL"))


def add_video_O(video, conn=None, env=os.getenv("ENV", "prod"), schema="yt_data"):
    """
    Inserts a video into youtube_videos.
    Skips if video_id already exists.
    video - dict with keys matching table columns.
    conn - optional existing DB connection.
    env - "prod" or "test" to determine connection type.
    """
    try:
        close_conn = False
        if conn is None:
            conn = get_db_connection(env)
            close_conn = True
        if env == "test":
            schema = os.getenv("POSTGRES_DB")

        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {schema}.youtube_videos (
                    video_id, title, channel_title, category_id, publish_date,
                    tags, thumbnail_link, description, comments_disabled,
                    ratings_disabled, video_error_or_removed
                )
                VALUES (
                    %(video_id)s, %(title)s, %(channel_title)s, %(category_id)s,
                    %(publish_date)s, %(tags)s, %(thumbnail_link)s, %(description)s,
                    %(comments_disabled)s, %(ratings_disabled)s, %(video_error_or_removed)s
                )
                ON CONFLICT (video_id) DO NOTHING;
            """, video)

        conn.commit()

        if close_conn:
            conn.close()

        return 1 ## Indicate success
    
    except Exception as e:
        print("Error adding video:", e)
        return 0 ## Indicate failure

    finally:
        if close_conn and conn:
            conn.close()

def add_trending_snapshot_O(snapshot, conn=None, env=os.getenv("ENV", "prod"), schema="yt_data"):
    """
    Adds a daily trending snapshot.
    Skips if an identical record already exists.
    snapshot - dict with keys matching table columns.
    conn - optional existing DB connection.
    env - "prod" or "test" to determine connection type.
    """
    try:
        close_conn = False
        if conn is None:
            conn = get_db_connection(env)
            close_conn = True
        if env == "test":
            schema = os.getenv("POSTGRES_DB")

        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {schema}.youtube_trending_history (
                    video_id, publish_date, views, likes, dislikes,
                    comment_count, region, recorded_at
                )
                VALUES (
                    %(video_id)s, %(publish_date)s, %(views)s, %(likes)s,
                    %(dislikes)s, %(comment_count)s, %(region)s, %(recorded_at)s
                )
                ON CONFLICT DO NOTHING;
            """, snapshot)

        conn.commit()

        if close_conn:
            conn.close()

        return 1 ## Indicate success
    
    except Exception as e:
        print("Error adding trending snapshot:", e)
        return 0 ## Indicate failure
    
    finally:
        if close_conn and conn:
            conn.close()
    

##Pipeline version with partitioned tables below
#Use these functions instead of the above for partitioned tables

def add_video_P(videos, conn=None, env=os.getenv("ENV", "prod"), schema="yt_data"):
    """
    Inserts a video into youtube_videos.
    Skips if video_id already exists.
    video - dict with keys matching table columns.
    conn - optional existing DB connection.
    env - "prod" or "test" to determine connection type.
    """
    try:
        close_conn = False
        if conn is None:
            conn = get_db_connection(env)
            close_conn = True

        if env == "test":
            schema = os.getenv("POSTGRES_DB")

        with conn.cursor() as cur:
            for vid in videos:
                cur.execute(f"""
                    INSERT INTO {schema}.youtube_videos_p (
                        video_id, title, channel_title,
                        category_id, publish_date, tags, views, likes,
                        comment_count, thumbnail_link, recorded_at
                    )
                    VALUES (
                        %(video_id)s, %(title)s, %(channel_title)s,
                        %(category_id)s, %(publish_date)s, %(tags)s, %(views)s, %(likes)s,
                        %(comment_count)s, %(thumbnail_link)s, %(recorded_at)s
                    )
                    ON CONFLICT (video_id) DO NOTHING;
                """, vid)

        conn.commit()
        
        if conn.notices:
                for notice in conn.notices:
                    message = notice.strip().replace('\n', ' ')
                    print("DB NOTICE:", message)
                conn.notices.clear() 

        if close_conn:
            conn.close()

        return 1 ## Indicate success
    
    except Exception as e:
        print("Error adding video:", e)
        return 0 ## Indicate failure
    
    finally:
        if close_conn and conn:
            conn.close()
    
def add_trending_snapshot_P(snapshot, conn=None, env=os.getenv("ENV", "prod"), schema="yt_data"):
    """
    Adds a daily trending snapshot.
    Skips if an identical record already exists.
    snapshot - dict with keys matching table columns.
    conn - optional existing DB connection.
    env - "prod" or "test" to determine connection type.
    """
    try:
        close_conn = False
        if conn is None:
            conn = get_db_connection(env)
            close_conn = True

        if env == "test":
            schema = os.getenv("POSTGRES_DB")

        with conn.cursor() as cur:
            # Make sure snapshot is a list
            if isinstance(snapshot, dict):
                snapshot = [snapshot]

            for vid in snapshot:
                cur.execute(f"""
                    INSERT INTO {schema}.youtube_trending_history_p (
                        video_id, publish_date, views, likes,
                        comment_count, recorded_at
                    )
                    VALUES (
                        %(video_id)s, %(publish_date)s, %(views)s, %(likes)s,
                        %(comment_count)s, %(recorded_at)s
                    )
                    ON CONFLICT (video_id, recorded_at) DO NOTHING;
                """, vid)

        conn.commit()

        if conn.notices:
                for notice in conn.notices:
                    message = notice.strip().replace('\n', ' ')
                    print("DB NOTICE:", message)
                conn.notices.clear() 

        if close_conn:
            conn.close()

        return 1 ## Indicate success
    
    except Exception as e:
        print("Error adding trending snapshot:", e)
        return 0 ## Indicate failure
    
    finally:
        if close_conn and conn:
            conn.close()

        
def wipe_youtube_tables(conn=None, env=os.getenv("ENV", "prod"), schema="yt_data"):
    """
    Deletes all records from youtube_videos_p and youtube_trending_history_p tables.
    Use with caution — this wipes all data and is meant for TESTING ONLY.
    
    Parameters:
    - conn: optional existing DB connection
    - env: 'prod' or 'test' (adjusts schema if needed)
    - schema: DB schema to use
    """
    try:
        close_conn = False
        if conn is None:
            conn = get_db_connection(env)
            close_conn = True

        if env == "test":
            schema = os.getenv("POSTGRES_DB")

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {schema}.youtube_trending_history_p;")
            cur.execute(f"DELETE FROM {schema}.youtube_videos_p;")

        conn.commit()
        print("Wiped youtube_videos_p and youtube_trending_history_p tables.")

        if close_conn:
            conn.close()

        return 1  # Success

    except Exception as e:
        print("Error wiping tables:", e)
        return 0  # Failure

    finally:
        if close_conn and conn:
            conn.close()