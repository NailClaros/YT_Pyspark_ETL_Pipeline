import pytest
import psycopg2
import boto3
import os
from dotenv import load_dotenv
load_dotenv()

@pytest.fixture(scope="session")
def db_conn():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )
    yield conn
    conn.close()

@pytest.fixture(scope="session")
def bad_db_conn():
    """A deliberately broken connection for negative testing."""
    try:
        conn = psycopg2.connect(
            host="localhost", 
            port="9999",      
            user="wrong_user",
            password="wrong_pass",
            dbname="does_not_exist"
        )
    except Exception:
        # Return a dummy object with .cursor() raising
        class DummyConn:
            def cursor(self, *args, **kwargs):
                raise psycopg2.OperationalError("Bad test DB connection")
            def close(self): pass
        return DummyConn()

    return conn

def prepare_youtube_tables(db_conn):
    """Create test YouTube tables in Postgres before tests and clean up after."""
    schema = os.getenv("POSTGRES_DB", "public")
    cur = db_conn.cursor()

    # Ensure schema exists
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema};')

    # Drop tables if they exist
    cur.execute(f'DROP TABLE IF EXISTS "{schema}".youtube_trending_history_p_test CASCADE;')
    cur.execute(f'DROP TABLE IF EXISTS "{schema}".youtube_videos_p_test CASCADE;')

    # Create youtube_videos_p_test table
    cur.execute(f"""
        CREATE TABLE "{schema}".youtube_videos_p_test (
            video_id       varchar(20) PRIMARY KEY,
            title          text NOT NULL,
            channel_title  text,
            category_id    integer,
            publish_date   timestamp,
            tags           text,
            thumbnail_link text,
            recorded_at    timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
            views          bigint,
            likes          bigint,
            comment_count  bigint
        );
    """)


    cur.execute(f"""
        CREATE TABLE "{schema}".youtube_trending_history_p_test (
            id            serial PRIMARY KEY,
            video_id      varchar(20) REFERENCES "{schema}".youtube_videos_p_test(video_id) ON DELETE CASCADE,
            publish_date  date NOT NULL,
            views         bigint,
            likes         bigint,
            comment_count bigint,
            recorded_at   timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT youtube_trending_history_p_test_pk UNIQUE (video_id, recorded_at)
        );
    """)

    db_conn.commit()
    cur.close()

    yield  # hand control to the test

    cur = db_conn.cursor()
    cur.execute(f'TRUNCATE TABLE "{schema}".youtube_trending_history_p_test CASCADE;')
    cur.execute(f'TRUNCATE TABLE "{schema}".youtube_videos_p_test CASCADE;')
    db_conn.commit()
    cur.close()

@pytest.fixture
def db_rows(db_conn):
    def _get_all():
        cur = db_conn.cursor()
        cur.execute(f'SELECT * FROM {os.getenv("POSTGRES_DB", "public")}.youtube_trending_history_p_test ORDER BY "recorded_at";')
        rows = cur.fetchall()
        cur.close()
        print(rows)
        return rows
    return _get_all


@pytest.fixture(scope="session")
def s3_test_good_client():
    return boto3.client(
        "s3",
        region_name="us-east-1",  
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_T"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY_T"),
    )

@pytest.fixture(scope="session")
def s3_test_bad_client():
    return boto3.client(
        "s3",
        region_name="us-east-1",  
        aws_access_key_id="bad_key",
        aws_secret_access_key="bad_secret",
    )

@pytest.fixture(scope="session")
def redis_test_client():
    from g_sheets import get_redis_client
    client = get_redis_client()
    yield client

    keys = client.keys("ptest:*")
    if keys:
        client.delete(*keys)

@pytest.fixture(scope="session")
def redis_bad_client():
    import redis
    """A Redis client guaranteed to fail for testing fallback logic."""
    client = redis.Redis(
        host="invalid-host",
        port=0,              
        username="wrong",
        password="wrong",
        db=0,
        decode_responses=True,
    )
    return client


@pytest.fixture(scope="function")
def get_cached_video_ids(redis_client):
    def _get_cached(prefix="ptest"):
        """Return all video_ids in Redis with the given prefix."""
        keys = redis_client.keys(f"{prefix}:*")
        return {k.split(":", 1)[1] for k in keys}
    return _get_cached


@pytest.fixture(autouse=True)
def delete_prefix_keys(redis_test_client, prefix="ptest"):
    """Delete all keys in Redis starting with the given prefix."""

    yield  # run the test

    keys = redis_test_client.keys(f"{prefix}:*")
    if keys:
        redis_test_client.delete(*keys)


import json
import gspread
from google.oauth2.service_account import Credentials
@pytest.fixture(scope="session")
def gsheet_client():
    import json
    import gspread
    from google.oauth2.service_account import Credentials

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.getenv("GOOGLE_SHEETS_CREDS")
    if not creds_json:
        raise Exception("Google Sheets credentials not found!")

    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)

    return client

@pytest.fixture
def read_sheet_rows(gsheet_client):
    def _read(sheet_name, sheet_id=os.getenv("SHEET_ID")):
        sheet = gsheet_client.open_by_key(sheet_id).worksheet(sheet_name)
        return sheet.get_all_records()
    return _read


@pytest.fixture(autouse=True)
def clear_sheets_after_test(gsheet_client):
    """Automatically clears specific sheets after every test."""
    sheet_id = os.getenv("SHEET_ID")
    client = gsheet_client.open_by_key(sheet_id)

    # Run the test first
    yield


    sheets_to_clear = ["tester-vids", "tester-snaps"]
    for sheet in sheets_to_clear:
        ws = client.worksheet(sheet)
        ws.clear()