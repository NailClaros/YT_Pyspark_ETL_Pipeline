import gspread
from google.oauth2.service_account import Credentials
import os
import json
import redis
from dotenv import load_dotenv
load_dotenv()

# ==== Google Sheets Setup ====
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds_json = os.getenv("GOOGLE_SHEETS_CREDS")
if not creds_json:
    raise Exception("Google Sheets credentials not found in environment variable!")

creds_dict = json.loads(creds_json)
creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
client = gspread.authorize(creds)

SHEET_ID = "19ZWtCdSaRtixWbSVgGPtsWWk-blKX3uSMt5n9sk7Jwc"

# Two tabs/sheets inside the same spreadsheet
VIDEOS_SHEET_NAME = "vids"
TRENDING_SHEET_NAME = "snapshots"

def get_redis_client(env: str = os.getenv("ENV", "prod")) -> redis.Redis:
    """
    Establishes and returns a Redis client based on the environment.
    - For 'test', local Redis on localhost or whatever you have set up
    - For 'prod', uses credentials from environment variables
    """
    try:
        if env.lower() == "test":
            print("Connecting to local Redis (test mode)...")
            client = redis.Redis(
                host="127.0.0.1",
                port=6379,
                db=0,
                decode_responses=True
            )
        else:
            print(f"Connecting to Redis (env={env})...")
            client = redis.Redis(
                host=os.getenv("REDIS_HOST"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                username=os.getenv("REDIS_USERNAME"),
                password=os.getenv("REDIS_PASSWORD"),
                ssl=bool(os.getenv("REDIS_SSL", "false").lower() == "true"),
                db=int(os.getenv("REDIS_DB", 0)),
                decode_responses=True
            )

        # Quick ping test for early failure detection
        client.ping()
        print("Redis connection successful")
        return client

    except Exception as e:
        print(f"Redis connection failed: {e}")
        return None

def clear_redis_cache(env: str = os.getenv("ENV", "prod")):
    """
    clears all keys in Redis for the specified environment.
    args:
    env: str : 'prod' or 'test' to determine which Redis to clear
    """
    try:
        redis_client = get_redis_client(env)  # or "test"
        for key in redis_client.scan_iter(match=f"{env.lower()}:*"):
            print(f"Deleting Redis key: {key}" )
            redis_client.delete(key)
    except Exception as e:
        print(f"Error clearing Redis cache: {e}")

def cache_video_ids_idempotent(videos, env=os.getenv('ENV', 'prod'), ttl_hours=24.0):
    """
    Idempotent version of cache_video_ids:
    - Only inserts new video IDs if not already cached.
    - Refreshes TTL for existing keys (sliding expiration).
    - Avoids redundant writes for unchanged data.

    Args:
        videos (list[dict]): List of video records containing 'video_id' keys.
        env (str): Environment identifier ('prod' or 'test').
        ttl_hours (float): TTL duration in hours.
    """
    redis_client = get_redis_client(env)
    if not redis_client:
        print("Redis not available — skipping caching.")
        return
    if not videos:
        print("No videos provided for caching.")
        return
    if ttl_hours <= 0:
        print("TTL hours must be positive.")
        return

    prefix = f"{env.lower()}:"
    ttl_seconds = int(ttl_hours * 3600)
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

    pipe = redis_client.pipeline(transaction=True)
    added, refreshed = 0, 0

    for video in videos:
        vid = video.get("video_id")
        if not vid:
            continue

        key = f"{prefix}{vid}"

        if redis_client.exists(key):  
            pipe.expire(key, ttl_seconds)  # reset TTL
            refreshed += 1
        else:
            pipe.hset(key, "video_id", vid)
            pipe.hset(key, "cached_at", now)
            pipe.hset(key, "env", env)
            pipe.hset(key, "title", video.get("title", ""))
            pipe.expire(key, ttl_seconds)
            added += 1

    try:
        pipe.execute()
        print(f"Added {added} new cached videos, refreshed {refreshed} existing ones.")
    except redis.exceptions.RedisError as e:
        print(f"Redis pipeline error: {e}")

def get_cached_video_ids(env="prod"):
    """
    Fetch all cached video IDs from Redis for a given environment.
    Returns a set of video IDs.
    """
    redis_client = get_redis_client(env)
    if not redis_client:
        print("Redis not available — returning empty set")
        return set()

    prefix = f"{env.lower()}:"
    keys = redis_client.keys(f"{prefix}*")
    video_ids = {k.split(":", 1)[1] for k in keys}  # strip prefix
    return video_ids


def _get_existing_keys(sheet_name, key_fields):
    """Collect existing keys from Google Sheet."""
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    records = sheet.get_all_records()  # list of dicts
    if not records:
        return set(), True

    existing = {tuple(str(row[k]) for k in key_fields) for row in records if all(k in row for k in key_fields)}
    needs_header = False
    return existing, needs_header

def get_existing_keys_cached(key_fields, sheet_name="",  env=os.getenv("ENV", "prod")):
    """
    Get existing keys from Redis first; fallback to Google Sheet if Redis unavailable.
    Returns:
      existing (set)        -> set of strings for single-key case, set of tuples for multi-key
      needs_header (bool)
    """
    redis_client = get_redis_client(env)
    prefix = f"{env.lower()}:"

    if redis_client:
        try:
            keys = redis_client.keys(f"{prefix}*") or []
            # keys should be strings if decode_responses=True in get_redis_client
            # strip prefix
            stripped = [k.split(":", 1)[1] for k in keys if ":" in k]

            # Single-key (video_id) -> return set of strings for easy comparison
            if len(key_fields) == 1 and key_fields[0] == "video_id":
                existing = set(stripped)
                needs_header = False
                return existing, needs_header

            # Multi-key fallback: we don't store multi-key combos in Redis by default
            # so return an empty set, forcing callers to use the sheet fallback below
            existing = set()
            needs_header = False
            return existing, needs_header

        except Exception as e:
            print(f"Redis unavailable for existing keys check ({e}) — falling back to Sheet.")

    # Fallback to Google Sheet (only if Redis not available or on multi-key)
    if not sheet_name:
        # no sheet provided and no redis — safe fallback
        return set(), True

    return _get_existing_keys(sheet_name, key_fields)

def _append_to_sheet(sheet_name, fieldnames, rows, needs_header):
    """
    Append rows to a Google Sheet.
    Efficiently checks only the first row to determine if a header is needed.
    """
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)

    try:
        # Only check the first row 
        first_row = sheet.row_values(1)
        header_missing = not first_row or len(first_row) == 0
    except Exception as e:
        print(f"Warning: Could not read first row for '{sheet_name}'. Assuming header missing. ({e})")
        header_missing = True

    if needs_header or header_missing:
        print(f"Adding header row to '{sheet_name}'...")
        sheet.append_row(fieldnames)

    # Convert dicts to lists matching the fieldnames
    cleaned_rows = []
    for row in rows:
        cleaned_row = [
            ", ".join(v) if isinstance(v, list)
            else str(v) if isinstance(v, (dict, tuple))
            else v
            for v in (row.get(f, "") for f in fieldnames)
        ]
        cleaned_rows.append(cleaned_row)

    # Batch append all rows at once
    if cleaned_rows:
        sheet.append_rows(cleaned_rows, value_input_option="USER_ENTERED")
        print(f"Added {len(cleaned_rows)} rows to Google Sheet '{sheet_name}'.")


# def update_videos_sheet(videos):
#     """Appends unique videos to Google Sheet (youtube_videos)."""
#     if not videos:
#         print("No videos to add.")
#         return

#     fieldnames = list(videos[0].keys())
#     existing_ids, needs_header = _get_existing_keys(VIDEOS_SHEET_NAME, ["video_id"])

#     new_videos = [v for v in videos if (v["video_id"],) not in existing_ids]
#     if not new_videos:
#         print("No new unique videos to add.")
#         return

#     _append_to_sheet(VIDEOS_SHEET_NAME, fieldnames, new_videos, needs_header)
#     print(f"Added {len(new_videos)} new videos to Google Sheet '{VIDEOS_SHEET_NAME}'.")

def update_videos_sheet(videos, env=os.getenv("ENV", "prod")):
    """Appends unique videos to Google Sheet (youtube_videos), using Redis if available."""
    if not videos:
        print("No videos to add.")
        return

    fieldnames = list(videos[0].keys())
    existing_ids, needs_header = get_existing_keys_cached(VIDEOS_SHEET_NAME, ["video_id"], env=env)

    new_videos = [v for v in videos if (v["video_id"],) not in existing_ids]
    if not new_videos:
        print("No new unique videos to add.")
        return

    _append_to_sheet(VIDEOS_SHEET_NAME, fieldnames, new_videos, needs_header)
    print(f"Added {len(new_videos)} new videos to Google Sheet '{VIDEOS_SHEET_NAME}'.")

# def update_trending_sheet(snapshots):
#     """Appends unique trending snapshots to Google Sheet (youtube_trending_history)."""
#     if not snapshots:
#         print("No snapshots to add.")
#         return

#     fieldnames = ["video_id", "publish_date", "views", "likes", "comment_count", "recorded_at"]
#     existing_pairs, needs_header = _get_existing_keys(TRENDING_SHEET_NAME, ["video_id", "recorded_at"])

#     new_rows = [
#         {k: s.get(k) for k in fieldnames}
#         for s in snapshots
#         if (s.get("video_id"), s.get("recorded_at")) not in existing_pairs
#     ]

#     if not new_rows:
#         print("No new trending snapshots to add.")
#         return

#     _append_to_sheet(TRENDING_SHEET_NAME, fieldnames, new_rows, needs_header)
#     print(f"Added {len(new_rows)} new records to Google Sheet '{TRENDING_SHEET_NAME}'.")


def update_trending_sheet(snapshots):
    """
    Appends all trending snapshot records directly to the Google Sheet (youtube_trending_history).
    - Skips Redis and Sheet deduplication checks since data is ephemeral.
    - Only fetches the first row to check for headers (efficient for large sheets).

    args:
        snapshots (list[dict]): List of trending snapshot records.
    """
    if not snapshots:
        print("No snapshots to add.")
        return

    fieldnames = ["video_id", "publish_date", "views", "likes", "comment_count", "recorded_at"]
    sheet = client.open_by_key(SHEET_ID).worksheet(TRENDING_SHEET_NAME)

    try:
        # Efficiently check only the first row
        first_row = sheet.row_values(1)
        needs_header = not first_row or len(first_row) == 0
    except Exception as e:
        print(f"Warning: Could not read first row — assuming header missing. ({e})")
        needs_header = True

    # Add header if missing
    if needs_header:
        print("Adding header row to trending sheet...")
        sheet.append_row(fieldnames)

    # Convert snapshots (list[dict]) to lists matching fieldnames
    cleaned_rows = []
    for s in snapshots:
        cleaned_rows.append([s.get(f, "") for f in fieldnames])

    # Batch append
    if cleaned_rows:
        sheet.append_rows(cleaned_rows, value_input_option="USER_ENTERED")
        print(f"Added {len(cleaned_rows)} trending snapshot records to '{TRENDING_SHEET_NAME}'.")
    else:
        print("No valid snapshot data to append.")


def clear_sheet_completely(sheet_name):
    """
    Deletes all rows and headers from a Google Sheet worksheet.
    """
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    sheet.clear()
    print(f"Cleared all content from '{sheet_name}'")
