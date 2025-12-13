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

SHEET_ID = os.getenv("SHEET_ID")

# Two tabs/sheets inside the same spreadsheet
VIDEOS_SHEET_NAME = "vids"
TRENDING_SHEET_NAME = "snapshots"

def get_redis_client(env: str = os.getenv("ENV", "test")) -> redis.Redis:
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

def cache_video_ids_idempotent(
    videos:list,
    env:str = os.getenv("ENV", "prod"),
    prefix:str = "",
    ttl_hours:float = 24.0,
    redis_client= None
):
    """
    Caches video IDs in Redis with idempotency.
    args:
        videos: list[dict] : List of video dicts containing at least 'video_id'
        env: str : Environment name for namespacing keys
        prefix: str : Optional prefix for Redis keys
        ttl_hours: float : Time-to-live for each key in hours
        redis_client: redis.Redis : Optional Redis client. If None, a new client will be
            created based on the environment.
    returns:
        dict : Summary of added, refreshed, skipped counts
    """
    try:
        redis_client = redis_client or get_redis_client(env)
        if not redis_client or not videos:
            return {"added": 0, "refreshed": 0, "skipped": 0}

        prefix = f"{prefix}:" if prefix else f"{env}:"
        ttl_seconds = int(ttl_hours * 3600)

        from datetime import datetime
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

        # Load existing IDs
        existing_ids = set()
        for k in redis_client.scan_iter(f"{prefix}*"):
            if isinstance(k, bytes):
                k = k.decode()
            existing_ids.add(k.replace(prefix, ""))

        pipe = redis_client.pipeline()
        added = refreshed = skipped = 0

        for video in videos:
            vid = video.get("video_id")
            if not vid:
                skipped += 1
                continue

            key = f"{prefix}{vid}"

            if vid in existing_ids:
                pipe.expire(key, ttl_seconds)
                refreshed += 1
            else:
                pipe.hset(key, "video_id", vid)
                pipe.hset(key, "cached_at", now)
                pipe.hset(key, "env", env)
                pipe.hset(key, "title", video.get("title", ""))
                pipe.hset(key, "channel_id", video.get("channel_id", ""))
                pipe.hset(key, "published_at", video.get("published_at", ""))
                pipe.hset(key, "duration", video.get("duration", ""))
                pipe.hset(key, "thumbnail", video.get("thumbnail", ""))
                pipe.hset(key, "in_sheet", "no")  # Mark as not yet written to sheet
                pipe.expire(key, ttl_seconds)
                added += 1

        pipe.execute()

        print(f"Redis summary → added: {added}, refreshed: {refreshed}, skipped: {skipped}")

        return {
            "added": added,
            "refreshed": refreshed,
            "skipped": skipped,
            "error": None
        }
    except Exception as e:
        print(f"Error caching video IDs in Redis: {e}")
        return {
            "added": 0,
            "refreshed": 0,
            "skipped": 0,
            "error": str(e)
        }


## Google sheets functions
def _get_existing_keys(sheet_name, key_fields):
    """Collect existing keys from Google Sheet.
    args:
        sheet_name: str : Name of the sheet/tab
        key_fields: list[str] : List of field names that make up the key    
    returns:
        existing_ids (list[]) : Set of tuples representing existing keys
        needs_header (bool) : Whether the sheet is empty and needs a header
    """
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    records = sheet.get_all_records()  # list of dicts
    if not records:
        return [], True

    existing = [(str(row[k]) for k in key_fields) for row in records if all(k in row for k in key_fields)]
    needs_header = False
    return existing, needs_header

## Redis_function
def get_existing_keys_cached(
    key_fields,
    sheet_name="",
    env=os.getenv("ENV", "test"),
    redis_client=None,
    prefix=""
):
    """
    Pull existing keys from Redis.
    Returns:
        existing_ids (set[str])
        needs_header (bool)
    """

    redis_client = redis_client or get_redis_client(env)
    prefix = f"{prefix}:" if prefix else f"{env}:"

    if redis_client:
        print("Fetching existing keys from Redis...")
        try:
            existing_ids = set()

            for k in redis_client.scan_iter(f"{prefix}*"):
                if isinstance(k, bytes):
                    k = k.decode()

                video_id = k.split(":", 1)[1]
                existing_ids.add(video_id)

            print(f"Found {len(existing_ids)} cached IDs in Redis.")
            return existing_ids, False

        except Exception as e:
            print(f"Redis unavailable ({e}) — falling back to Sheet.")

    # Fallback
    if not sheet_name:
        return set(), True

    return _get_existing_keys(sheet_name, key_fields)


def _append_to_sheet(sheet_name, fieldnames, rows, needs_header):
    """
    Append rows to a Google Sheet.
    Efficiently checks only the first row to determine if a header is needed.
    args:
        sheet_name: str : Name of the sheet/tab
        fieldnames: list[str] : List of field names (columns)
        rows: list[dict] : List of row dicts to append
        needs_header: bool : Whether to add header row
    returns:
        int : Number of rows added
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
        return len(cleaned_rows)




def update_videos_sheet(
    videos,
    env=os.getenv("ENV", "prod"),
    sheet_name="",
    redis_client=None,
    prefix=""
):
    """Appends unique videos to Google Sheet using Redis to prevent duplicates after insert.
    args:
        videos: list[dict] : List of video dicts containing at least 'video_id
        env: str : Environment name for namespacing keys
        sheet_name: str : Optional sheet name. If empty, defaults to "vids".
        redis_client: redis.Redis : Optional Redis client. If None, a new client will be
            created based on the environment.
        prefix: str : Optional prefix for Redis keys
    returns:
        int : Number of new videos added to the sheet
    """
    if not videos:
        print("No videos to add.")
        return 0

    fieldnames = list(videos[0].keys())

    # Fetch existing IDs (only those marked as in_sheet=yes)
    existing_ids, needs_header = get_existing_keys_cached(
        key_fields=["video_id"],
        sheet_name=sheet_name if sheet_name else "vids",
        redis_client=redis_client,
        prefix=prefix
    )

    # Filter new videos
    new_videos = [v for v in videos if v["video_id"] not in existing_ids]

    print(f"Found {len(new_videos)} new unique videos to add.")

    if not new_videos:
        print("No new unique videos to add.")
        return 0

    # Append to sheet
    _append_to_sheet(sheet_name if sheet_name else "vids", fieldnames, new_videos, needs_header)
    print(f"Added {len(new_videos)} new videos to Google Sheet '{sheet_name if sheet_name else 'vids'}'.")

    # Mark Redis entries as actually written into the sheet
    if redis_client:
        for v in new_videos:
            key = f"{prefix}:{v['video_id']}"
            redis_client.hset(key, "in_sheet", "yes")

    return len(new_videos)


def update_trending_sheet(snapshots, xclient=None, sheet_name=""):
    """
    Appends all trending snapshot records directly to the Google Sheet (youtube_trending_history).
    - Skips Redis and Sheet deduplication checks since data is ephemeral.
    - Only fetches the first row to check for headers (efficient for large sheets).

    args:
        snapshots (list[dict]): List of trending snapshot records.
        xclient: gspread.Client : Optional gspread client. If None, uses default client.
        sheet_name (str): Optional sheet name. If empty, defaults to "snapshots".
    """
    if not snapshots:
        print("No snapshots to add.")
        return 0

    fieldnames = ["video_id", "publish_date", "views", "likes", "comment_count", "recorded_at"]
    if xclient is None and sheet_name == "":
        sheet = client.open_by_key(os.getenv("SHEET_ID")).worksheet("snapshots")
    else:
        sheet = xclient.open_by_key(os.getenv("SHEET_ID")).worksheet(sheet_name)

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
        print(f"Added {len(cleaned_rows)} trending snapshot records to '{sheet_name if sheet_name else "Snapshots"}'.")
        return len(cleaned_rows)
    else:
        print("No valid snapshot data to append.")


def clear_sheet_completely(client, sheet_name):
    """
    Deletes all rows and headers from a Google Sheet worksheet.
    Used for testing purposes.
    args:
        client: gspread.Client : gspread client
        sheet_name: str : Name of the sheet/tab to clear
    """
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    sheet.clear()
    print(f"Cleared all content from '{sheet_name}'")

## Old functions for reference
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