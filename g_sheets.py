import gspread
from google.oauth2.service_account import Credentials
import os
import json
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


def _get_existing_keys(sheet_name, key_fields):
    """Collect existing keys from Google Sheet."""
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    records = sheet.get_all_records()  # list of dicts
    if not records:
        return set(), True

    existing = {tuple(str(row[k]) for k in key_fields) for row in records if all(k in row for k in key_fields)}
    needs_header = False
    return existing, needs_header


def _append_to_sheet(sheet_name, fieldnames, rows, needs_header):
    """Append rows to Google Sheet, adding header if needed."""
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    existing_data = sheet.get_all_values()

    if needs_header or not existing_data:
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
    

def update_videos_sheet(videos):
    """Appends unique videos to Google Sheet (youtube_videos)."""
    if not videos:
        print("No videos to add.")
        return

    fieldnames = list(videos[0].keys())
    existing_ids, needs_header = _get_existing_keys(VIDEOS_SHEET_NAME, ["video_id"])

    new_videos = [v for v in videos if (v["video_id"],) not in existing_ids]
    if not new_videos:
        print("No new unique videos to add.")
        return

    _append_to_sheet(VIDEOS_SHEET_NAME, fieldnames, new_videos, needs_header)
    print(f"Added {len(new_videos)} new videos to Google Sheet '{VIDEOS_SHEET_NAME}'.")


def update_trending_sheet(snapshots):
    """Appends unique trending snapshots to Google Sheet (youtube_trending_history)."""
    if not snapshots:
        print("No snapshots to add.")
        return

    fieldnames = ["video_id", "trending_date", "views", "likes", "comment_count", "recorded_at"]
    existing_pairs, needs_header = _get_existing_keys(TRENDING_SHEET_NAME, ["video_id", "recorded_at"])

    new_rows = [
        {k: s.get(k) for k in fieldnames}
        for s in snapshots
        if (s.get("video_id"), s.get("recorded_at")) not in existing_pairs
    ]

    if not new_rows:
        print("No new trending snapshots to add.")
        return

    _append_to_sheet(TRENDING_SHEET_NAME, fieldnames, new_rows, needs_header)
    print(f"Added {len(new_rows)} new records to Google Sheet '{TRENDING_SHEET_NAME}'.")


def clear_sheet_completely(sheet_name):
    """
    Deletes all rows and headers from a Google Sheet worksheet.
    """
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    sheet.clear()
    print(f"Cleared all content from '{sheet_name}'")
