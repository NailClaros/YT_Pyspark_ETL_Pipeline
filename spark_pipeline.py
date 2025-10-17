from ty_api import run_yt_api
from db import add_video_P, add_trending_snapshot_P, wipe_youtube_tables
from g_sheets import update_videos_sheet, update_trending_sheet, clear_sheet_completely
from dotenv import load_dotenv
import os

load_dotenv()

def run_pipeline(api_key=os.getenv("YT_API_KEY")):
    
    try:
        # # Testind code to wipe tables and sheets and verify functionality
        # wipe_youtube_tables()
        # clear_sheet_completely("vids")
        # clear_sheet_completely("snapshots")
        # from time import sleep
        # sleep(5)  # Just to ensure tables are wiped before proceeding

        #-- Fetch data from YouTube API
        videos = run_yt_api(api_key, size=10)
        if not videos:
            print("No videos fetched from YouTube API.")
            return

        #-- Insert videos into the database
        db_result_videos = add_video_P(videos)
        if db_result_videos:
            print(f"Inserted {len(videos)} videos into the database.")
        else:
            print("Failed to insert videos into the database.")
            raise Exception("DB insertion failed.")

        #-- Insert trending snapshots into the database
        db_result_snapshots = add_trending_snapshot_P(videos)
        if db_result_snapshots:
            print(f"Inserted {len(videos)} trending snapshots into the database.")
        else:
            print("Failed to insert trending snapshots into the database.")
            raise Exception("DB insertion failed.")

        #-- Update Google Sheets
        update_videos_sheet(videos)
        update_trending_sheet(videos)
    except Exception as e:
        print(f"Pipeline failed: {e}")

run_pipeline()