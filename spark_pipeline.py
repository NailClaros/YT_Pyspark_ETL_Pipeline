from ty_api import run_yt_api
from db import add_video_P, add_trending_snapshot_P, wipe_youtube_tables
from g_sheets import update_videos_sheet, update_trending_sheet, clear_sheet_completely, \
    cache_video_ids_idempotent, get_existing_keys_cached
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

        
        print("===\nSpark YT Pipeline running...\n===\n\n")

        #-- Fetch data from YouTube API
        print("\033[4m" + "--Running YT API function..." + "\033[0m\n\n")
        videos = run_yt_api(api_key, size=10)
        if not videos:
            print("No videos fetched from YouTube API.")
            return

        print("\n=== Checking Redis cache for existing videos ===\n")
        cached_ids, _ = get_existing_keys_cached(sheet_name="", key_fields=["video_id"], env="test")
        new_videos = [v for v in videos if v["video_id"] not in cached_ids]

        print(f"Fetched {len(videos)} videos from API.")
        print(f"{len(cached_ids)} cached videos found.")
        print(f"{len(new_videos)} new videos will be processed.\n")


        if not new_videos:
            print("******")
            print("All videos are already cached — skipping DB and video Sheet updates and updating snapshot sheet.")
            print("******\n\n")

            print("\033[4m" + "--Running Database functions.." + "\033[0m")
            db_result_snapshots = add_trending_snapshot_P(videos)
            if db_result_snapshots:
                print(f"Inserted {len(videos)} trending snapshots into the database.\n\n")
            else:
                print("Failed to insert trending snapshots into the database.")
                raise Exception("DB insertion failed.")

            print("\033[4m" + "--Running Google Sheets functions..." + "\033[0m\n\n")
            #-- Update trending snapshots sheet
            update_trending_sheet(videos)

            ##-- Update Redis cache
            print("\n=== Updating Redis cache ===\n")
            cache_video_ids_idempotent(videos, ttl_hours=0.009)

            return


        print("\n\n\033[4m" + "--Running Database functions.." + "\033[0m")

        #-- Insert videos into the database
        db_result_videos = add_video_P(new_videos)
        if db_result_videos:
            print(f"{len(new_videos)} were found from a sucessful API call, \n..attempting to send to db...\n")
        else:
            print("Failed to insert videos into the database.")
            raise Exception("DB insertion failed.")

        #-- Insert trending snapshots into the database
        db_result_snapshots = add_trending_snapshot_P(videos)
        if db_result_snapshots:
            print(f"Inserted {len(videos)} trending snapshots into the database.\n\n")
        else:
            print("Failed to insert trending snapshots into the database.")
            raise Exception("DB insertion failed.")


        print("\033[4m" + "--Running Google Sheets functions..." + "\033[0m\n\n")
        #-- Update videos sheet
        update_videos_sheet(new_videos)
        #-- Update trending snapshots sheet
        update_trending_sheet(videos)

        ##-- Update Redis cache
        print("\n=== Updating Redis cache ===\n")
        cache_video_ids_idempotent(videos, ttl_hours=0.009)

    except Exception as e:
        print(f"Pipeline failed: {e}")

run_pipeline()