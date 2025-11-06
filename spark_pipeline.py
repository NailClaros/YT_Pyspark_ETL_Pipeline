from ty_api import run_yt_api
from db import add_video_P, add_trending_snapshot_P, wipe_youtube_tables
from g_sheets import update_videos_sheet, update_trending_sheet, clear_sheet_completely, \
    cache_video_ids_idempotent, get_existing_keys_cached, clear_redis_cache
from dotenv import load_dotenv
import os

load_dotenv()

def run_pipeline(api_key=os.getenv("YT_API_KEY")):
    
    try:
        # # Testind code to wipe tables and sheets and verify functionality
        # wipe_youtube_tables()
        # clear_sheet_completely("vids")
        # clear_sheet_completely("snapshots")
        # clear_redis_cache()
        # from time import sleep
        # sleep(5)  # Just to ensure tables are wiped before proceeding

        
        print("\033[1;32m===========\nSpark YT Pipeline running...\n===========\033[0m\n\n")

        #-- Fetch data from YouTube API
        print("\033[4m" + "--Running YT API function..." + "\033[0m\n\n")
        videos = run_yt_api(api_key, size=10)
        if not videos:
            print("No videos fetched from YouTube API.")
            return

        print("\n\033[33m=== Checking Redis cache for existing videos ===\033[0m\n")
        cached_ids, _ = get_existing_keys_cached(key_fields=["video_id"])
        new_videos = [v for v in videos if v["video_id"] not in cached_ids]

        print(f"Fetched {len(videos)} videos from API.")
        print(f"{len(cached_ids)} cached videos found.")
        print(f"{len(new_videos)} new videos will be processed.\n")

        ##-- If no new videos, skip DB and Sheet updates and just update snapshot sheet
        if not new_videos:
            print("\033[33m******\033[0m")
            print("\033[33mAll videos are already cached — skipping DB and video Sheet updates and updating snapshot sheet.\033[0m")
            print("\033[33m******\033[0m\n\n")

            print("\033[4m" + "--Running Database functions.." + "\033[0m")
            db_result_snapshots = add_trending_snapshot_P(videos, env="prod")
            if db_result_snapshots:
                print(f"\033[34mAttempted to Inserted {len(videos)} trending snapshots into the database.\033[0m\n\n")
            else:
                print("Failed to insert trending snapshots into the database. Aborting pipeline.")
                raise Exception("DB insertion failed.")

            print("\033[4m" + "--Running Google Sheets functions..." + "\033[0m\n\n")
            #-- Update trending snapshots sheet
            update_trending_sheet(videos)

            ##-- Update Redis cache
            print("\n=== Updating Redis cache ===\n")
            cache_video_ids_idempotent(videos, env="test", ttl_hours=24)

            print("\n\n\033[1;32mPipeline completed successfully!\033[0m\n")

            return {"status": "success", "message": "No new videos to process."}

        ##-- Proceed with DB and Sheet updates for new videos
        print("\n\n\033[4m" + "--Running Database functions.." + "\033[0m")




        #-- Insert videos into the database
        db_result_videos = add_video_P(new_videos, env="prod")
        if db_result_videos:
            print(f"\033[34m{len(new_videos)} were found from a sucessful API call, \n..attempting to send to videos table...\033[0m\n")
        else:
            print("Failed to insert videos into the database.")
            raise Exception("DB insertion failed for video Table. Aborting pipeline.")

        #-- Insert trending snapshots into the database
        db_result_snapshots = add_trending_snapshot_P(videos, env="prod")
        if db_result_snapshots:
            print(f"\033[34mAttempted to Inserted {len(videos)} trending snapshots into the database.\033[0m\n\n")
        else:
            print("Failed to insert trending snapshots into the database. Aborting pipeline.")
            raise Exception("DB insertion failed.")


        print("\033[4m" + "--Running Google Sheets functions..." + "\033[0m\n\n")
        #-- Update videos sheet
        update_videos_sheet(new_videos)
        #-- Update trending snapshots sheet
        update_trending_sheet(videos)

        ##-- Update Redis cache
        print("\n=== Updating Redis cache ===\n")
        # cache_video_ids_idempotent(videos, ttl_hours=0.009)
        cache_video_ids_idempotent(new_videos, ttl_hours=24)

        print("\n\n\033[1;32mPipeline completed successfully!\033[0m\n")
        return {"status": "success"}

    except Exception as e:
        print(f"Pipeline failed: {e}")
        return {"status": "failure"}

run_pipeline()