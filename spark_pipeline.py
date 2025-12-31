from ty_api import run_yt_api
from db import add_video_P, add_trending_snapshot_P, wipe_youtube_tables
from g_sheets import update_videos_sheet, update_trending_sheet, clear_sheet_completely, \
    cache_video_ids_idempotent, get_existing_keys_cached, clear_redis_cache
from dotenv import load_dotenv
import os

load_dotenv()

def run_pipeline(api_key=os.getenv("YT_API_KEY"), ptest_mode: bool = False):
    try:
        # # Testind code to wipe tables and sheets and verify functionality
        # wipe_youtube_tables()
        # clear_sheet_completely(sheet_name="vids")
        # clear_sheet_completely(sheet_name="snapshots")
        # clear_redis_cache()
        # from time import sleep
        # sleep(5)  # Just to ensure tables are wiped before proceeding

        
        print(f"\033[1;32m===========\nSpark YT Pipeline running... [mode:{os.getenv("ENV", "test")}]\n===========\033[0m\n\n")

        #-- Fetch data from YouTube API
        print("\033[4m" + "--Running YT API function..." + "\033[0m\n\n")
        videos = run_yt_api(api_key, size=10)
        if not videos:
            print("No videos fetched from YouTube API.")
            raise Exception("No data from YouTube API.")

        #-- Check Redis cache for existing videos
        print("\n\033[33m=== Checking Redis cache for existing videos ===\033[0m\n")
        if ptest_mode:
            cached_ids, _ = get_existing_keys_cached(key_fields=["video_id"], prefix="ptest")
        else:
            cached_ids, _ = get_existing_keys_cached(key_fields=["video_id"])
        print(f"Cached IDs retrieved: {cached_ids}")
        print(f"secondary return from get_existing_keys_cached: {_}\n")
        current_video_ids = {v["video_id"] for v in videos}
        
        print(f"Current video IDs from API: {current_video_ids}\n")
        new_videos = [v for v in videos if v["video_id"] not in cached_ids]
        print(f"new videos to process: {[v['video_id'] for v in new_videos]}")

        print(f"Fetched {len(videos)} videos from API.\n")

        print(f"{len(cached_ids)} cached videos found.")
        print(f"{len(new_videos)} new videos will be processed.\n")

        
        ##-- If no new videos, skip DB and Sheet updates and just update snapshot sheet
        if not new_videos:
            print("\033[33m******\033[0m")
            print("\033[33mAll videos are already cached — skipping DB and video Sheet updates and updating snapshot sheet.\033[0m")
            print("\033[33m******\033[0m\n\n")

            ##-- Update Redis cache
            print("\n=== Updating Redis cache ===\n")
            if ptest_mode:
                cache_video_ids_idempotent(videos=videos, ttl_hours=24, prefix="ptest")
            else:
                cache_video_ids_idempotent(videos, ttl_hours=24)

            print("\033[4m" + "--Running Database functions.." + "\033[0m")
            if ptest_mode:
                db_result_snapshots = add_trending_snapshot_P(videos, env="test", table="youtube_trending_history_p_test", schema=os.getenv("POSTGRES_DB"))
            else:
                db_result_snapshots = add_trending_snapshot_P(videos)

            if db_result_snapshots:
                print(f"\033[34mAttempted to Inserted {len(videos)} trending snapshots into the database.\033[0m\n\n")
            else:
                print("Failed to insert trending snapshots into the database. Aborting pipeline.")
                raise Exception("DB insertion failed.")

            print("\033[4m" + "--Running Google Sheets functions..." + "\033[0m\n\n")
            #-- Update trending snapshots sheet
            if ptest_mode:
                update_trending_sheet(videos, sheet_name="tester-snaps")
                update_videos_sheet(videos, sheet_name="tester-vids", prefix="ptest")
            else:
                update_trending_sheet(videos)
                update_videos_sheet(videos)

            print("\n\n\033[1;32mPipeline completed successfully!\033[0m\n")

            return {"status": "success", 
                    "message": "No new videos to process.",
                    "refreshed_videos": len(videos),}

        ##-- Proceed with DB and Sheet updates for new videos
        print("\n\n\033[4m" + "--Running Database functions.." + "\033[0m")
        #-- Insert videos into the database
        if ptest_mode:
            db_result_videos = add_video_P(new_videos, env="test", table="youtube_videos_p_test", schema=os.getenv("POSTGRES_DB"))
        else:
            db_result_videos = add_video_P(new_videos)

        if db_result_videos:
            print(f"\033[34m{len(new_videos)} were found from a sucessful API call, \n..attempting to send to videos table...\033[0m\n")
        else:
            print("Failed to insert videos into the database.")
            raise Exception("DB insertion failed for video Table. Aborting pipeline.")

        #-- Insert trending snapshots into the database
        if ptest_mode:
            db_result_snapshots = add_trending_snapshot_P(videos, env="test", table="youtube_trending_history_p_test", schema=os.getenv("POSTGRES_DB"))
        else: 
            db_result_snapshots = add_trending_snapshot_P(videos)

        if db_result_snapshots:
            print(f"\033[34mAttempted to Inserted {len(videos)} trending snapshots into the database.\033[0m\n\n")
        else:
            print("Failed to insert trending snapshots into the database. Aborting pipeline.")
            raise Exception("DB insertion failed.")



        ##-- Update Redis cache
        print("\n=== Updating Redis cache ===\n")
        # cache videos
        if ptest_mode:
            cache_video_ids_idempotent(videos=videos, ttl_hours=24, prefix="ptest")
        else:
            cache_video_ids_idempotent(videos=videos, ttl_hours=24)



        print("\033[4m" + "--Running Google Sheets functions..." + "\033[0m\n\n")
        if ptest_mode:
            #-- Update videos sheet
            update_videos_sheet(new_videos, sheet_name="tester-vids", prefix="ptest")
            #-- Update trending snapshots sheet
            update_trending_sheet(videos, sheet_name="tester-snaps")
        else:
            #-- Update videos sheet
            update_videos_sheet(new_videos)
            #-- Update trending snapshots sheet
            update_trending_sheet(videos)



        print("\n\n\033[1;32mPipeline completed successfully!\033[0m\n")
        return {"status": "success",
                "new_videos_processed": len(new_videos),
                "refreshed_videos": len(videos) - len(new_videos),
                "total_videos": len(videos),}

    except Exception as e:
        print(f"Pipeline failed: {e}")
        return {"status": "failure",
                "message": str(e)}

run_pipeline()