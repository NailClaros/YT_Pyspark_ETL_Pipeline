from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()
import os

def run_yt_api(yt_key, size=5) -> list[dict]:
    """
        Fetches the most popular videos from YouTube API.
        size: Number of videos to fetch (max 50 and defaults to 5).
        yt_key: Optional YouTube API key. If not provided, uses the environment variable.
        Returns a list of dictionaries with video IDs as keys and metadata (title, views, tags) as values.
    """
    if not yt_key:
        YT_API_KEY = os.getenv("YT_API_KEY")
    else:
        YT_API_KEY = yt_key
    
    if size < 1:
        return []
    
    try:
        youtube = build("youtube", "v3", developerKey=YT_API_KEY)
        request = youtube.videos().list(
            part="snippet,statistics",
            chart="mostPopular",
            regionCode="US",
            maxResults=size
        )

        response = request.execute()

        results = []

        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})

            thumbnails = snippet.get("thumbnails", {})
            dt = datetime.strptime(snippet.get("publishedAt"),"%Y-%m-%dT%H:%M:%SZ")

            record = {
                "video_id": item.get("id"),
                "title": snippet.get("title"),
                "channel_title": snippet.get("channelTitle"),
                "category_id": snippet.get("categoryId"),
                "trending_date": dt.strftime("%m-%d-%Y"),
                "tags": snippet.get("tags", []),
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
                "thumbnail_link": thumbnails.get("high", {}).get("url") or thumbnails.get("default", {}).get("url"),
                "description": snippet.get("description"),
                "recorded_at": datetime.now().strftime("%m-%d-%Y")
            }

            results.append(record)

            # for key, value in record.items():
            #     print(f"{key}: {value}")
            # print("---")

        return results

    except Exception as e:
        print(f"Error fetching data from YouTube API: {e}")
        return []
    


## Good way to test the function
# res = run_yt_api(os.getenv("YT_API_KEY"), size=10)

# from db import add_video_P, add_trending_snapshot_P

# db_inset = add_video_P(res, env="test", schema="aq_test_local")
# if db_inset == 1:
#     print("Video inserted successfully.")

# db_inset2 = add_trending_snapshot_P(res, env="test", schema="aq_test_local")
# if db_inset2 == 1:
#     print("Trending snapshot inserted successfully.")

# print(f"Total videos fetched: {len(res)}")

# print(f"{len(res)} videos added to the database.")