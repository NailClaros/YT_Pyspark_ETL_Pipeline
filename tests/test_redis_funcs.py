from g_sheets import cache_video_ids_idempotent, get_existing_keys_cached
import pytest

test_videos = [
        {
            "video_id": "test_vid_1",
            "title": "Test Video 1",
            "publish_date": "2023-01-01",
            "views": 1000,
            "likes": 100,
            "comment_count": 10,
            "thumbnail_link": "http://example.com/thumb1.jpg",
            "recorded_at": "2023-10-01 12:00:00",
            "category_id": 1,
            "channel_title": "Test Channel",
            "recorded_at": "2023-10-01 12:00:00",
            "tags": ["test", "video"]
        },
        {
            "video_id": "test_vid_2",
            "title": "Test Video 2",
            "publish_date": "2023-01-02",
            "views": 2000,
            "likes": 200,
            "comment_count": 20,
            "thumbnail_link": "http://example.com/thumb2.jpg",
            "recorded_at": "2023-10-01 12:05:00",
            "category_id": 2,
            "channel_title": "Test Channel 2",
            "recorded_at": "2023-10-01 12:05:00",
            "tags": ["test", "video2"]
        }
    ]

test_videos_bad = [
    {
            "video_id": "",
            "title": "Test Video 1",
            "publish_date": "2023-01-01",
            "views": 1000,
            "likes": 100,
            "comment_count": 10,
            "thumbnail_link": "http://example.com/thumb1.jpg",
            "recorded_at": "2023-10-01 12:00:00",
            "category_id": 1,
            "channel_title": "Test Channel",
            "recorded_at": "2023-10-01 12:00:00",
            "tags": ["test", "video"]
        },
        {
            "video_id": "test_vid_2",
            "title": "Test Video 2",
            "publish_date": "2023-01-02",
            "views": 2000,
            "likes": 200,
            "comment_count": 20,
            "thumbnail_link": "http://example.com/thumb2.jpg",
            "recorded_at": "2023-10-01 12:05:00",
            "category_id": 2,
            "channel_title": "Test Channel 2",
            "recorded_at": "2023-10-01 12:05:00",
            "tags": ["test", "video2"]
        }
]

def sim_cahced_ids(redis_client, test_videos, prefix="ptest"):
    """
    This funcion simulates caching video IDs in Redis for testing purposes and
    does what the function in g_sheets.py - update_video_sheet - relies on
    by setting the "in_sheet" field for each video ID key.
    
    args:
        redis_client: redis.Redis : Redis client
        test_videos: list[dict] : List of video dicts to cache
        prefix: str : Prefix for Redis keys
    """
    if redis_client:
        for v in test_videos:
            key = f"{prefix}:{v['video_id']}"
            redis_client.hset(key, "in_sheet", "yes")

@pytest.mark.parametrize("videos", [test_videos])
def test_simple_cache_video_ids_idempotent(videos, redis_test_client):

    cached_ids_before = get_existing_keys_cached(key_fields=["video_id"], redis_client=redis_test_client, prefix="ptest")[0]
    assert len(cached_ids_before) == 0

    cached_ids = cache_video_ids_idempotent(videos, redis_client=redis_test_client, prefix="ptest")
    assert cached_ids["added"] == 2
    
    sim_cahced_ids(redis_test_client, test_videos)
    ids = get_existing_keys_cached(key_fields=["video_id"], redis_client=redis_test_client, prefix="ptest")[0]
    print(f"Cached IDs after: {ids}")
    print(f"ids type: {type(ids)}")
    assert "test_vid_1" in ids
    assert "test_vid_2" in ids

    cached_ids_after = get_existing_keys_cached(key_fields=["video_id"], redis_client=redis_test_client, prefix="ptest")[0]
    assert len(cached_ids_after) == 2
    assert "test_vid_1" in cached_ids_after
    assert "test_vid_2" in cached_ids_after

@pytest.mark.parametrize("videos", [test_videos])
def test_idempotent_cache_video_ids_idempotent(videos, redis_test_client):

    # First caching
    cached_ids_1 = cache_video_ids_idempotent(videos, redis_client=redis_test_client, prefix="ptest")
    assert cached_ids_1["added"] == 2
    assert cached_ids_1["refreshed"] == 0
    assert cached_ids_1["skipped"] == 0
    assert cached_ids_1["error"] == None

    # Simulate that these IDs are now in the sheet
    sim_cahced_ids(redis_test_client, test_videos)

    # Second caching - should be idempotent and add 0 new IDs
    cached_ids_2 = cache_video_ids_idempotent(videos, redis_client=redis_test_client, prefix="ptest")
    assert cached_ids_2["added"] == 0
    assert cached_ids_2["refreshed"] == 2
    assert cached_ids_1["skipped"] == 0
    assert cached_ids_1["error"] == None

    # Verify cached IDs
    cached_ids_final = get_existing_keys_cached(key_fields=["video_id"], redis_client=redis_test_client, prefix="ptest")[0]
    assert len(cached_ids_final) == 2
    assert "test_vid_1" in cached_ids_final
    assert "test_vid_2" in cached_ids_final

@pytest.mark.parametrize("videos", [test_videos_bad])
def test_partial_cache_video_ids_idempotent(videos, redis_test_client):
    # First caching
    cached_ids_1 = cache_video_ids_idempotent(videos, redis_client=redis_test_client, prefix="ptest")
    assert cached_ids_1["added"] == 1
    assert cached_ids_1["refreshed"] == 0
    assert cached_ids_1["skipped"] == 1
    assert cached_ids_1["error"] == None


    # Second caching - should add 0 new IDs, refresh 2, skip 0
    cached_ids_2 = cache_video_ids_idempotent(videos, redis_client=redis_test_client, prefix="ptest")
    assert cached_ids_2["added"] == 0
    assert cached_ids_2["refreshed"] == 1
    assert cached_ids_2["skipped"] == 1
    assert cached_ids_2["error"] == None

    # Verify cached IDs
    cached_ids_final = get_existing_keys_cached(key_fields=["video_id"], redis_client=redis_test_client, prefix="ptest")[0]
    assert len(cached_ids_final) == 1
    assert "test_vid_1" not in cached_ids_final
    assert "test_vid_2" in cached_ids_final

@pytest.mark.parametrize("videos", [test_videos_bad])
def test_cache_video_ids_idempotent_bad_data(videos, redis_test_client):
    cached_ids = cache_video_ids_idempotent(videos, redis_client=redis_test_client, prefix="ptest")
    assert cached_ids["added"] == 1  # Only one valid video_id
    assert cached_ids["refreshed"] == 0
    assert cached_ids["skipped"] == 1
    assert cached_ids["error"] is None  

    cached_ids_final = get_existing_keys_cached(key_fields=["video_id"], redis_client=redis_test_client, prefix="ptest")[0]
    assert len(cached_ids_final) == 1
    assert "test_vid_2" in cached_ids_final

@pytest.mark.parametrize("videos", [test_videos])
def test_cache_video_ids_idempotent_no_redis(videos, redis_bad_client):
    try:
        cached_ids = cache_video_ids_idempotent(videos, redis_client=redis_bad_client, prefix="ptest")
    except Exception as e:
        print(f"Expected error occurred: {e}")

    assert cached_ids["added"] == 0  
    assert cached_ids["refreshed"] == 0
    assert cached_ids["skipped"] == 0
    assert cached_ids["error"] is not None  # Expect an error due to bad Redis connection