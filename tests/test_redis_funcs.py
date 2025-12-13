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

def sim_cahced_ids(redis_client, test_videos, prefix="ptest"):
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