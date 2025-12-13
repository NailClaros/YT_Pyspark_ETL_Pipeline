from g_sheets import update_videos_sheet, update_trending_sheet, cache_video_ids_idempotent
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

@pytest.mark.parametrize("videos", [test_videos])
def test_simple_add_videos_sheet(videos, redis_test_client, read_sheet_rows):

    cache_video_ids_idempotent(videos, redis_client=redis_test_client)
    
    added_count = update_videos_sheet(
        videos,
        sheet_name="tester-vids",
        redis_client=redis_test_client,
        prefix="ptest"
    )   

    assert added_count == 2

    records = read_sheet_rows("tester-vids")

    assert any(r["video_id"] == "test_vid_1" for r in records)
    assert any(r["video_id"] == "test_vid_2" for r in records)

@pytest.mark.parametrize("videos", [test_videos])
def test_add_duplicate_videos_sheet(videos, redis_test_client, read_sheet_rows):
    x = cache_video_ids_idempotent(videos, redis_client=redis_test_client)
    print(f"Cached {len(x)} video IDs: {x}")
    # First addition
    added_count_1 = update_videos_sheet(
        videos,
        sheet_name="tester-vids",
        redis_client=redis_test_client,
        prefix="ptest"
    )   
    assert added_count_1 == 2
    records = read_sheet_rows("tester-vids")
    assert len(records) == 2  # 2 records added
    assert any(r["video_id"] == "test_vid_1" for r in records)
    assert any(r["video_id"] == "test_vid_2" for r in records)

    cache_video_ids_idempotent(videos, redis_client=redis_test_client)
    # Second addition (duplicates)
    added_count_2 = update_videos_sheet(
        videos,
        sheet_name="tester-vids",
        redis_client=redis_test_client,
        prefix="ptest"
    )   
    assert added_count_2 == 0  # No new records should be added

    records = read_sheet_rows("tester-vids")

    assert len(records) == 2  # Still only 2 records
    assert any(r["video_id"] == "test_vid_1" for r in records)
    assert any(r["video_id"] == "test_vid_2" for r in records)

@pytest.mark.parametrize("videos", [test_videos])
def test_simple_add_trending_sheet(videos, gsheet_client, read_sheet_rows):
    added_count = update_trending_sheet(
        snapshots=videos,
        sheet_name="tester-snaps",
        xclient=gsheet_client
    )   

    assert added_count == 2

    records = read_sheet_rows("tester-snaps")

    assert any(r["video_id"] == "test_vid_1" for r in records)
    assert any(r["video_id"] == "test_vid_2" for r in records)

