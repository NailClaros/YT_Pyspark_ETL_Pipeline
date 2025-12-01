from g_sheets import update_videos_sheet, update_trending_sheet, clear_sheet_completely

def test_simple_add_videos_sheet(redis_test_client, read_sheet_rows):
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

    added_count = update_videos_sheet(
        test_videos,
        sheet_name="tester-vids",
        redis_client=redis_test_client,
        prefix="ptest:"
    )   

    assert added_count == 2

    records = read_sheet_rows("tester-vids")

    assert any(r["video_id"] == "test_vid_1" for r in records)
    assert any(r["video_id"] == "test_vid_2" for r in records)

