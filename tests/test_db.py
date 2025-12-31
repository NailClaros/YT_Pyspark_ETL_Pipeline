from db import add_video_P, add_trending_snapshot_P
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

test_snapshot_bad = [
        {
            "video_id": "test_vid_1",
            "publish_date": "2023-01-01",
            "views": 1500,
            "likes": 150,
            "comment_count": 15,
            "recorded_at": "2023-10-02 12:00:00"
        },
        {
            "video_id": "test_vid_2",
            "publish_date": "2023-01-02",
            "views": 2500,
            "likes": 250,
            "comment_count": 25,
            "recorded_at": "2023-10-02 12:05:00"
        },
        {
            "video_id": "test_vid_1",
            "publish_date": "2023-01-01",
            "views": 1500,
            "likes": 150,
            "comment_count": 15,
            "recorded_at": "2023-10-02 12:05:00"
        },
        {
            "video_id": "test_vid_2",
            "publish_date": "2023-01-02",
            "views": 2500,
            "likes": 250,
            "comment_count": 25,
            "recorded_at": "2023-10-02 12:10:00"
        }
    ]

test_snapshot_good = [
        {
            "video_id": "test_vid_1",
            "publish_date": "2023-01-01",
            "views": 1500,
            "likes": 150,
            "comment_count": 15,
            "recorded_at": "2023-10-02 12:00:00"
        },
        {
            "video_id": "test_vid_2",
            "publish_date": "2023-01-02",
            "views": 2500,
            "likes": 250,
            "comment_count": 25,
            "recorded_at": "2023-10-02 12:05:00"
        },
        {
            "video_id": "test_vid_1",
            "publish_date": "2023-01-01",
            "views": 1500,
            "likes": 150,
            "comment_count": 15,
            "recorded_at": "2023-10-02 13:00:00"
        },
        {
            "video_id": "test_vid_2",
            "publish_date": "2023-01-02",
            "views": 2500,
            "likes": 250,
            "comment_count": 25,
            "recorded_at": "2023-10-02 13:05:00"
        }
    ]

@pytest.mark.parametrize("videos", [test_videos])
def test_bad_conn_add_videos(videos, bad_db_conn, db_rows_videos):
    check = add_video_P(videos, conn=bad_db_conn, env="test", table="youtube_videos_p_test")
    assert check == 0  # failure due to bad connection

    rows = db_rows_videos()
    assert len(rows) == 0  # no rows added in real DB

@pytest.mark.parametrize("videos", [test_videos])
def test_add_videos(videos, db_rows_videos):
    check = add_video_P(videos, env="test", table="youtube_videos_p_test")
    assert check == 1  # success

    rows = db_rows_videos()
    assert len(rows) == 2

    video_ids = {row[0] for row in rows}
    assert "test_vid_1" in video_ids
    assert "test_vid_2" in video_ids

@pytest.mark.parametrize("videos", [test_videos])
def test_add_videos_duplicate_P(videos, db_rows_videos):
    videos1 = [test_videos[0]]  # Only one video to test duplicate insert
    check1 = add_video_P(videos1, env="test", table="youtube_videos_p_test")
    check2 = add_video_P(videos1, env="test", table="youtube_videos_p_test")  # Duplicate insert

    assert check1 == 1  # first insert success
    assert check2 == 1  # second insert should also return success but not add duplicate

    rows = db_rows_videos()
    assert len(rows) == 1  # still only 1 unique videos from previous test
    assert rows[0][0] == "test_vid_1"

    check = add_video_P(videos, env="test", table="youtube_videos_p_test")
    assert check == 1  # success
    rows = db_rows_videos()
    assert len(rows) == 2  # now both videos should be present
    video_ids = {row[0] for row in rows}
    assert "test_vid_1" in video_ids
    assert "test_vid_2" in video_ids

    check_dup = add_video_P(videos, env="test", table="youtube_videos_p_test")  # Duplicate insert
    assert check_dup == 1  # success but no duplicates added
    rows = db_rows_videos()
    assert len(rows) == 2  # still only 2 unique videos
    rows = db_rows_videos()
    assert len(rows) == 2  # still only 2 unique videos
    video_ids = {row[0] for row in rows}
    assert "test_vid_1" in video_ids
    assert "test_vid_2" in video_ids

@pytest.mark.parametrize("snapshots", [test_snapshot_bad])
def test_bad_conn_add_trending_snapshots(snapshots, bad_db_conn, db_rows_trend):
    check = add_trending_snapshot_P(snapshots, conn=bad_db_conn, env="test", table="youtube_trending_history_p_test")
    assert check == 0  # failure due to bad connection

    rows = db_rows_trend()
    assert len(rows) == 0  # no rows added in real DB


@pytest.mark.parametrize(("snapshots", "videos"),
    [(test_snapshot_good, test_videos),])
def test_add_trending_snapshots(snapshots, db_rows_trend, videos):
    rows = db_rows_trend()
    assert len(rows) == 0  # no rows added in real DB

    check = add_video_P(videos, env="test", table="youtube_videos_p_test")## preload data for relational integrity
    assert check == 1  # success

    check = add_trending_snapshot_P(snapshots, env="test", table="youtube_trending_history_p_test")
    assert check == 1  # success

    rows = db_rows_trend()
    assert len(rows) == 4  # all 4 snapshots added

    recorded_ats = [row[6].strftime("%Y-%m-%d %H:%M:%S") for row in rows]
    assert "2023-10-02 12:00:00" in recorded_ats
    assert "2023-10-02 12:05:00" in recorded_ats
    assert "2023-10-02 13:00:00" in recorded_ats
    assert "2023-10-02 13:05:00" in recorded_ats

@pytest.mark.parametrize(("snapshots", "videos"),
    [(test_snapshot_bad, test_videos),])
def test_add_trending_snapshots_bad_duplicate(snapshots, db_rows_trend, videos):
    rows = db_rows_trend()
    assert len(rows) == 0  # no rows added in real DB

    check = add_video_P(videos, env="test", table="youtube_videos_p_test")## preload data for relational integrity
    assert check == 1  # success

    check1 = add_trending_snapshot_P(snapshots, env="test", table="youtube_trending_history_p_test")
    assert check1 == 1  # success

    rows = db_rows_trend()
    assert len(rows) == 2  # 2 unique snapshots added

    # Attempt to add duplicates
    check2 = add_trending_snapshot_P(snapshots, env="test", table="youtube_trending_history_p_test")
    assert check2 == 1  # success but no duplicates added

    rows = db_rows_trend()
    assert len(rows) == 2  # still only 2 unique snapshots

@pytest.mark.parametrize(("snapshots", "videos"),
    [(test_snapshot_good, test_videos),])
def test_add_trending_snapshots_bad_db(bad_db_conn, snapshots, videos, db_rows_trend):
    rows = db_rows_trend()
    assert len(rows) == 0  # no rows added in real DB

    check = add_video_P(videos, conn=bad_db_conn, env="test", table="youtube_videos_p_test")## preload data for relational integrity
    assert check == 0  # failure due to bad connection

    check = add_trending_snapshot_P(snapshots, conn=bad_db_conn, env="test", table="youtube_trending_history_p_test")
    assert check == 0  # failure due to bad connection

    rows = db_rows_trend()
    assert len(rows) == 0  # no rows added in real DB

@pytest.mark.parametrize("snapshots", [test_snapshot_good])
def test_add_trending_snapshots_no_videos(snapshots, db_rows_trend):
    rows = db_rows_trend()
    assert len(rows) == 0  # no rows added in real DB

    check = add_trending_snapshot_P(snapshots, env="test", table="youtube_trending_history_p_test")
    assert check == 0  # failure due to missing videos

    rows = db_rows_trend()
    assert len(rows) == 0  # no rows added in real DB