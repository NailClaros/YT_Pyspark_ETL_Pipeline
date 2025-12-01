from ty_api import run_yt_api
import os

def test_succesful_run_yt_api():
    """Test the run_yt_api function to ensure it fetches data correctly."""
    # auto gets api key from env
    videos = run_yt_api(yt_key=os.getenv("YT_API_KEY"), size=5)
    assert isinstance(videos, list), "Expected a list of videos"
    assert len(videos) == 5, "Expected 5 videos to be fetched"

def test_run_yt_api_no_key():
    """Test the run_yt_api function with no API key."""
    videos = run_yt_api(size=5)
    # without an API key, it should run as long as the proper key env vars are set 
    assert len(videos) == 5, "Expected 5 videos to be fetched even with no API key"
    assert isinstance(videos, list), "Expected a list type even when no API key is provided"

def test_run_yt_api_invalid_key():
    """Test the run_yt_api function with an invalid API key."""
    try:
        videos = run_yt_api(yt_key="INVALID_KEY", size=5)
    except Exception as e:
        assert "Error fetching data from YouTube API" in str(e)

    assert isinstance(videos, list), "Expected a list of videos"
    assert len(videos) == 0, "Expected no videos to be fetched with an invalid API key"

def test_run_yt_api_zero_size():
    """Test the run_yt_api function with size set to zero."""
    videos = run_yt_api(size=0)
    ## should return an empty list
    assert isinstance(videos, list), "Expected a list of videos"
    assert len(videos) == 0, "Expected no videos to be fetched when size is zero"

def test_run_yt_api_negative_size():
    """Test the run_yt_api function with a negative size."""
    videos = run_yt_api(size=-5)
    ## should return an empty list
    assert isinstance(videos, list), "Expected a list of videos"
    assert len(videos) == 0, "Expected no videos to be fetched when size is negative"