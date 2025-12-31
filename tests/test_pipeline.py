from spark_pipeline import run_pipeline
from time import sleep

def test_run_pipeline_success(clear_sheets_after_test):
    result = run_pipeline(ptest_mode=True)
    assert result["status"] == "success"
    assert "new_videos_processed" in result and result["new_videos_processed"] == 10
    assert "refreshed_videos" in result and result["refreshed_videos"] == 0
    assert "total_videos" in result and result["total_videos"] == 10
    sleep(5)  # To avoid rate limits in tests

def test_run_pipeline_success_no_new_videos(clear_sheets_after_test):
    # First run to populate the database
    result = run_pipeline(ptest_mode=True)
    assert result["status"] == "success"
    assert "new_videos_processed" in result and result["new_videos_processed"] == 10
    assert "refreshed_videos" in result and result["refreshed_videos"] == 0
    assert "total_videos" in result and result["total_videos"] == 10
    sleep(5)  # To avoid rate limits in tests

    # Second run should find no new videos
    result = run_pipeline(ptest_mode=True)
    assert result["status"] == "success"
    assert "message" in result and "No new videos to process" in result["message"]
    assert "refreshed_videos" in result and result["refreshed_videos"] == 10
    sleep(5)  # To avoid rate limits in tests

def test_run_pipeline_failure(clear_sheets_after_test):
    # Simulate failure by passing invalid parameters
    try:
        result = run_pipeline(api_key="bad key", ptest_mode=True)
    except Exception as e:
        pass  # Expected to raise an exception

    assert result["status"] == "failure"
    assert "message" in result
    sleep(5)  # To avoid rate limits in tests