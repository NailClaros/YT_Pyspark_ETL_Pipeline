import csv
import os


def _get_existing_keys(file_path, key_fields):
    """Helper to collect unique identifiers from an existing CSV file."""
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return set(), True  # No file or empty will need header

    existing = set()
    needs_header = True

    with open(file_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames and all(k in reader.fieldnames for k in key_fields):
            needs_header = False
            for row in reader:
                existing.add(tuple(row[k] for k in key_fields))
        else:
            print(f"Header missing or invalid in {file_path} — will rewrite it.")

    return existing, needs_header


def _append_to_csv(file_path, fieldnames, rows, needs_header):
    """Helper to write rows to CSV, adding header if needed."""
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if needs_header:
            writer.writeheader()
        writer.writerows(rows)


def update_videos_csv(videos, file_path="youtube_videos.csv"):
    """
    Appends unique videos to a CSV file.
    Writes headers automatically on first run or if missing.
    Uniqueness based on 'video_id'.
    """
    if not videos:
        print("No videos to add.")
        return

    fieldnames = list(videos[0].keys())
    existing_ids, needs_header = _get_existing_keys(file_path, ["video_id"])

    # Filter unique
    new_videos = [v for v in videos if v["video_id"] not in (id_[0] for id_ in existing_ids)]
    if not new_videos:
        print("No new unique videos to add.")
        return

    _append_to_csv(file_path, fieldnames, new_videos, needs_header)
    print(f"Added {len(new_videos)} new videos to {file_path}.")


def update_trending_csv(snapshots, file_path="youtube_trending_history.csv"):
    """
    Appends unique trending snapshots to a CSV file.
    Writes headers automatically on first run or if missing.
    Uniqueness based on ('video_id', 'recorded_at').
    """
    if not snapshots:
        print("No snapshots to add.")
        return

    fieldnames = ["video_id", "trending_date", "views", "likes", "comment_count", "recorded_at"]
    existing_pairs, needs_header = _get_existing_keys(file_path, ["video_id", "recorded_at"])

    new_rows = [
        {k: s.get(k) for k in fieldnames}
        for s in snapshots
        if (s.get("video_id"), s.get("recorded_at")) not in existing_pairs
    ]

    if not new_rows:
        print("No new trending snapshots to add.")
        return

    _append_to_csv(file_path, fieldnames, new_rows, needs_header)
    print(f"Added {len(new_rows)} new snapshot records to {file_path}.")
