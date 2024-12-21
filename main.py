import requests
import json
import time
import os
from datetime import datetime
from filelock import FileLock

# =========== CONFIGURATION ===========

QUERY = "biomedical | (bio medical)"
FIELDS = "publicationDate,year,title,openAccessPdf"
INITIAL_DATE = "2024-12-16"        # Starting date
OUTPUT_FILE = "papers.jsonl"       # Where fetched papers are written
LOCK_FILE = "papers.jsonl.lock"    # For safe writes to OUTPUT_FILE
PROCESSED_IDS_FILE = "processed_ids.json"  # Single file for active date + processed IDs

# How often (in seconds) we “promote” pending_newest_val => newest_val
UPDATE_INTERVAL = 10

# Small sleep to avoid slamming the API in a tight loop
FETCH_SLEEP = 1

# =====================================

running = True

# The official newest_val we use in queries,
# frozen for a 10-second window at a time.
newest_val = INITIAL_DATE

# The “pending” newest_val for the next update window.
pending_newest_val = newest_val

# For controlling how often we update newest_val
last_update_time = time.time()

# In-memory set of processed paper IDs for the *current* newest_val
processed_ids = set()

def load_processed_ids_file():
    """
    Attempt to load 'processed_ids.json'.
    Structure expected:
    {
      "date": "YYYY-MM-DD",
      "paperIds": ["paper1", "paper2", ...]
    }
    Returns (loaded_date_str, loaded_ids_set).
    If file not found or invalid, returns (None, empty set).
    """
    if not os.path.exists(PROCESSED_IDS_FILE):
        return None, set()  # File doesn't exist => no data

    try:
        with open(PROCESSED_IDS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Basic validation
        loaded_date = data.get("date")
        paper_ids = data.get("paperIds", [])
        if not isinstance(loaded_date, str) or not isinstance(paper_ids, list):
            print(f"[WARN] {PROCESSED_IDS_FILE} has invalid structure.")
            return None, set()
        return loaded_date, set(paper_ids)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[WARN] Could not read {PROCESSED_IDS_FILE}: {e}")
        return None, set()

def save_processed_ids_file(date_str: str, ids_set: set):
    """
    Save the current 'date' and paper IDs to 'processed_ids.json'.
    Overwrites any existing content.
    """
    data = {
        "date": date_str,
        "paperIds": list(ids_set)
    }
    try:
        with open(PROCESSED_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except OSError as e:
        print(f"[ERROR] Could not write {PROCESSED_IDS_FILE}: {e}")

def build_url(start_date: str) -> str:
    """
    Build the Semantic Scholar Bulk API URL
    from 'start_date' up to today's date, ascending by publication date.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    url = (
        "http://api.semanticscholar.org/graph/v1/"
        f"paper/search/bulk?query={QUERY}"
        f"&fields={FIELDS}"
        f"&publicationDateOrYear={start_date}:{today_str}"
        "&sort=publicationDate:asc:"
    )
    return url

def is_future_date(date_str: str) -> bool:
    """
    Returns True if date_str (YYYY-MM-DD) is in the future.
    Treat invalid format dates as 'future' to skip them.
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj > datetime.now()
    except ValueError:
        return True

def fetch_papers():
    """
    Continuously fetch new papers using the global newest_val,
    writing new results to OUTPUT_FILE without duplicates.
    - Update pending_newest_val if we discover a later publication date.
    - Every UPDATE_INTERVAL seconds, if pending_newest_val != newest_val:
        * Overwrite processed_ids.json with new date + empty set
        * Reload (which starts empty, or might load existing if we changed date earlier)
    """
    global newest_val, pending_newest_val, processed_ids
    global running, last_update_time

    # 1) On startup, try to load the existing processed_ids file
    file_date, file_ids = load_processed_ids_file()

    if file_date == newest_val:
        # The file's date matches our current newest_val
        processed_ids = file_ids
        print(f"[INFO] Loaded {len(processed_ids)} processed IDs for date={newest_val}.")
    else:
        # The file is either for another date or invalid
        print("[INFO] Starting fresh for newest_val=", newest_val)
        processed_ids = set()
        # Save a fresh file with the current newest_val
        save_processed_ids_file(newest_val, processed_ids)

    while running:
        # 2) Check if it's time to “promote” pending_newest_val => newest_val
        now = time.time()
        if now - last_update_time >= UPDATE_INTERVAL:
            if pending_newest_val != newest_val:
                print(f"[INFO] Advancing newest_val from {newest_val} to {pending_newest_val}.")
                newest_val = pending_newest_val

                # Overwrite processed_ids file with the new date + empty set
                processed_ids = set()
                save_processed_ids_file(newest_val, processed_ids)
                print(f"[INFO] Now tracking 0 processed IDs for newest_val={newest_val}.")

            last_update_time = now

        url = build_url(newest_val)
        print(f"[DEBUG] Querying URL: {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            token = data.get("token")
            total = data.get("total")
            if total is not None:
                print(f"[DEBUG] API indicates ~{total} documents for newest_val={newest_val}.")

            while True:
                papers = data.get("data", [])
                if not papers:
                    print("[DEBUG] No papers in this batch.")

                for paper in papers:
                    paper_id = paper.get("paperId")
                    pub_date = paper.get("publicationDate")

                    # Skip if no ID
                    if not paper_id:
                        print("[WARN] Paper found with no paperId, skipping.")
                        continue

                    # Skip if already processed
                    if paper_id in processed_ids:
                        # Already in the set => do not write again
                        continue

                    # Skip future-dated publications
                    if pub_date and is_future_date(pub_date):
                        print(f"[WARN] Skipping future publication date: {pub_date}")
                        continue

                    # If we see a pub date newer than our pending_newest_val, update it
                    if pub_date:
                        try:
                            pub_date_obj = datetime.strptime(pub_date, "%Y-%m-%d")
                            pending_obj = datetime.strptime(pending_newest_val, "%Y-%m-%d")
                            if pub_date_obj > pending_obj:
                                pending_newest_val = pub_date
                                print(f"[INFO] Updated pending_newest_val to {pending_newest_val}.")
                        except ValueError:
                            print(f"[WARN] Invalid publication date '{pub_date}', skipping.")
                            continue

                    # It's a new, valid paper => write to file
                    with FileLock(LOCK_FILE):
                        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                            f.write(json.dumps(paper) + "\n")

                    # Mark it as processed
                    processed_ids.add(paper_id)

                # Persist the updated processed_ids after each batch/page
                save_processed_ids_file(newest_val, processed_ids)

                if token:
                    # Move to the next page
                    next_url = f"{url}&token={token}"
                    print(f"[DEBUG] Fetching next page with token={token}...")
                    response = requests.get(next_url)
                    response.raise_for_status()
                    data = response.json()
                    token = data.get("token")
                else:
                    print("[DEBUG] No more pages for this query.")
                    break

        except Exception as e:
            print(f"[ERROR] Error occurred while fetching: {e}")

        # Small sleep to avoid spamming the API in a tight loop
        time.sleep(FETCH_SLEEP)

def main():
    try:
        fetch_papers()
    except KeyboardInterrupt:
        print("[INFO] Interrupted by user.")
    finally:
        print("[INFO] Exiting.")

if __name__ == "__main__":
    main()
