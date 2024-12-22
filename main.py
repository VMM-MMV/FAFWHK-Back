import requests
import json
import time
import os
from datetime import datetime
from filelock import FileLock

# =========== CONFIGURATION ===========

QUERY = "biomedical | (bio medical)"
FIELDS = "publicationDate,year,title,openAccessPdf,s2FieldsOfStudy,abstract,journal"
INITIAL_DATE = "2024-1-15"             # Starting date
OUTPUT_FILE = "papers.jsonl"            # Where fetched papers are written
LOCK_FILE = "papers.jsonl.lock"         # For safe writes to OUTPUT_FILE
PROCESSED_IDS_FILE = "processed_ids.json"  # Stores ALL processed IDs, by date

# How often (in seconds) we “promote” pending_newest_val => newest_val
UPDATE_INTERVAL = 5

# Small sleep to avoid hammering the API in a tight loop
FETCH_SLEEP = 2

# =====================================

running = True

# The official newest_val we use in queries,
# frozen for a 10-second window at a time.
newest_val = INITIAL_DATE

# The “pending” newest_val for the next update window.
pending_newest_val = newest_val

# For controlling how often we update newest_val
last_update_time = time.time()

# A dictionary that will map date_str => set_of_ids, loaded from processed_ids.json
processed_ids_by_date = {}
# A global set of all processed IDs across all dates (for fast membership checks)
all_processed_ids = set()


# ----------------------------------------------------------------------
#                           FILE STORAGE FUNCTIONS
# ----------------------------------------------------------------------

def load_all_processed_ids():
    """
    Load processed IDs from PROCESSED_IDS_FILE, which looks like:
      {
        "processedIds": {
          "YYYY-MM-DD": ["paperId1", "paperId2", ...],
          "YYYY-MM-DD": [...]
        }
      }
    Returns a dict date => set_of_ids, plus a global set of all IDs.
    If file does not exist or is invalid, returns empty structures.
    """
    if not os.path.exists(PROCESSED_IDS_FILE):
        return {}, set()  # no file yet => empty

    try:
        with open(PROCESSED_IDS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "processedIds" not in data:
            print(f"[WARN] {PROCESSED_IDS_FILE} has invalid structure. Starting fresh.")
            return {}, set()

        processed_section = data["processedIds"]
        if not isinstance(processed_section, dict):
            print(f"[WARN] 'processedIds' key is not a dict. Starting fresh.")
            return {}, set()

        # Convert all lists into sets
        loaded_by_date = {}
        for date_str, id_list in processed_section.items():
            if isinstance(id_list, list):
                loaded_by_date[date_str] = set(id_list)
            else:
                loaded_by_date[date_str] = set()

        # Build one big set of all IDs
        global_set = set()
        for s in loaded_by_date.values():
            global_set |= s  # union

        return loaded_by_date, global_set

    except (json.JSONDecodeError, OSError) as e:
        print(f"[WARN] Could not read {PROCESSED_IDS_FILE}: {e}. Starting fresh.")
        return {}, set()


def save_all_processed_ids(by_date_dict):
    """
    Save the dict (date => set_of_ids) to PROCESSED_IDS_FILE in JSON format:
    {
      "processedIds": {
        "YYYY-MM-DD": [...],
        ...
      }
    }
    """
    # Convert sets back to lists
    to_save = {}
    for date_str, ids_set in by_date_dict.items():
        to_save[date_str] = list(ids_set)

    data = {"processedIds": to_save}

    try:
        with open(PROCESSED_IDS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except OSError as e:
        print(f"[ERROR] Could not write to {PROCESSED_IDS_FILE}: {e}")


# ----------------------------------------------------------------------
#                           LOGIC FUNCTIONS
# ----------------------------------------------------------------------

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
    - We store processed IDs across ALL dates in a single file (processed_ids.json).
    - If we see a new paper, we add it to `processed_ids_by_date[that_date]` and
      also to the global set `all_processed_ids`.
    - We keep checking if the paper's date is beyond pending_newest_val => then we
      update pending_newest_val.
    - Every UPDATE_INTERVAL seconds, if pending_newest_val != newest_val, we promote it.
    """
    global newest_val, pending_newest_val
    global running, last_update_time
    global processed_ids_by_date, all_processed_ids

    # Initially load everything from file
    processed_ids_by_date, all_processed_ids = load_all_processed_ids()
    print(f"[INFO] Loaded {len(all_processed_ids)} total processed IDs across all dates.")

    while running:
        # 1) Check if it's time to promote newest_val => pending_newest_val
        now = time.time()
        if now - last_update_time >= UPDATE_INTERVAL:
            if pending_newest_val != newest_val:
                print(f"[INFO] Advancing newest_val from {newest_val} to {pending_newest_val}.")
                newest_val = pending_newest_val
            last_update_time = now

        # 2) Build the query URL using the frozen newest_val
        url = build_url(newest_val)
        print(f"[DEBUG] Querying URL: {url}")

        try:
            # Fetch the first "page"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            token = data.get("token")
            total = data.get("total")
            if total is not None:
                print(f"[DEBUG] API indicates ~{total} documents for newest_val={newest_val}.")

            # Keep paging if there's a 'token'
            while True:
                papers = data.get("data", [])
                if not papers:
                    print("[DEBUG] No papers in this batch.")

                for paper in papers:
                    paper_id = paper.get("paperId")
                    pub_date = paper.get("publicationDate")

                    if not paper_id:
                        print("[WARN] Paper found with no paperId, skipping.")
                        continue

                    # Already processed across ANY date => skip
                    if paper_id in all_processed_ids:
                        # This prevents re-writing old data
                        continue

                    # Skip future-dated pubs
                    if pub_date and is_future_date(pub_date):
                        print(f"[WARN] Skipping future publication date: {pub_date}")
                        continue

                    # If we see a pub_date that is beyond pending_newest_val, update it
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

                    # It's a brand new paper => write to file
                    with FileLock(LOCK_FILE):
                        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                            f.write(json.dumps(paper) + "\n")

                    # Mark it as processed
                    all_processed_ids.add(paper_id)

                    # Also store it by date
                    # If the paper has no valid date, store under "unknown"
                    date_key = pub_date if pub_date else "unknown"
                    if date_key not in processed_ids_by_date:
                        processed_ids_by_date[date_key] = set()
                    processed_ids_by_date[date_key].add(paper_id)

                # Save updated processed IDs to file after each batch
                save_all_processed_ids(processed_ids_by_date)

                # Move on if there's another "page"
                if token:
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
