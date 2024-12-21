import requests
import json
import threading
import time

query = "biomedical | (bio medical)"
fields = "publicationDate,year,title,openAccessPdf"
initial_date = "2016-03-05"  
newest_val = initial_date

lock = threading.Lock()

retrieved = 0
running = True

def build_url(newest_val):
    return f"http://api.semanticscholar.org/graph/v1/paper/search/bulk?query={query}&fields={fields}&publicationDateOrYear={newest_val}:&sort=publicationDate:asc:"

def fetch_papers():
    global newest_val, retrieved, running

    with open("papers.jsonl", "a") as file:
        while running:
            try:
                with lock:
                    url = build_url(newest_val)
                print(f"Querying URL: {url}")

                response = requests.get(url)
                response.raise_for_status()
                r = response.json()

                if "total" in r:
                    print(f"Will retrieve an estimated {r['total']} documents")

                if "data" in r:
                    for paper in r["data"]:
                        publication_date = paper.get("publicationDate")
                        if publication_date:
                            with lock:
                                if publication_date > newest_val:
                                    newest_val = publication_date
                                    print(f"Updated newest_val to: {newest_val}")

                        print(json.dumps(paper), file=file)

                    retrieved += len(r["data"])
                    print(f"Retrieved {retrieved} papers...")

                if "token" not in r:
                    print("No more pages to fetch. Exiting.")
                    break

                url = f"{url}&token={r['token']}"
            except Exception as e:
                print(f"Error occurred: {e}")
                break

            time.sleep(1)

def update_url_timer():
    global newest_val, running

    while running:
        with lock:
            current_url = build_url(newest_val)
        print(f"Updated URL to: {current_url}")
        time.sleep(10)  

def main():
    global running

    try:
        fetch_thread = threading.Thread(target=fetch_papers)
        timer_thread = threading.Thread(target=update_url_timer)

        fetch_thread.start()
        timer_thread.start()

        fetch_thread.join()
        timer_thread.join()
    except KeyboardInterrupt:
        print("Stopping threads...")
        running = False

if __name__ == "__main__":
    main()
