import requests
import json

# query = "biomedical | (bio medical)"
query = "computer science"
fields = "publicationDate,year,title,openAccessPdf"

url = f"http://api.semanticscholar.org/graph/v1/paper/search/bulk?query={query}&fields={fields}&publicationDateOrYear=2024-12-01:"
r = requests.get(url).json()

print(f"Will retrieve an estimated {r['total']} documents")
retrieved = 0

papers = []

while True:
    if "data" in r:
        retrieved += len(r["data"])
        print(f"Retrieved {retrieved} papers...")
        papers.extend(r["data"])
    if "token" not in r:
        break
    r = requests.get(f"{url}&token={r['token']}").json()

with open(f"cs.jsonl", "w") as file:
    json.dump(papers, file, indent=4)

print(f"Done! Retrieved {retrieved} papers total")
