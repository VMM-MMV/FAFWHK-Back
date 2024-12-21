import requests
import json

query = "biomedical | bio medical"
fields = "year,title,openAccessPdf"

url = f"http://api.semanticscholar.org/graph/v1/paper/search/bulk?query={query}&fields={fields}&year=2023-"
r = requests.get(url).json()

print(f"Will retrieve an estimated {r['total']} documents")
retrieved = 0
valid_papers = 0

with open("papers.jsonl", "a") as file:
    while True:
        if "data" in r:
            for paper in r["data"]:
                if paper.get("openAccessPdf") is not null:  # Check if openAccessPdf is not null
                    print(json.dumps(paper), file=file)
                    valid_papers += 1
            retrieved += len(r["data"])
            print(f"Retrieved {retrieved} papers... (Valid papers: {valid_papers})")
        if "token" not in r:
            break
        r = requests.get(f"{url}&token={r['token']}").json()

print(f"Done! Retrieved {retrieved} papers total, {valid_papers} with openAccessPdf.")
