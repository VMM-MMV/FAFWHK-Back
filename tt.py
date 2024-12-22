import requests
import json

query = "biomedical | ((computer science) + (ai |(artificial inteligence)) +(civil engineering) + architecture"
fields = "publicationDate,title,openAccessPdf,abstract"

url = f"http://api.semanticscholar.org/graph/v1/paper/search/bulk?query={query}&fields={fields}&publicationDateOrYear=2016-03-05:"
r = requests.get(url).json()

print(f"Will retrieve an estimated {r['total']} documents")
retrieved = 0

with open(f"papers.json2", "a") as file:
    newest_val = "1980-01-01"
    while True:
        if "data" in r:
            retrieved += len(r["data"])
            print(f"Retrieved {retrieved} papers...")
            for paper in r["data"]:
                print(json.dumps(paper), file=file)
        if "token" not in r:
            break
        r = requests.get(f"{url}&token={r['token']}").json()

print(f"Done! Retrieved {retrieved} papers total")