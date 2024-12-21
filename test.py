import requests 
import json 
 
query = "biomedical | (bio medical)" 
fields = "publicationDate,year,title,openAccessPdf" 
 
url = f"http://api.semanticscholar.org/graph/v1/paper/search/bulk?query={query}&fields={fields}&publicationDateOrYear=2016-03-05:&sort=publicationDate:asc" 
r = requests.get(url).json() 
 
print(f"Will retrieve an estimated {r['total']} documents") 
retrieved = 0 
 
with open(f"papers.jsonl", "a") as file: 
    newest_val = "1980-01-01"  # Start with a date far in the past
    while True: 
        if "data" in r: 
            retrieved += len(r["data"]) 
            print(f"Retrieved {retrieved} papers...") 
            for paper in r["data"]: 
                publication_date = paper.get("publicationDate")
                
                if publication_date and publication_date < newest_val: 
                    newest_val = publication_date  # Update newest_val if the date is not None and is earlier
                
                print(json.dumps(paper), file=file) 
        if "token" not in r: 
            break 
        r = requests.get(f"{url}&token={r['token']}").json() 
 
print(f"Done! Retrieved {retrieved} papers total")
