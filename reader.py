import json
from util.pdf_manager import url_to_txt

def get_paper_url(pdf_field):
    pdf_url = None
    if isinstance(pdf_field, dict) and "url" in pdf_field:
        pdf_url = pdf_field["url"]
    elif isinstance(pdf_field, str):
        pdf_url = pdf_field
    
    return pdf_url

def extract_body(json_file_path):
    with open(json_file_path, 'r') as file:
        papers = [json.loads(line) for line in file]
        
    if isinstance(papers, list):
        with open(f"papers_body.jsonl", "a") as file:
            for paper in papers:
                pdf_url = get_paper_url(paper.get("openAccessPdf"))
                if not pdf_url:
                    continue

                pdf_txt = url_to_txt(pdf_url)
                
                if not pdf_txt: continue

                temp_paper = paper
                temp_paper.pop("openAccessPdf")
                temp_paper.pop("year")
                temp_paper["document_content"] = pdf_txt
                print(json.dumps(temp_paper), file=file)

def find_entry(file_path, target_paper_id):
    with open(file_path, 'r') as file:
        for line in file:
            entry = json.loads(line)
            if entry.get("paperId") == target_paper_id:
                return entry

if __name__ == "__main__":
    extract_body("papers.jsonl")