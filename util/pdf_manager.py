import requests
from PyPDF2 import PdfReader
import json
from io import BytesIO

def yield_txt(papers):
    for paper in papers:
        
        pdf_field = paper.get("openAccessPdf")
        
        pdf_url = None
        if isinstance(pdf_field, dict) and "url" in pdf_field:
            pdf_url = pdf_field["url"]
        elif isinstance(pdf_field, str):
            pdf_url = pdf_field
        
        if not pdf_url:
            continue
        
        try:
            response = requests.get(pdf_url)
            response.raise_for_status()

            pdf_bytes = BytesIO(response.content)

            reader = PdfReader(pdf_bytes)
            text = ""
            for page in reader.pages:
                text += page.extract_text()
            
            yield text

        except Exception as e:
            print(f"Error processing {paper}: {e}")

if __name__ == "__main__":
    with open('cs.jsonl', 'r') as file:
        data = json.load(file)

    for text in yield_txt(data):
        print(text)
