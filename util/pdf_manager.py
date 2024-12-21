import requests
from PyPDF2 import PdfReader
import json
from io import BytesIO

def url_to_txt(pdf_url):
    try:
        response = requests.get(pdf_url)
        response.raise_for_status()

        pdf_bytes = BytesIO(response.content)

        reader = PdfReader(pdf_bytes)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        
        return text

    except Exception as e:
        print(f"Error processing {pdf_url}: {e}")

if __name__ == "__main__":
    with open('cs.jsonl', 'r') as file:
        data = json.load(file)


