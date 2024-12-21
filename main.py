import os
import requests
import json
from PyPDF2 import PdfReader
from tempfile import NamedTemporaryFile

# Directory to save extracted text
text_dir = "texts"
os.makedirs(text_dir, exist_ok=True)

# Open the JSONL file with paper entries
with open("papers.jsonl", "r") as file:
    for line in file:
        paper = json.loads(line.strip())
        
        # Get the PDF field
        pdf_field = paper.get("openAccessPdf")
        
        # Check if the field is a dictionary or a direct URL
        pdf_url = None
        if isinstance(pdf_field, dict) and "url" in pdf_field:
            pdf_url = pdf_field["url"]
        elif isinstance(pdf_field, str):
            pdf_url = pdf_field
        
        if not pdf_url:
            continue  # Skip if no valid PDF URL
        
        # Extract paper ID and title for naming
        paper_id = paper.get("paperId", "unknown_id")
        title = paper.get("title", "unknown_title").replace(" ", "_").replace("/", "_")
        text_path = os.path.join(text_dir, f"{paper_id}_{title}.txt")
        
        try:
            # Download the PDF into a temporary file
            print(f"Downloading PDF for {title}...")
            response = requests.get(pdf_url)
            response.raise_for_status()  # Raise an error if the download fails
            
            with NamedTemporaryFile(suffix=".pdf") as temp_pdf:
                temp_pdf.write(response.content)
                temp_pdf.flush()  # Ensure all data is written
                
                # Extract text from the PDF
                print(f"Extracting text from {title}...")
                reader = PdfReader(temp_pdf.name)
                text = ""
                for page in reader.pages:
                    text += page.extract_text()
                
                # Save the extracted text
                with open(text_path, "w", encoding="utf-8") as text_file:
                    text_file.write(text)
                print(f"Saved extracted text to {text_path}")
        
        except Exception as e:
            print(f"Error processing {title}: {e}")
