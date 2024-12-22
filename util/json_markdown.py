import os
import json
from typing import Dict, Any, List

class JsonToMarkdown:
    _directory_created = False

    @classmethod
    def create_directory(cls):
        if not cls._directory_created:
            os.makedirs("md", exist_ok=True)
            cls._directory_created = True

    @staticmethod
    def extract_tags(paper: Dict[str, Any]) -> List[str]:
        """Extract unique categories from s2FieldsOfStudy list."""
        if "s2FieldsOfStudy" not in paper or not paper["s2FieldsOfStudy"]:
            return ["Uncategorized"]
        
        # Extract unique categories using a set comprehension
        unique_categories = {
            field["category"] 
            for field in paper["s2FieldsOfStudy"] 
            if isinstance(field, dict) and "category" in field
        }
        
        return list(unique_categories) if unique_categories else ["Uncategorized"]

    @staticmethod
    def create_markdown(paper: Dict[str, Any]) -> str:
        required_keys = ["paperId", "title", "year", "publicationDate"]
        if not all(key in paper for key in required_keys):
            return "Invalid paper: missing required keys"

        # Extract tags from s2FieldsOfStudy
        tags = JsonToMarkdown.extract_tags(paper)
        
        # Handle optional PDF URL
        pdf_url = ""
        if not isinstance(paper.get("openAccessPdf"), dict):
            return "invalid paper: missing pdf"
        pdf_url = paper["openAccessPdf"].get("url", "")

        # Handle optional abstract
        abstract = paper.get("abstract", "")
        if abstract is None:
            abstract = ""

        JsonToMarkdown.create_directory()
        with open(f"md/{paper['paperId']}.mdx", "w", encoding="UTF-8") as file:
            yaml_front_matter = f"""---
title: '{paper['title']}'
date: {paper["publicationDate"]}
tags: {json.dumps(tags)}
draft: false
layout: PostBanner
summary: {abstract}
---

[Click to read full PDF]({pdf_url})

"""
            file.write(yaml_front_matter)


        return f"File created successfully: md/{paper['paperId']}.mdx"
    

    
with open ("papers.jsonl","r") as file:
    for line in file.readlines():
        if line:
            print(line)
            
            res = JsonToMarkdown.create_markdown(json.loads(line.replace("\'","")))
            print(res)