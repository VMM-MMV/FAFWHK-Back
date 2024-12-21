from elasticsearch import Elasticsearch
import os
import json
from util.pdf_manager import url_to_txt
from datetime import datetime
from dotenv import load_dotenv
import os

class PaperSearchSystem:
    def __init__(self, elastic_host='localhost', elastic_port=9200, index_name='papers'):
        """Initialize the search system with Elasticsearch connection"""
        # Updated connection initialization
        # self.es = Elasticsearch(f"http://{elastic_host}:{elastic_port}")
        load_dotenv()

        es_key = os.getenv('ES_KEY')

        self.es = Elasticsearch(f"https://ce8f4aa8de214bfa80e5c5b25079d80e.europe-west3.gcp.cloud.es.io:443", api_key=es_key)
        self.index_name = index_name
        
        # Create index if it doesn't exist
        if not self.es.indices.exists(index=self.index_name):
            self.create_index()

    def create_index(self):
        """Create Elasticsearch index with appropriate mappings for paper content search"""
        settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "paper_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "stop",
                                "snowball"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "paperId": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "analyzer": "paper_analyzer"
                    },
                    "document_content": {
                        "type": "text",
                        "analyzer": "paper_analyzer",
                        "term_vector": "with_positions_offsets"
                    },
                    "publicationDate": {"type": "date"},
                    "indexed_date": {"type": "date"}
                }
            }
        }
        self.es.indices.create(index=self.index_name, body=settings)

    def index_paper(self, paper_data):
        """Index a single paper entry into Elasticsearch"""
        try:
            # Ensure the paper has required fields
            if 'paperId' not in paper_data:
                raise ValueError("Missing required field: paperId")

            # Handle None values in document_content
            if paper_data.get('document_content') is None:
                paper_data['document_content'] = ""

            # Add indexing timestamp
            doc = {
                **paper_data,
                'indexed_date': datetime.now().isoformat()
            }

            # Index the document
            self.es.index(
                index=self.index_name,
                id=paper_data['paperId'],
                body=doc
            )
            print(f"Successfully indexed paper: {paper_data['paperId']}")
            
        except Exception as e:
            print(f"Error indexing paper {paper_data.get('paperId', 'unknown')}: {str(e)}")

    def index_papers_from_json(self, json_file_path):
        """Index multiple papers from a JSON file"""
        try:
            with open(json_file_path, 'r') as file:
                papers = json.load(file)
                
            if isinstance(papers, list):
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
                    
                    [print(x, str(y)[:10], end=", ") for x, y in temp_paper.items()]

                    self.index_paper(temp_paper)
            else:
                self.index_paper(papers)
                
        except Exception as e:
            print(f"Error reading JSON file: {str(e)}")

    def search(self, query, size=10, min_date=None, max_date=None, sort_by_date=False, min_score=None):
        """
        Search for papers based on content matching with the query
        
        Parameters:
        - query: search query string
        - size: number of results to return
        - min_date: minimum publication date (string in YYYY-MM-DD format)
        - max_date: maximum publication date (string in YYYY-MM-DD format)
        - sort_by_date: if True, sort results by publication date (newest first)
        - min_score: minimum relevance score to include in results
        """
        search_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["document_content^3", "title"],
                                "type": "best_fields",
                                "operator": "or",
                                "minimum_should_match": "75%"
                            }
                        }
                    ],
                    "filter": []
                }
            },
            "highlight": {
                "fields": {
                    "document_content": {
                        "fragment_size": 150,
                        "number_of_fragments": 3,
                        "pre_tags": ["<mark>"],
                        "post_tags": ["</mark>"]
                    }
                }
            }
        }

        # Add date range if specified
        if min_date or max_date:
            date_range = {"range": {"publicationDate": {}}}
            if min_date:
                date_range["range"]["publicationDate"]["gte"] = min_date
            if max_date:
                date_range["range"]["publicationDate"]["lte"] = max_date
            search_body["query"]["bool"]["filter"].append(date_range)

        # Add sorting if requested
        if sort_by_date:
            search_body["sort"] = [
                {"_score": "desc"},
                {"publicationDate": {"order": "desc"}}
            ]

        response = self.es.search(
            index=self.index_name,
            body=search_body,
            size=size
        )

        results = []
        for hit in response['hits']['hits']:
            # Skip results below minimum score if specified
            if min_score and hit['_score'] < min_score:
                continue
                
            result = {
                'paperId': hit['_source']['paperId'],
                'title': hit['_source']['title'],
                'publicationDate': hit['_source']['publicationDate'],
                'score': hit['_score'],
                'highlights': hit.get('highlight', {}).get('document_content', [])
            }
            results.append(result)

        return results

# # Example usage
# if __name__ == "__main__":
#     # Initialize the search system
#     search_system = PaperSearchSystem()

#     # Example paper entry
#     sample_paper = {
#         "paperId": "00f463bd1d89304afe540d0307418ef3325d21f2",
#         "title": "Enhancing Professional Employability: The Impact of Agile Methodology Training",
#         "document_content": "This paper explores the effectiveness of agile methodology training...",
#         "publicationDate": "2024-12-10"
#     }

#     # Index a single paper
#     search_system.index_paper(sample_paper)

#     # Search for papers with specific content
#     results = search_system.search(
#         "agile methodology effectiveness",
#         size=5,
#         min_score=0.5,
#         sort_by_date=True
#     )
    
#     # Print results with highlights
#     for result in results:
#         print(f"Paper ID: {result['paperId']}")
#         print(f"Title: {result['title']}")
#         print(f"Score: {result['score']}")
#         print("Relevant excerpts:")
#         for highlight in result['highlights']:
#             print(f"  - {highlight}")
#         print("---")

def get_paper_url(pdf_field):
    pdf_url = None
    if isinstance(pdf_field, dict) and "url" in pdf_field:
        pdf_url = pdf_field["url"]
    elif isinstance(pdf_field, str):
        pdf_url = pdf_field
    
    return pdf_url

if __name__ == "__main__":
    search_system = PaperSearchSystem()
        
    # search_system.index_papers_from_json('cs.jsonl')

    # Search for recent files
    # from datetime import datetime, timedelta
    # min_date = datetime.now() - timedelta(days=30)  # Last 30 days
    
    results = search_system.search(
        "social media"
        # min_date=min_date,
        # sort_by_date=True  # Sort by newest first
    )
    
    for result in results:
        print(result["highlights"])