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
                papers = [json.loads(line) for line in file]
                
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

    def search(self, query, size=10, min_date=None, max_date=None, sort_by_date=None, min_score=None):
        """
        Search for papers based on content matching with the query.

        Parameters:
        - query: search query string
        - size: number of results to return
        - min_date: minimum publication date (string in YYYY-MM-DD format)
        - max_date: maximum publication date (string in YYYY-MM-DD format)
        - sort_by_date: "asc" for oldest first, "desc" for newest first, or None for relevance.
        - min_score: minimum relevance score to include in results.
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

        # Add date range filter if specified
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
                {"publicationDate": {"order": sort_by_date}}
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
    
    def get_all(self, size=100, sort_by_date=None):
        """
        Retrieve all papers from the Elasticsearch index.

        Parameters:
        - size: number of results to fetch in each batch (default: 100).
        - sort_by_date: "asc" for oldest first, "desc" for newest first, or None for no sorting.
        """
        search_body = {
            "query": {
                "match_all": {}
            }
        }

        # Add sorting if requested
        if sort_by_date:
            search_body["sort"] = [
                {"publicationDate": {"order": sort_by_date}}
            ]

        results = []
        scroll_timeout = "2m"

        # Start scroll search
        response = self.es.search(
            index=self.index_name,
            body=search_body,
            size=size,
            scroll=scroll_timeout
        )
        scroll_id = response["_scroll_id"]

        # Collect results from the initial response
        results.extend(response['hits']['hits'])

        # Continue scrolling until no more hits
        while len(response['hits']['hits']) > 0:
            response = self.es.scroll(
                scroll_id=scroll_id,
                scroll=scroll_timeout
            )
            scroll_id = response["_scroll_id"]
            results.extend(response['hits']['hits'])

        # Transform results into a readable format
        return [
            {
                'paperId': hit['_source']['paperId'],
                'title': hit['_source']['title'],
                'publicationDate': hit['_source']['publicationDate']
            }
            for hit in results
        ]


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
    from datetime import datetime, timedelta
    min_date = datetime.now() - timedelta(days=30)  # Last 30 days
    
    # results = search_system.search(
    #     "some papers on artificial intelligence"
    #     # min_date=min_date,
    #     # sort_by_date=True  # Sort by newest first
    # )

    results = search_system.get_all(sort_by_date="desc")
    
    for result in results:
        print(result)
     