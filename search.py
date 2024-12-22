from elasticsearch import Elasticsearch
import os
import json
from util.pdf_manager import url_to_txt
from datetime import datetime
from dotenv import load_dotenv
from reco_algo import UserPaperInteractions, UserPreferences
import os

class PaperSearchSystem:
    def __init__(self, elastic_host='localhost', elastic_port=9200, index_name='papers'):
        """Initialize the search system with Elasticsearch connection and user preferences"""
        load_dotenv()
        es_key = os.getenv('ES_KEY')
        
        self.es = Elasticsearch(
            "https://ce8f4aa8de214bfa80e5c5b25079d80e.europe-west3.gcp.cloud.es.io:443",
            api_key=es_key
        )
        self.index_name = index_name
        self.user_preferences = UserPreferences()
        
        if not self.es.indices.exists(index=self.index_name):
            self.create_index()

    def create_index(self):
        """Create Elasticsearch index with appropriate mappings"""
        settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "paper_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "stop", "snowball"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "paperId": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "paper_analyzer"},
                    "document_content": {
                        "type": "text",
                        "analyzer": "paper_analyzer",
                        "term_vector": "with_positions_offsets"
                    },
                    "authorId": {"type": "keyword"},
                    "publicationDate": {"type": "date"},
                    "indexed_date": {"type": "date"}
                }
            }
        }
        self.es.indices.create(index=self.index_name, body=settings)

    def get_all(self, user_id=None, size=100, sort_by_date=None):
        """
        Retrieve papers with optional user personalization
        
        Parameters:
        - user_id: Optional user ID for personalized results
        - size: Number of results per batch
        - sort_by_date: "asc" or "desc" for date sorting
        """
        search_body = self._build_search_body(user_id, sort_by_date)
        
        results = []
        scroll_timeout = "2m"

        # Initial search
        response = self.es.search(
            index=self.index_name,
            body=search_body,
            size=size,
            scroll=scroll_timeout
        )
        
        scroll_id = response["_scroll_id"]
        results.extend(response['hits']['hits'])

        # Continue scrolling
        while len(response['hits']['hits']) > 0:
            response = self.es.scroll(
                scroll_id=scroll_id,
                scroll=scroll_timeout
            )
            scroll_id = response["_scroll_id"]
            results.extend(response['hits']['hits'])

        # Process and sort results
        processed_results = self._process_results(results, user_id)
        
        return processed_results

    def _build_search_body(self, user_id, sort_by_date):
        """Build Elasticsearch query based on user preferences"""
        search_body = {
            "query": {
                "bool": {
                    "must": [{"match_all": {}}],
                    "must_not": [],
                    "should": []
                }
            }
        }

        if user_id and user_id in self.user_preferences.user_data:
            user_prefs = self.user_preferences.user_data[user_id]

            # Exclude blocked authors
            blocked_authors = user_prefs['blocked_authors']
            if blocked_authors:
                search_body["query"]["bool"]["must_not"].append({
                    "terms": {"authorId": list(blocked_authors)}
                })

            # Boost papers from followed authors
            followed_authors = user_prefs['followed_authors']
            if followed_authors:
                search_body["query"]["bool"]["should"].append({
                    "terms": {"authorId": list(followed_authors), "boost": 2.0}
                })

            # Boost papers similar to "show_more"
            show_more = user_prefs['show_more']
            if show_more:
                search_body["query"]["bool"]["should"].append({
                    "more_like_this": {
                        "fields": ["title", "document_content"],
                        "like": [{"_index": self.index_name, "_id": pid} for pid in show_more],
                        "min_term_freq": 1,
                        "max_query_terms": 12,
                        "boost": 1.5
                    }
                })

            # Exclude papers similar to "show_less"
            show_less = user_prefs['show_less']
            if show_less:
                search_body["query"]["bool"]["must_not"].append({
                    "more_like_this": {
                        "fields": ["title", "document_content"],
                        "like": [{"_index": self.index_name, "_id": pid} for pid in show_less],
                        "min_term_freq": 1,
                        "max_query_terms": 12
                    }
                })

        # Add date sorting if requested
        if sort_by_date:
            search_body["sort"] = [{"publicationDate": {"order": sort_by_date}}]

        return search_body

    def _process_results(self, results, user_id):
        """Process and score search results"""
        processed_results = []
        
        for hit in results:
            source = hit['_source']
            result = {
                'paperId': source['paperId'],
                'title': source['title'],
                'publicationDate': source['publicationDate'],
                'authorId': source.get('authorId'),
                'base_score': hit['_score']
            }
            
            # Add user interaction data if available
            if user_id and user_id in self.user_preferences.user_data:
                user_prefs = self.user_preferences.user_data[user_id]
                paper_id = source['paperId']
                
                result.update({
                    'view_time': user_prefs['view_times'].get(paper_id, 0),
                    'view_count': user_prefs['view_counts'].get(paper_id, 0),
                    'is_bookmarked': paper_id in user_prefs['bookmarks'],
                    'is_downloaded': paper_id in user_prefs['downloads'],
                    'show_more': paper_id in user_prefs['show_more'],
                    'show_less': paper_id in user_prefs['show_less']
                })
                
                # Calculate user preference score
                score = result['base_score']
                score *= 1.5 if result['is_bookmarked'] else 1.0
                score *= 1.3 if result['is_downloaded'] else 1.0
                score *= 1.5 if result['show_more'] else 1.0
                score *= 0.5 if result['show_less'] else 1.0
                score *= 1.0 + (result['view_time'] / 3600)  # Boost based on hours spent reading
                
                result['final_score'] = score
            else:
                result['final_score'] = result['base_score']
            
            processed_results.append(result)

        # Sort by final score
        return sorted(processed_results, key=lambda x: x['final_score'], reverse=True)

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
     