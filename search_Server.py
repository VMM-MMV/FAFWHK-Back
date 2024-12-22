from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from datetime import datetime
from dotenv import load_dotenv
import os
from marshmallow import Schema, fields, validate, ValidationError
from LLMGroq import LLMGroq
from papers import get_paper_info

app = Flask(__name__)

# Load environment variables
load_dotenv()
es_key = os.getenv('ES_KEY')

# Validation Schemas
class SearchSchema(Schema):
    query = fields.Str(required=True, validate=validate.Length(min=1))
    size = fields.Int(missing=10, validate=validate.Range(min=1, max=100))
    min_date = fields.Date(format='%Y-%m-%d', missing=None)
    max_date = fields.Date(format='%Y-%m-%d', missing=None)
    sort_by_date = fields.Str(missing=None, validate=validate.OneOf(['asc', 'desc', None]))
    min_score = fields.Float(missing=None, validate=validate.Range(min=0))

class GetAllSchema(Schema):
    size = fields.Int(missing=100, validate=validate.Range(min=1, max=1000))
    sort_by_date = fields.Str(missing=None, validate=validate.OneOf(['asc', 'desc', None]))

class PaperSearchSystem:
    def __init__(self, elastic_host='localhost', elastic_port=9200, index_name='papers'):
        """Initialize the search system with Elasticsearch connection"""
        self.es = Elasticsearch(
            f"https://ce8f4aa8de214bfa80e5c5b25079d80e.europe-west3.gcp.cloud.es.io:443",
            api_key=es_key
        )
        self.index_name = index_name
        
        # Create index if it doesn't exist
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

    def search(self, query, size=10, min_date=None, max_date=None, sort_by_date=None, min_score=None):
        """Search for papers based on content matching"""
        search_body = {
            "query": {
                "bool": {
                    "must": [{
                        "multi_match": {
                            "query": query,
                            "fields": ["document_content^3", "title"],
                            "type": "best_fields",
                            "operator": "or",
                            "minimum_should_match": "75%"
                        }
                    }],
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

        if min_date or max_date:
            date_range = {"range": {"publicationDate": {}}}
            if min_date:
                date_range["range"]["publicationDate"]["gte"] = min_date
            if max_date:
                date_range["range"]["publicationDate"]["lte"] = max_date
            search_body["query"]["bool"]["filter"].append(date_range)

        if sort_by_date:
            search_body["sort"] = [{"publicationDate": {"order": sort_by_date}}]

        response = self.es.search(index=self.index_name, body=search_body, size=size)

        results = []
        for hit in response['hits']['hits']:
            if min_score and hit['_score'] < min_score:
                continue
            results.append({
                'paperId': hit['_source']['paperId'],
                'title': hit['_source']['title'],
                'publicationDate': hit['_source']['publicationDate'],
                'score': hit['_score'],
                'highlights': hit.get('highlight', {}).get('document_content', [])
            })

        return results

    def get_all(self, size=100, sort_by_date="desc"):
        """Retrieve all papers from the index"""
        search_body = {"query": {"match_all": {}}}

        if sort_by_date:
            search_body["sort"] = [{"publicationDate": {"order": sort_by_date}}]

        results = []
        scroll_timeout = "2m"

        response = self.es.search(
            index=self.index_name,
            body=search_body,
            size=size,
            scroll=scroll_timeout
        )
        
        scroll_id = response["_scroll_id"]
        results.extend(response['hits']['hits'])

        while len(response['hits']['hits']) > 0:
            response = self.es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
            scroll_id = response["_scroll_id"]
            results.extend(response['hits']['hits'])

        return [{
            'paperId': hit['_source']['paperId'],
            'title': hit['_source']['title'],
            'publicationDate': hit['_source']['publicationDate']
        } for hit in results]

# Initialize search system
search_system = PaperSearchSystem()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/search', methods=['POST'])
def search():
    """Search papers endpoint"""
    try:
        # Validate request data
        schema = SearchSchema()
        data = schema.load(request.json)
        
        # Perform search
        results = search_system.search(
            query=data['query'],
            size=data['size'],
            min_date=data['min_date'],
            max_date=data['max_date'],
            sort_by_date=data['sort_by_date'],
            min_score=data['min_score']
        )
        
        return jsonify({
            "status": "success",
            "count": len(results),
            "results": results
        })
        
    except ValidationError as err:
        return jsonify({
            "status": "error",
            "message": "Validation error",
            "errors": err.messages
        }), 400
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/papers', methods=['GET'])
def get_all_papers():
    """Get all papers endpoint"""
    try:
        # Validate query parameters
        schema = GetAllSchema()
        data = schema.load(request.args)
        
        # Get papers
        results = search_system.get_all(
            size=data['size'],
            sort_by_date=data['sort_by_date']
        )
        
        return jsonify({
            "status": "success",
            "count": len(results),
            "results": results
        })
        
    except ValidationError as err:
        return jsonify({
            "status": "error",
            "message": "Validation error",
            "errors": err.messages
        }), 400
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    
@app.route('/ask-paper-question', methods=['POST'])
def ask_paper_question():
    try:
        data = request.get_json()

        paper_id = data.get('paper_id')
        user_question = data.get('question', "")

        if not paper_id:
            return jsonify({"error": "Missing paper_id"}), 400

        paper_info = get_paper_info(paper_id)
        
        llm = LLMGroq()
        prompt = "You are a helpful AI assistant. You will be given a research paper, and a question on it, respond."
        res = llm.query(prompt + user_question + str(paper_info))
        
        return jsonify({"response": res})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Run Flask app on port 8443
    app.run(host='0.0.0.0', port=8443, debug=True)
