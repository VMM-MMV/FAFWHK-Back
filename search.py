from elasticsearch import Elasticsearch, helpers
import os
import hashlib
import base64

class ElasticDocumentSearch:
    def __init__(self, elastic_host='localhost', elastic_port=9200, index_name='documents'):
        """
        Initialize Elasticsearch document search system.
        
        Args:
            elastic_host (str): Elasticsearch host
            elastic_port (int): Elasticsearch port
            index_name (str): Name of the index to store documents
        """
        self.es = Elasticsearch([{'host': elastic_host, 'port': elastic_port, 'scheme': 'http'}])
        self.index_name = index_name
        self._create_index()
    
    def _create_index(self):
        """Create the Elasticsearch index with appropriate mappings."""
        if not self.es.indices.exists(index=self.index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "content": {
                            "type": "text",
                            "analyzer": "standard"
                        },
                        "file_path": {
                            "type": "keyword"
                        },
                        "file_name": {
                            "type": "keyword"
                        },
                        "file_extension": {
                            "type": "keyword"
                        },
                        "content_hash": {
                            "type": "keyword"
                        }
                    }
                },
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    }
                }
            }
            self.es.indices.create(index=self.index_name, body=mapping)

    def index_directory(self, directory_path, batch_size=100):
        """
        Index all files in the specified directory.
        
        Args:
            directory_path (str): Path to directory containing files
            batch_size (int): Number of documents to index in each batch
        """
        def generate_documents():
            for root, _, files in os.walk(directory_path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    try:
                        # Get file metadata
                        stat = os.stat(file_path)
                        file_extension = os.path.splitext(filename)[1]
                        
                        # Read file content
                        with open(file_path, 'rb') as f:
                            content = f.read()
                            
                        # Create content hash for deduplication
                        content_hash = hashlib.md5(content).hexdigest()
                        
                        # Convert binary content to text if possible
                        try:
                            content = content.decode('utf-8')
                        except UnicodeDecodeError:
                            # If we can't decode as text, encode as base64
                            content = base64.b64encode(content).decode('utf-8')
                        
                        yield {
                            "_index": self.index_name,
                            "_id": content_hash,
                            "_source": {
                                "content": content,
                                "file_path": file_path,
                                "file_name": filename,
                                "file_extension": file_extension,
                                "file_size": stat.st_size,
                                "last_modified": stat.st_mtime,
                                "content_hash": content_hash
                            }
                        }
                    except Exception as e:
                        print(f"Error processing file {file_path}: {str(e)}")

        # Bulk index the documents
        try:
            success, failed = helpers.bulk(
                self.es,
                generate_documents(),
                chunk_size=batch_size,
                raise_on_error=False
            )
            print(f"Indexed {success} documents successfully. {len(failed)} failed.")
        except Exception as e:
            print(f"Error during bulk indexing: {str(e)}")

    def search(self, query, top_n=5):
        """
        Search for documents matching the query.
        
        Args:
            query (str): Search query
            top_n (int): Number of top results to return
            
        Returns:
            list: List of dictionaries containing search results
        """
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["content^2", "file_name"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            "highlight": {
                "fields": {
                    "content": {
                        "fragment_size": 150,
                        "number_of_fragments": 1
                    }
                }
            },
            "_source": ["file_path", "file_name", "file_size", "last_modified"],
            "size": top_n
        }

        try:
            response = self.es.search(index=self.index_name, body=search_body)
            results = []
            
            for hit in response['hits']['hits']:
                result = {
                    'file_path': hit['_source']['file_path'],
                    'file_name': hit['_source']['file_name'],
                    'score': round(hit['_score'], 2),
                    'size': hit['_source']['file_size'],
                    'last_modified': hit['_source']['last_modified']
                }
                
                # Add highlight if available
                if 'highlight' in hit:
                    result['highlight'] = hit['highlight']['content'][0]
                
                results.append(result)
            
            return results
        
        except Exception as e:
            print(f"Search error: {str(e)}")
            return []

    def delete_index(self):
        """Delete the Elasticsearch index."""
        try:
            self.es.indices.delete(index=self.index_name)
            print(f"Index '{self.index_name}' deleted successfully.")
        except Exception as e:
            print(f"Error deleting index: {str(e)}")

def demo_search():
    """Demo function showing how to use the ElasticDocumentSearch class."""
    # Initialize the search system
    search_system = ElasticDocumentSearch()
    
    # Index documents from a directory
    search_system.index_directory("/path/to/your/documents")
    
    # Perform a search
    query = "your search query here"
    results = search_system.search(query)
    
    # Print results
    print(f"\nSearch results for: '{query}'")
    print("-" * 50)
    for result in results:
        print(f"File: {result['file_name']}")
        print(f"Path: {result['file_path']}")
        print(f"Score: {result['score']}")
        if 'highlight' in result:
            print(f"Preview: {result['highlight']}")
        print("-" * 50)

if __name__ == "__main__":
    demo_search()