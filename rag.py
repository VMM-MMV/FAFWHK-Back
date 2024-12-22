from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import ElasticsearchStore
from langchain.embeddings import OpenAIEmbeddings
from langchain.document_loaders import TextLoader
from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv

class DocumentRAG:
    def __init__(self, index_name="documents"):
        load_dotenv()

        es_key = os.getenv('ES_KEY')

        self.es_client = Elasticsearch(f"https://ce8f4aa8de214bfa80e5c5b25079d80e.europe-west3.gcp.cloud.es.io:443", api_key=es_key)
        self.index_name = index_name
        self.embeddings = OpenAIEmbeddings()
        self.vector_store = ElasticsearchStore(
            es_client=self.es_client,
            index_name=self.index_name,
            embedding=self.embeddings
        )
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200
        )

    def get_paper_content(self, paper_id):
        """
        Retrieve full paper content from Elasticsearch
        """
        try:
            response = self.es_client.get(
                index=self.index_name,
                id=paper_id
            )
            return response['_source']
        except Exception as e:
            raise ValueError(f"Error retrieving paper content: {str(e)}")

    def query_paper(self, query, paper_id, top_k=5):
        """
        Query paper content using vector similarity search
        """
        filter = {
            "term": {
                "metadata.paper_id": paper_id
            }
        }
        
        results = self.vector_store.similarity_search_with_score(
            query,
            k=top_k,
            filter=filter
        )
        
        return results

    def get_context_string(self, results, paper_content):
        """
        Create context string combining relevant passages and paper metadata
        """
        context = f"Paper Title: {paper_content.get('title', 'N/A')}\n"
        context += f"Authors: {', '.join(paper_content.get('authors', []))}\n"
        context += f"Publication Date: {paper_content.get('publication_date', 'N/A')}\n\n"
        context += "Relevant Passages:\n"
        
        for doc, score in results:
            context += f"\nPassage (similarity score {score:.4f}):\n{doc.page_content}\n"
        
        return context

def get_paper_info(paper_id, rag_system):
    """
    Get complete paper information including relevant context
    """
    try:
        # Get full paper content
        paper_content = rag_system.get_paper_content(paper_id)
        
        # Get paper sections and metadata
        sections = paper_content.get('sections', [])
        metadata = paper_content.get('metadata', {})
        
        # Combine all content for context
        full_content = {
            'title': paper_content.get('title'),
            'authors': paper_content.get('authors', []),
            'publication_date': paper_content.get('publication_date'),
            'sections': sections,
            'metadata': metadata
        }
        
        return full_content
        
    except Exception as e:
        raise ValueError(f"Error retrieving paper information: {str(e)}")

def main():
    rag = DocumentRAG()
    file_ids = []
    for file_path in ["doc1.txt", "doc2.txt", "doc3.txt"]:
        file_id = rag.ingest_file(file_path)
        file_ids.append(file_id)
    
    selected_file_id = file_ids[0]
    query = "What is the main topic discussed?"
    
    results = rag.query_file(query, selected_file_id)
    context = rag.get_context_string(results)
    
    print(f"Context from file {selected_file_id}:")
    print(context)