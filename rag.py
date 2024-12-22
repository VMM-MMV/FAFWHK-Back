from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import ElasticsearchStore
from langchain.embeddings import OpenAIEmbeddings
from langchain.document_loaders import TextLoader
from elasticsearch import Elasticsearch
import os

class DocumentRAG:
    def __init__(self, elasticsearch_url, index_name="documents"):
        # Initialize Elasticsearch client
        self.es_client = Elasticsearch(elasticsearch_url)
        self.index_name = index_name
        
        # Initialize embeddings
        self.embeddings = OpenAIEmbeddings()
        
        # Initialize vector store
        self.vector_store = ElasticsearchStore(
            es_client=self.es_client,
            index_name=self.index_name,
            embedding=self.embeddings
        )
        
        # Initialize text splitter
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200
        )

    def ingest_file(self, file_path):
        """
        Ingest a file into Elasticsearch with metadata
        """
        # Load the document
        loader = TextLoader(file_path)
        document = loader.load()
        
        # Split the document into chunks
        splits = self.text_splitter.split_documents(document)
        
        # Add file_id metadata to each split
        file_id = os.path.basename(file_path)
        for split in splits:
            split.metadata["file_id"] = file_id
        
        # Add to vector store
        self.vector_store.add_documents(splits)
        
        return file_id

    def query_file(self, query, file_id, top_k=5):
        """
        Query a specific file using metadata filtering
        """
        filter = {
            "term": {
                "metadata.file_id": file_id
            }
        }
        
        results = self.vector_store.similarity_search_with_score(
            query,
            k=top_k,
            filter=filter
        )
        
        return results

    def get_context_string(self, results):
        """
        Convert search results into a context string for the LLM
        """
        context = ""
        for doc, score in results:
            context += f"\nPassage (similarity score {score:.4f}):\n{doc.page_content}\n"
        return context

def main():
    rag = DocumentRAG(
        elasticsearch_url="http://localhost:9200"
    )

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