from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch

# ✅ Elasticsearch Configuration
ELASTICSEARCH_URL = "https://e4d509b4d8fb49a78a19a571c1b65bba.us-central1.gcp.cloud.es.io:443"
API_KEY = "dHlnVEtaVUJCYWNWcEcwczVQcE46d2tOTURWLXBUSmFvQkg1bmxma1VkQQ=="  # Replace with actual API key

es = Elasticsearch(
    ELASTICSEARCH_URL,
    api_key=API_KEY
)

app = FastAPI()

# ✅ CORS Configuration for React Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Allow frontend requests
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

INDEX_NAME = "pdf_documents"

@app.get("/")
def home():
    return {"message": "Welcome to FastAPI with Elasticsearch"}

@app.get("/search")
def search_pdfs(query: str = Query(..., description="Search query")):
    """Search PDFs stored in Elasticsearch based on a query."""
    search_body = {
        "query": {
            "match": {
                "page_content": query
            }
        }
    }

    response = es.search(index=INDEX_NAME, body=search_body)

    results = [
        {
            "pdf_name": hit["_source"]["pdf_name"],
            "page_number": hit["_source"]["page_number"],
            "page_content": hit["_source"]["page_content"],
            "imagekit_link": hit["_source"]["imagekit_link"],
        }
        for hit in response["hits"]["hits"]
    ]

    return {"results": results}


# ✅ Run FastAPI Server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
