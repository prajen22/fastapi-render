from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
import fitz  # PyMuPDF for PDF text extraction
import os
import tempfile
from elasticsearch.helpers import bulk
from imagekitio import ImageKit


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
    allow_origins=["*"],  # Change this for security (use frontend domain)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ ImageKit Configuration
imagekit = ImageKit(
    private_key="YOUR_PRIVATE_KEY",
    public_key="YOUR_PUBLIC_KEY",
    url_endpoint="YOUR_URL_ENDPOINT"
)

INDEX_NAME = "pdf_documents"

# ✅ Create Elasticsearch Index (if not exists)
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                "properties": {
                    "pdf_name": {"type": "text"},
                    "page_number": {"type": "integer"},
                    "page_content": {"type": "text"},
                    "imagekit_link": {"type": "keyword"}
                }
            }
        }
    )

def upload_to_imagekit(file_path, file_name):
    """Uploads a file to ImageKit.io and returns the file URL."""
    try:
        with open(file_path, "rb") as file:
            response = imagekit.upload(
                file=file,
                file_name=file_name
            )
        return response.url if hasattr(response, "url") else None
    except Exception as e:
        print("Error uploading to ImageKit:", e)
        return None


def process_and_store(pdf_path):
    """Uploads the PDF to ImageKit, extracts text, and stores data in Elasticsearch."""
    pdf_document = fitz.open(pdf_path)
    pdf_name = os.path.basename(pdf_path)
    
    # ✅ Upload full PDF to ImageKit
    pdf_cdn_link = upload_to_imagekit(pdf_path, pdf_name)
    if not pdf_cdn_link:
        return None

    # ✅ Extract text & store each page separately in Elasticsearch
    actions = []
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        page_text = page.get_text("text").strip()
        page_link = f"{pdf_cdn_link}#page={page_num + 1}"

        # ✅ Prepare bulk insert action
        actions.append({
            "_index": INDEX_NAME,
            "_source": {
                "pdf_name": pdf_name,
                "page_number": page_num + 1,
                "page_content": page_text,
                "imagekit_link": page_link
            }
        })

    bulk(es, actions)  # ✅ Bulk insert all pages at once
    return pdf_cdn_link

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    """Endpoint to upload a PDF, extract text, and store in Elasticsearch."""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
            temp_pdf.write(file.file.read())
            temp_pdf_path = temp_pdf.name

        pdf_url = process_and_store(temp_pdf_path)

        if pdf_url:
            return {"message": "PDF uploaded successfully", "pdf_url": pdf_url}
        else:
            return {"error": "Upload failed"}

    except Exception as e:
        return {"error": str(e)}

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
