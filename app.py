from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
import fitz  # PyMuPDF for PDF text extraction
import os
import tempfile
from elasticsearch.helpers import bulk
from imagekitio import ImageKit
import groq
from pydantic import BaseModel
import requests
from fastapi import FastAPI
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from fastapi.middleware.cors import CORSMiddleware
import os
from datetime import datetime
from uuid import uuid4
from typing import List
from fastapi import HTTPException
from cassandra.query import SimpleStatement
from fastapi import FastAPI, HTTPException

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import uuid
from typing import List




# Replace with hardcoded values if the module is unavailable
astra_client_id = "sNmpeUqPgFtgxXQnmjczDsaI"  # Replace with your actual client ID
astra_client_secret = "+PlIZTTxCJA8DWfd39I7Y1vNsDMxTbRkWKkSvIrtA-H8U9rmldAnflhc,Ok6poD,E3Oy72yD6.UfZwxbr4QOmQMqli4jqbsjiL6JZIE31kztIYTbnhZjyCgPwoZdUfnN"  # Replace with your actual client secret
astra_database_id = "tecgium"  # Replace with your actual database ID

# ✅ Elasticsearch Configuration
ELASTICSEARCH_URL = "https://0a56ba4d59e5434ba03a49f61f6adca1.us-central1.gcp.cloud.es.io:443"
abcdef=10



API_KEY = "SmJ6T3JaWUJyTWFhb1duQTNWSGs6VzVsbjEtdjVxTzlyWUhOSGo3N1ZLUQ=="

# YVMzaGJwVUJsbWVLelFDNXhOTDM6bjZFVXRiRk1Eb0NJSGZiZkVabWRWUQ==
# https://my-elasticsearch-project-d4e765.es.us-east-1.aws.elastic.cloud:443

es = Elasticsearch(
    ELASTICSEARCH_URL,
    api_key=API_KEY
)
# ✅ Groq API Configuration
GROQ_API_KEY = "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"

# ✅ FastAPI App Initialization
app = FastAPI()

# ✅ CORS Configuration (Allow only frontend domain in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change "" to frontend domain in production
    allow_credentials=True,
    allow_methods=["*"],  # Allows all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# ✅ ImageKit Configuration
imagekit = ImageKit(
    private_key='private_lJZeBuXRen5WI4WpjNRjf1DZW4E=',
    public_key='public_djwqIa18ksHGZEGTJk59MFOp/mA=',
    url_endpoint='https://ik.imagekit.io/46k1lkvq2'
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


def connect_to_cassandra():
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-tecgium.zip'  # Path to your secure connect bundle
    }
    cluster = Cluster(cloud=cloud_config, auth_provider=PlainTextAuthProvider(astra_client_id, astra_client_secret))
    session = cluster.connect()
    session.set_keyspace("tech")
    return session

# Cassandra session
session = connect_to_cassandra()

# --- Pydantic model ---
class LLMResponse(BaseModel):
    llm_response: str

# To store all responses
all_responses = []

# def upload_to_imagekit(file_path, file_name):
#     """Uploads a file to ImageKit.io and returns the file URL."""
#     try:
#         with open(file_path, "rb") as file:
#             response = imagekit.upload(
#                 file=file,
#                 file_name=file_name
#             )
#         return response.get("url")  # Corrected response handling
#     except Exception as e:
#         print("Error uploading to ImageKit:", e)
#         return None


def upload_to_imagekit(file_path, file_name):
    try:
        with open(file_path, "rb") as file:
            response = imagekit.upload(
                file=file,
                file_name=file_name
            )

        print("Upload Response Type:", type(response))  # Debugging
        print("Upload Response Data:", response._dict_)  # Print all properties
        
        return response.url  # Correctly access the URL

    except Exception as e:
        print("Error uploading to ImageKit:", str(e))
        return None

def process_and_store(pdf_path):
    """Extracts text from PDF and stores in Elasticsearch."""
    try:
        pdf_document = fitz.open(pdf_path)
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]

        pdf_cdn_link = upload_to_imagekit(pdf_path, pdf_name + ".pdf")
        if not pdf_cdn_link:
            return None

        # ✅ Extract text & store each page separately in Elasticsearch
        actions = []
        for page_num in range(len(pdf_document)):
            page = pdf_document[page_num]
            page_text = page.get_text("text").strip()
            page_link = f"{pdf_cdn_link}#page={page_num + 1}"

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

    except Exception as e:
        print("Error processing PDF:", e)
        return None

# @app.post("/upload")
# async def upload_pdf(file: UploadFile = File(...)):
#     """Endpoint to upload a PDF, extract text, and store in Elasticsearch."""
#     try:
#         pdf_name = os.path.splitext(file.filename)[0]

#         with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
#             temp_pdf.write(file.file.read())
#             temp_pdf_path = temp_pdf.name

#         pdf_url = process_and_store(temp_pdf_path)

#         if pdf_url:
#             return {"message": "PDF uploaded successfully", "pdf_url": pdf_url, "pdf_name": pdf_name}
#         else:
#             return {"error": "Upload failed"}

#     except Exception as e:
#         return {"error": str(e)}


@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    """Handles PDF upload, processes text, stores in Elasticsearch, and uploads to ImageKit."""
    try:
        # ✅ Save temporarily
        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, file.filename)

        with open(temp_file_path, "wb") as buffer:
            buffer.write(await file.read())

        # ✅ Process & Upload
        pdf_cdn_link = process_and_store(temp_file_path)
        if not pdf_cdn_link:
            return {"success": False, "message": "Failed to upload to ImageKit"}

        return {"success": True, "message": "File uploaded successfully", "cdn_link": pdf_cdn_link}

    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}

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
    try:
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
    except Exception as e:
        return {"error": str(e)}

def generate_response(knowledge_base, user_query):
    """Generates response using retrieved knowledge and Groq LLM."""
    if not knowledge_base:
        return "No relevant information found. Please refine your query."

    context = "\n".join([
        f"📄 Page: {kb['page_number']} | 🖼 Image: {kb['imagekit_link']}\n🔹 {kb['page_content'][:300]}..."
        for kb in knowledge_base[:3]
    ])

    prompt = f"""
    You are an AI assistant specializing in technical knowledge retrieval.
    Answer the user's query based on the provided database.

    Knowledge Base:
    {context}

    User Query: {user_query}

    Provide a concise, accurate response using the knowledge base and mention the relevant page numbers.
    """

    client = groq.Client(api_key=GROQ_API_KEY)

    try:
        response = client.chat.completions.create(
            model="llama3-70b-8192",
            messages=[{"role": "system", "content": "Answer using the given knowledge base."},
                      {"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content if response.choices else "No response generated."

    except Exception as e:
        print(f"Error generating response: {e}")
        return "An error occurred while generating a response."

class QueryRequest(BaseModel):
    query: str

# @app.get("/list_pdfs")
# def list_pdfs():
#     """Retrieve all PDFs stored in Elasticsearch."""
#     try:
#         search_body = {
#             "size": 1000,
#             "query": {
#                 "match_all": {}
#             }
#         }
#         response = es.search(index=INDEX_NAME, body=search_body)

#         pdf_data = {}
#         for hit in response["hits"]["hits"]:
#             pdf_name = hit["_source"]["pdf_name"]
#             if pdf_name not in pdf_data:
#                 pdf_data[pdf_name] = {
#                     "pdf_name": pdf_name,
#                     "num_pages": 0,
#                     "imagekit_link": hit["_source"]["imagekit_link"]
#                 }
#             pdf_data[pdf_name]["num_pages"] += 1

#         return {"results": list(pdf_data.values())}

#     except Exception as e:
#         return {"error": str(e)}

# @app.delete("/delete_pdf/{pdf_name}")
# def delete_pdf(pdf_name: str):
#     """Delete all pages of a PDF from Elasticsearch."""
#     try:
#         delete_body = {
#             "query": {
#                 "match": {
#                     "pdf_name": pdf_name
#                 }
#             }
#         }
#         response = es.delete_by_query(index=INDEX_NAME, body=delete_body)
        
#         if response["deleted"] > 0:
#             return {"message": f"Deleted PDF: {pdf_name}"}
#         return {"error": "PDF not found"}

#     except Exception as e:
#         return {"error": str(e)}

@app.get("/stats")
async def get_pdf_stats():
    """Fetch total number of PDFs and total pages stored."""
    try:
        response = es.search(index=INDEX_NAME, body={
            "size": 0,  # Don't return documents, just aggregations
            "aggs": {
                "unique_pdfs": {
                    "cardinality": {
                        "field": "imagekit_link.keyword"
                    }
                },
                "total_pages": {
                    "value_count": {
                        "field": "page_number"
                    }
                }
            }
        })

        return {
            "total_pdfs": response["aggregations"]["unique_pdfs"]["value"],
            "total_pages": response["aggregations"]["total_pages"]["value"]
        }

    except Exception as e:
        return {"error": str(e)}

  


@app.get("/list_pdfs")
async def list_pdfs():
    """Retrieve all PDFs with links to page 1."""
    try:
        response = es.search(index=INDEX_NAME, body={
            "size": 1000,
            "query": {
                "match": {"page_number": 1}  # Only fetch page 1 entries
            }
        })

        results = [
            {
                "pdf_name": hit["_source"]["pdf_name"],
                "page_link": hit["_source"]["imagekit_link"]
            }
            for hit in response["hits"]["hits"]
        ]

        return {"documents": results}

    except Exception as e:
        return {"error": str(e)}


@app.delete("/delete_pdf")
async def delete_pdf(pdf_name: str):
    """Delete a PDF and all its pages from Elasticsearch."""
    try:
        response = es.delete_by_query(index=INDEX_NAME, body={
            "query": {
                "match": {"pdf_name": pdf_name}
            }
        })

        return {"message": f"Deleted {response['deleted']} pages from {pdf_name}"}

    except Exception as e:
        return {"error": str(e)}


@app.post("/llm")
async def llm_query(request: QueryRequest):
    """Retrieve top search result and generate response using LLM."""
    
    user_query = request.query
    search_results = search_pdfs(query=user_query)
    
    if "results" in search_results and search_results["results"]:
        llm_response = generate_response(search_results["results"], user_query)
        return {"results": search_results["results"], "llm_response": llm_response}

    return {"results": [], "llm_response": "No relevant information found."}








current_user = None
current_login_time = None

# === MODELS ===
class LoginData(BaseModel):
    login_id: str
    password: str

class LLMResponse(BaseModel):
    llm_response: str
    query: str


# === USER LOGIN ===
@app.post("/user_login")
async def user_login(data: LoginData):
    global current_user, current_login_time
    try:
        check_user = session.execute(
            "SELECT password FROM user_details WHERE username=%s",
            (data.login_id,)
        ).one()

        if check_user and check_user.password == data.password:
            current_user = data.login_id
            current_login_time = datetime.utcnow()

            return {
                "success": True
            }

        return {"success": False, "error": "Invalid credentials"}

    except Exception as e:
        print("Login error:", e)
        raise HTTPException(status_code=500, detail="Internal server error")



class LLMResponse(BaseModel):
    llm_response: str
    query: str

# Endpoint to receive LLM response and store in Cassandra
@app.post("/receive_llm_response")
async def receive_llm_response(data: LLMResponse):
    global current_user

    # Ensure user is logged in (current_user should not be None or empty)
    if not current_user:
        raise HTTPException(status_code=403, detail="User not logged in")

    try:
        # Store the response in the all_responses list for temporary storage
        all_responses.append(data.llm_response)

        # Generate UUID for dummy_id
        dummy_id = uuid4()

        # Ensure current_user's table exists (check for existence)
        # Replace the table name dynamically with current_user's table
        table_name = f"{current_user}_log"

        # Ensure the query structure matches the table schema
        query = f"""
        INSERT INTO {table_name} (dummy_id, llm_response, queries)
        VALUES (%s, %s, %s)
        """
        
        # Execute the query with the required values
        session.execute(query, (dummy_id, data.llm_response, data.query))

        return {"message": "LLM response and query recorded."}

    except Exception as e:
        print("LLM log error:", e)
        raise HTTPException(status_code=500, detail="Failed to insert LLM response")




class LLMesponse(BaseModel):
    llm_response: str  # Only llm_response field


@app.post("/get_user_bookmarks", response_model=List[LLMesponse])
def get_user_bookmarks():
    global current_user

    # Check if the user is logged in
    if not current_user:
        raise HTTPException(status_code=403, detail="User not logged in")
    
    # Execute query to fetch llm_response data
    try:
        rows = session.execute(f"SELECT llm_response FROM {current_user}_log;")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Prepare the result
    result = [{"llm_response": row.llm_response} for row in rows]

    # Return the result
    return result

class querysponse(BaseModel):
    queries: str

@app.post("/get_user_queries", response_model=List[querysponse])
def get_user_queries():
    global current_user

    # Check if the user is logged in
    if not current_user:
        raise HTTPException(status_code=403, detail="User not logged in")
    
    # Execute query to fetch only queries (without llm_response)
    try:
        rows = session.execute(f"SELECT queries FROM {current_user}_log;")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Prepare the result
    result = [{"queries": row.queries} for row in rows]

    # Return the result
    return result
class ContactUsData(BaseModel):
    first_name: str
    last_name: str
    email: str
    message: str

@app.post("/submit_contact")
async def submit_contact(data: ContactUsData):
    try:
        insert_query = SimpleStatement("""
            INSERT INTO contact_us (id, first_name, last_name, email, message)
            VALUES (%s, %s, %s, %s, %s)
        """)
        session.execute(insert_query, (
            uuid.uuid4(),
            data.first_name,
            data.last_name,
            data.email,
            data.message
        ))
        return {"message": "Contact form submitted successfully"}
    except Exception as e:
        print("🔥 Contact submission error:", str(e))  # ADD THIS LINE
        raise HTTPException(status_code=500, detail=str(e))

class ContactUsResponse(BaseModel):
    id: str
    first_name: str
    last_name: str
    email: str
    message: str

@app.get("/get_contact_messages", response_model=List[ContactUsResponse])
async def get_contact_messages():
    try:
        query = "SELECT id, first_name, last_name, email, message FROM contact_us"
        rows = session.execute(query)

        messages = []
        for row in rows:
            messages.append({
                "id": str(row.id),
                "first_name": row.first_name,
                "last_name": row.last_name,
                "email": row.email,
                "message": row.message
            })

        return messages

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching contact messages: {str(e)}")

@app.get("/get_ticket_count")
async def get_ticket_count():
    try:
        # Query to count the rows in the contact_us table
        count_query = "SELECT COUNT(*) FROM contact_us"
        result = session.execute(count_query)
        count = result.one()[0]  # Extract the count from the result
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching ticket count: {str(e)}")
    

@app.get("/get_user_count")
async def get_user_count():
    try:
        # Query to count the rows in the contact_us table
        count_query = "SELECT COUNT(*) FROM user_details"
        result = session.execute(count_query)
        count = result.one()[0]  # Extract the count from the result
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching ticket count: {str(e)}")


class QueryText(BaseModel):
    query_text: str

@app.post("/delete_bookmark_by_text")
async def delete_bookmark_by_text(payload: QueryText, request: Request):
    global current_user

    # 1. Check if the user is logged in
    if not current_user:
        raise HTTPException(status_code=403, detail="User not logged in")

    try:
        # 2. Log the input for debug
        body = await request.json()
        print("Received body:", body)

        query_text = payload.query_text
        print("Query text:", query_text)

        # 3. Sanitize table name
        table_name = f"{current_user}_log"
        if not table_name.replace("_", "").isalnum():
            raise HTTPException(status_code=400, detail="Invalid table name")


        # 5. Execute delete safely
        session.execute(f"DELETE FROM {table_name} WHERE llm_response = %s", (query_text,))

        return JSONResponse(
            content={"message": f"Bookmark with query '{query_text}' deleted successfully"},
            headers={
                "x-content-type-options": "nosniff",
                "cache-control": "no-store, no-cache, must-revalidate, proxy-revalidate"
            }
        )

    except Exception as e:
        print("Error:", str(e))
        return JSONResponse(
            content={"message": f"Failed to delete bookmark: {str(e)}"},
            headers={
                "x-content-type-options": "nosniff",
                "cache-control": "no-store, no-cache, must-revalidate, proxy-revalidate"
            }
        )

if __name__ == "main":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
