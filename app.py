from fastapi import FastAPI
import os
import snowflake.connector
from mistralai.client import MistralClient
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

# ✅ Initialise FastAPI app
app = FastAPI()

# ✅ Initialise Mistral client
mistral_client = MistralClient(api_key=os.getenv("MISTRAL_API_KEY"))

# ✅ Snowflake connection helper
def test_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_TIMESTAMP;")
    return cursor.fetchone()

# ✅ Endpoint: Check Snowflake connectivity
@app.get("/check-snowflake")
def check_snowflake():
    result = test_connection()
    return {"status": "connected", "timestamp": result}

# ✅ Endpoint: Run SQL query
@app.get("/run-query")
def run_query(sql: str):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    return {"data": cursor.fetchall()}

# ✅ Endpoint: Ask Mistral AI
@app.post("/chat")
def chat(prompt: str):
    response = mistral_client.chat.complete(
        model="mistral-medium",
        messages=[{"role": "user", "content": prompt}]
    )
    return {"reply": response.choices[0].message.content}
``
