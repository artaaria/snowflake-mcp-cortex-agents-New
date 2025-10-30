from fastapi import FastAPI
import os
import snowflake.connector
from mistralai.client import ChatClient
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

# ✅ Initialise FastAPI app
app = FastAPI()

# ✅ Initialise Mistral ChatClient
mistral_client = ChatClient(api_key=os.getenv("MISTRAL_API_KEY"))

# ✅ Snowflake connection helper
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

# ✅ Endpoint: Check Snowflake connectivity
@app.get("/check-snowflake")
def check_snowflake():
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_TIMESTAMP;")
        result = cursor.fetchone()
        return {"status": "connected", "timestamp": result}
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()

# ✅ Endpoint: Run SQL query
@app.get("/run-query")
def run_query(sql: str):
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        return {"data": cursor.fetchall()}
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()

# ✅ Endpoint: Ask Mistral AI
@app.post("/chat")
def chat(prompt: str):
    try:
        response = mistral_client.chat(
            model="mistral-medium",
            messages=[{"role": "user", "content": prompt}]
        )
        return {"reply": response.choices[0].message.content}
    except Exception as e:
        return {"error": str(e)}
