from typing import Dict
import os
import uuid
import httpx
import asyncio
from dotenv import load_dotenv

from mistralai.client import MistralClient
import os
import snowflake.connector
from fastapi import FastAPI
app = FastAPI()

# ✅ Load environment variables
load_dotenv()

# ✅ Initialise MCP server
mcp = FastMCP()
port = int(os.getenv("PORT", 8000))

# ✅ Initialise Mistral client
mistral_client = MistralClient(api_key=os.getenv("MISTRAL_API_KEY"))

# ✅ Snowflake API constants
SNOWFLAKE_ACCOUNT_URL = os.getenv("SNOWFLAKE_ACCOUNT_URL")
SNOWFLAKE_PAT = os.getenv("SNOWFLAKE_PAT")


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

API_HEADERS = {
    "Authorization": f"Bearer {SNOWFLAKE_PAT}",
    "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
    "Content-Type": "application/json",
}


@app.get("/check-snowflake")
def check_snowflake():
    result = test_connection()
    return {"status": "connected", "timestamp": result}

# ✅ Tool: Ask Mistral LLM
@mcp.tool()
async def ask_mistral(prompt: str) -> Dict:
    """Call Mistral LLM for conversational AI"""
    response = mistral_client.chat.complete(
        model="mistral-medium",
        messages=[{"role": "user", "content": prompt}]
    )
    return {"response": response.choices[0].message.content}

# ✅ Tool: Run Snowflake Cortex Agent

    headers = {**API_HEADERS, "Accept": "text/event-stream"}
    request_id = str(uuid.uuid4())

    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream(
            "POST",
            f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/cortex/agent:run",
            json=payload,
            headers=headers,
            params={"requestId": request_id}
        ) as resp:
            resp.raise_for_status()
            text, sql, citations = await process_sse_response(resp)

    results = await execute_sql(sql) if sql else None
    return {"text": text, "sql": sql, "results": results}

# ✅ Start MCP server
if __name__ == "__main__":
    mcp.run(transport="http", port=port)
