from typing import Any, Dict, Tuple, List, Optional
import httpx
import os
import json
import uuid
from dotenv import load_dotenv, find_dotenv
import asyncio
load_dotenv(find_dotenv())

from fastapi import FastAPI
import snowflake.connector
from fastmcp import FastMCP

# Initialize MCP and FastAPI
mcp = FastMCP("cortex_agent")
app = FastAPI()

# ✅ Add your Snowflake insert endpoint here
@app.post("/mcp/insert_customer")
def insert_customer(data: dict):
    conn = snowflake.connector.connect(
        user="YOUR_USER",
        password="YOUR_PASSWORD",
        account="YOUR_ACCOUNT",
        warehouse="YOUR_WAREHOUSE",
        database="YOUR_DATABASE",
        schema="YOUR_SCHEMA"
    )
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO customer_data (name, email) VALUES (%s, %s)",
        (data["name"], data["email"])
    )
    conn.commit()
    cursor.close()
    conn.close()
    return {"status": "success", "inserted": data}

# ✅ Keep your MCP tools and logic below
@mcp.tool()
async def run_cortex_agents(query: str) -> Dict[str, Any]:
  payload = {
        "model": "claude-3-5-sonnet",
        "response_instruction": "You are a helpful AI assistant.",
        "experimental": {},
        "tools": [
            {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "Analyst1"}},
            {"tool_spec": {"type": "cortex_search", "name": "Search1"}},
            {"tool_spec": {"type": "sql_exec", "name": "sql_execution_tool"}},
        ],
        "tool_resources": {
            "Analyst1": {"semantic_model_file": SEMANTIC_MODEL_FILE},
            "Search1":  {"name": CORTEX_SEARCH_SERVICE},
        },
        "tool_choice": {"type": "auto"},
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": query}]}
        ],
    }
    request_id = str(uuid.uuid4())
    url = f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/cortex/agent:run"
    headers = {
        **API_HEADERS,
        "Accept": "text/event-stream",
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream(
            "POST",
            url,
            json=payload,
            headers=headers,
            params={"requestId": request_id},
        ) as resp:
            resp.raise_for_status()
            text, sql, citations = await process_sse_response(resp)
    results = await execute_sql(sql) if sql else None
    return {
        "text": text,
        "citations": citations,
        "sql": sql,
        "results": results,
    }
    pass

if __name__ == "__main__":
    import uvicorn
    # Run FastAPI and MCP together
    uvicorn.run(app, host="0.0.0.0", port=8000)
