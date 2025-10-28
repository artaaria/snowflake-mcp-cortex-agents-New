import os
import streamlit as st
from dotenv import load_dotenv
import snowflake.connector
import requests


load_dotenv()
mcp = FastMCP("cortex_agent")

# Initialize Mistral
mistral_client = MistralClient(api_key=os.getenv("MISTRAL_API_KEY"))

# Snowflake constants
SNOWFLAKE_ACCOUNT_URL = os.getenv("SNOWFLAKE_ACCOUNT_URL")
SNOWFLAKE_PAT = os.getenv("SNOWFLAKE_PAT")
API_HEADERS = {
    "Authorization": f"Bearer {SNOWFLAKE_PAT}",
    "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
    "Content-Type": "application/json",
}

@mcp.tool()
async def ask_mistral(prompt: str) -> dict:
    """Call Mistral LLM"""
    response = mistral_client.chat.complete(
        model="mistral-medium",
        messages=[{"role": "user", "content": prompt}]
    )
    return {"response": response.choices[0].message.content}

@mcp.tool()
async def run_cortex_agents(query: str) -> dict:
    """Run Snowflake Cortex agent and execute SQL"""
    payload = {
        "model": "claude-3-5-sonnet",
        "messages": [{"role": "user", "content": query}],
        "tools": [{"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "Analyst"}}],
    }
    headers = {**API_HEADERS, "Accept": "text/event-stream"}
    request_id = str(uuid.uuid4())

    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream("POST", f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/cortex/agent:run",
                                 json=payload, headers=headers, params={"requestId": request_id}) as resp:
            resp.raise_for_status()
            text, sql, citations = await process_sse_response(resp)

    results = await execute_sql(sql) if sql else None
    return {"text": text, "sql": sql, "results": results}

if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8000)
    
# --- Mistral API call ---
def query_mistral(prompt: str) -> str:
    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        raise RuntimeError("MISTRAL_API_KEY missing.")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": os.getenv("MISTRAL_MODEL", "mistral-7b-instruct"),
        "messages": [{"role": "user", "content": prompt}],
    }
    try:
        resp = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            json=payload,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[Mistral error] {e}"

# --- UI: Ask Mistral ---
query = st.text_input("Ask Mistral something:")
if query:
    response = query_mistral(query)
    st.markdown("**Mistral Response:**")
    st.write(response)

# --- Optional: Run a Snowflake query ---
#try:
#    conn = get_snowflake_conn()
 #   cursor = conn.cursor()
  #  cursor.execute("SELECT CURRENT_DATE;")
   # result = cursor.fetchone()
   # st.markdown("**Snowflake Current Date:**")
    #st.write(result[0])
#except Exception as e:
   # st.error(f"Snowflake connection error: {e}")
