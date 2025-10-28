import os
import streamlit as st
from dotenv import load_dotenv
import snowflake.connector
import requests

# Load environment variables
load_dotenv()

st.set_page_config(page_title="Snowflake MCP Server", layout="wide")
st.title("Snowflake MCP Server with Mistral AI")

# --- Display config info (non-sensitive) ---
st.caption(f"Snowflake Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
st.caption(f"User: {os.getenv('SNOWFLAKE_USER')}")
st.caption(f"Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")

# --- Validate required environment variables ---
required_env_vars = [
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE", "MISTRAL_API_KEY"
]
missing = [var for var in required_env_vars if not os.getenv(var)]
if missing:
    st.error(f"Missing environment variables: {', '.join(missing)}")

# --- Snowflake connection (cached) ---
@st.cache_resource(show_spinner=False)
def get_snowflake_conn():
    auth = os.getenv("SNOWFLAKE_AUTHENTICATOR", "oauth").lower()
    common = dict(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    if auth == "oauth":
        token = os.getenv("SNOWFLAKE_OAUTH_TOKEN")
        if not token:
            raise RuntimeError("SNOWFLAKE_OAUTH_TOKEN is not set for oauth authenticator.")
        return snowflake.connector.connect(authenticator="oauth", token=token, **common)
    elif auth == "externalbrowser":
        return snowflake.connector.connect(
            authenticator="externalbrowser",
            user=os.getenv("SNOWFLAKE_USER"),
            **common
        )
    else:
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            **common
        )

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
try:
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_DATE;")
    result = cursor.fetchone()
    st.markdown("**Snowflake Current Date:**")
    st.write(result[0])
except Exception as e:
    st.error(f"Snowflake connection error: {e}")
