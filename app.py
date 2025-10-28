import os
import streamlit as st
from dotenv import load_dotenv
import snowflake.connector
import requests

load_dotenv()

st.title("Snowflake MCP Server with Mistral AI")

# --- Config display (non-sensitive) ---
st.caption(f"Snowflake Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
st.caption(f"User: {os.getenv('SNOWFLAKE_USER')}")
st.caption(f"Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")

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
        # Expect a real OAuth ACCESS TOKEN (e.g., from an OAuth flow / IdP)
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
        # username/password path
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            **common
        )

# --- Mistral API call (requests) ---
def query_mistral(prompt: str) -> str:
    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        raise RuntimeError("MISTRAL_API_KEY missing.")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        # pick a model your account has access to (examples: "mistral-medium", "open-mistral-7b")
        "model": os.getenv("MISTRAL_MODEL", "mistral-7b-instruct"),
        "messages": [{"role": "user", "content": prompt}],
    }

    # Corporate proxy/CAs: requests will honor HTTPS_PROXY/REQUESTS_CA_BUNDLE if set
    # Add a timeout and basic error handling
    try:
        resp = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            json=payload,
            headers=headers,
            timeout=30,     # important
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        return f"[Mistral error] {e}"

# --- UI: Ask Mistral ---
query = st.text_input("
