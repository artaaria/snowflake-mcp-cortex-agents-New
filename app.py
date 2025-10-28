import streamlit as st
import snowflake.connector
import requests
import os

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

# Mistral API call
def query_mistral(prompt):
    headers = {
        "Authorization": f"Bearer {os.getenv('MISTRAL_API_KEY')}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "mistral-7b-instruct",
        "messages": [{"role": "user", "content": prompt}]
    }
    response = requests.post("https://api.mistral.ai/v1/chat/completions", json=payload, headers=headers)
    return response.json()["choices"][0]["message"]["content"]

# Streamlit UI
st.title("Snowflake MCP Server with Mistral AI")

query = st.text_input("Ask Mistral something:")
if query:
    response = query_mistral(query)
    st.write("Mistral Response:", response)

# Example Snowflake query
cursor = conn.cursor()
cursor.execute("SELECT CURRENT_DATE;")
result = cursor.fetchone()
st.write("Snowflake Date:", result[0])
