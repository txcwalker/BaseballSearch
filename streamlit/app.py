# app.py

import os
import sys
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

# Add project root to import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Custom imports
from nlp.generate_sql import build_prompt, get_sql_from_gemini, load_schema, load_prompt_template

# Load AWS RDS environment
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")

# PostgreSQL config
DB_PARAMS = {
    "host": os.getenv("AWSHOST"),
    "port": os.getenv("AWSPORT"),
    "dbname": os.getenv("AWSDATABASE"),
    "user": os.getenv("AWSUSER"),
    "password": os.getenv("AWSPASSWORD"),
}

# Streamlit app
st.title("Databaseball Explorer")

# Load schema and prompt template
schema_str = load_schema()
prompt_template = load_prompt_template()

# User input
nl_query = st.text_input("Ask a baseball question:")
submit = st.button("Generate SQL and Run")

if submit and nl_query:
    full_prompt = build_prompt(nl_query, schema_str, prompt_template)
    sql_query = get_sql_from_gemini(full_prompt)

    st.code(sql_query, language="sql")

    try:
        conn = psycopg2.connect(**DB_PARAMS)
        df_result = pd.read_sql_query(sql_query, conn)
        conn.close()
        st.success("✅ Query successful!")
        st.dataframe(df_result)
    except Exception as e:
        st.error(f"❌ Error running PostgreSQL query: {e}")
