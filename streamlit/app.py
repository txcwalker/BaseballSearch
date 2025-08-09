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
from render_sidebar import render_sidebar



def title_case_columns(df):
    df.columns = [
        col.replace("_", " ").title() if isinstance(col, str) else col
        for col in df.columns
    ]
    return df

def style_dataframe(df):
    return df.style.set_properties(**{
        'background-color': '#fdfdfd',
        'color': '#111',
        'border-color': '#ccc',
        'font-size': '14px',
        'text-align': 'left'
    }).set_table_styles([
        {'selector': 'th', 'props': [('background-color', '#003f5c'), ('color', 'white'), ('font-size', '15px')]}
    ])


def handle_model_response(response_text):
    if not response_text:
        return "I wasn’t able to generate a valid query for that question."

    if response_text.startswith("Unfortunately I currently do not have access"):
        return response_text

    if response_text.startswith("I can only answer baseball questions."):
        return response_text

    if "SELECT" not in response_text.upper():
        return "I wasn’t able to generate a valid query for that question."

    return None


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

st.set_page_config(page_title="Welcome to Databaseball", layout="wide")
render_sidebar()

# === Hero Section ===
st.markdown("""
    <div style='text-align: center; padding: 3rem 0 2rem 0; background-color: #f0f8ff; border-radius: 12px;'>
        <h1 style='font-size: 3.5em; margin-bottom: 0.2em;'>
            Welcome to Databaseball!
            <span style='font-size: 0.4em; color: white; background-color: #f39c12; padding: 4px 8px; border-radius: 8px; margin-left: 10px; vertical-align: middle;'>BETA</span>
        </h1>
        <p style='font-size: 1.3em; color: #444;'>Ask questions. Explore stats. Discover the game.</p>
    </div>
""", unsafe_allow_html=True)


# === Optional Image ===
# st.image("static/images/BaseballSearchLogo.png", width=150)

# === Features Grid ===
st.markdown("### 🛠️ What You Can Do")
col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 🔍 Ask Questions")
    st.markdown("- 'Show Shohei Ohtani's stats in 2022'\n- 'Top 10 home run hitters since 2010'\n - 'What Pitchers have the single season discrepancy between FIP and ERA since 2015'")

with col2:
    st.markdown("#### 🔁 Updated Daily")
    st.markdown("- Powered by AWS + GitHub Actions\n- Data from FanGraphs & Lahman\n- Always current")

# with col3:
#   st.markdown("#### 📊 See Visuals")
#   st.markdown("- Auto-generated charts\n- Side-by-side stat comparisons\n- Season & career summaries")


# === Quick Nav Links ===
st.markdown("---")
st.markdown("### Read Me")
st.page_link("pages/how_to_use.py", label="❓ How to Use")

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
        df_result = title_case_columns(df_result)
        st.success("✅ Query successful!")
        st.dataframe(df_result)

    except Exception as e:
        st.error(f"❌ Error running PostgreSQL query: {e}")

