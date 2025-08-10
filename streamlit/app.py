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
import nlp.generate_sql as gsql

from render_sidebar import render_sidebar

def looks_like_sql(s: str) -> bool:
    lo = (s or "").lstrip().lower()
    return lo.startswith(("select", "with", "explain", "insert into", "update", "delete from", "create view", "create table"))

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
schema_str = gsql.load_schema()
prompt_template = gsql.load_prompt_template()


# User input
nl_query = st.text_input("Ask a baseball question:")
submit = st.button("Generate SQL and Run")

if submit and nl_query:
    norm_q, season = gsql.normalize_query(nl_query)
    full_prompt = gsql.build_prompt(norm_q, schema_str, prompt_template, season, current_year=gsql.CURRENT_YEAR)
    sql_query = gsql.get_sql_from_gemini(full_prompt)

    action = gsql.handle_model_response(sql_query, season)
    if action == "__REPROMPT__":
        sql_query = gsql.get_sql_from_gemini(
            full_prompt + "\n\n# REMINDER: REQUESTED_SEASON == CURRENT_YEAR; provide season-to-date SQL.")
        action = gsql.handle_model_response(sql_query, season)

    if action and action != "__REPROMPT__":
        st.error(action)
        st.stop()
    if not looks_like_sql(sql_query):
        st.error("I couldn’t generate executable SQL from that question.")
        st.stop()

    st.code(sql_query, language="sql")

    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            df_result = pd.read_sql_query(sql_query, conn)
        df_result = title_case_columns(df_result)
        st.success("✅ Query successful!")
        st.dataframe(df_result)
    except Exception as e:
        st.error(f"❌ Error running PostgreSQL query: {e}")

