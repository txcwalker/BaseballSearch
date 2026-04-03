import streamlit as st
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from render_sidebar import render_sidebar

st.set_page_config(page_title="About · Databaseball", page_icon="⚾")
render_sidebar()

st.markdown("""
    <div style='text-align: center; padding: 2rem 0 1rem 0;'>
        <h1 style='font-size: 2.2em; letter-spacing: -1px;'>About Databaseball</h1>
        <p style='font-size: 1.05em; color: #666;'>
            A fast, flexible way to explore baseball stats using natural language.
        </p>
    </div>
""", unsafe_allow_html=True)

st.markdown("---")

# Purpose
st.markdown("### 🎯 Purpose")
st.markdown("""
Databaseball was built to make baseball data exploration **accessible and intuitive** — 
even for users without SQL or coding experience.

Instead of digging through spreadsheets or learning query syntax, you just ask a question 
in plain English. The app translates it into a database query, runs it against a 
continuously-updated PostgreSQL database, and returns real results instantly.

Whether you're a casual fan or a serious sabermetrician, Databaseball lets you explore 
career trends, player comparisons, stat leaderboards, and advanced analytics across **decades of MLB data**.
""")

st.markdown("---")

# Tech Stack
st.markdown("### 🛠️ Tech Stack")

col1, col2 = st.columns(2)
with col1:
    st.markdown("""
    **Frontend & App**
    - [Streamlit](https://streamlit.io) — UI framework
    - Python 3.11

    **Database**
    - PostgreSQL on **AWS RDS**
    - Lahman Baseball Database (historical)
    - FanGraphs (advanced stats)
    - Statcast via `pybaseball` *(coming soon)*
    """)
with col2:
    st.markdown("""
    **AI / NLP**
    - **Google Gemini** — natural language → SQL translation
    - Custom prompt templates for common query patterns
    - Fast-path router for leaderboard queries

    **Infrastructure**
    - **GitHub Actions** — daily ETL pipelines
    - **AWS RDS** — managed PostgreSQL
    - Streamlit Cloud — app hosting
    """)

st.markdown("---")

# Creator
st.markdown("### 👤 About the Creator")

col1, col2 = st.columns([2, 1])
with col1:
    st.markdown("""
    Hi, I'm **Cameron Walker** — a data scientist and lifelong baseball fan based in Texas.

    I built Databaseball because I wanted a tool that made it easy to answer the kinds of 
    questions baseball fans actually ask: *"Who had the best ERA since 2010?"*, 
    *"How does Ohtani's WAR compare to Trout's?"*, *"Which teams overperformed their Pythagorean record?"*

    This project combines my interest in natural language interfaces, data engineering, 
    and the sport I grew up watching.
    """)
with col2:
    st.markdown("""
    **Find me online:**

    🐙 [GitHub](https://github.com/txcwalker)

    💼 [LinkedIn](https://www.linkedin.com/in/cameronjwalker9/)

    🌐 [Website](https://txcwalker.github.io/)

    ✉️ [Email](mailto:cwalkerprojects7@gmail.com)
    """)

st.markdown("---")

st.markdown("""
    <div style='text-align: center; color: #888; font-size: 0.85em; padding: 0.5rem 0;'>
        Databaseball · Beta v0.1 · Built with ❤️ and too many baseball stats
    </div>
""", unsafe_allow_html=True)