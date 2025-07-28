import streamlit as st
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from render_sidebar import render_sidebar

st.set_page_config(page_title="About")
render_sidebar()

st.markdown("""
    <div style='text-align: center; padding: 2rem 0;'>
        <h1>About Databaseball</h1>
        <p style='font-size: 1.1em;'>A fast, flexible way to explore baseball stats using natural language.</p>
    </div>
""", unsafe_allow_html=True)

st.markdown("---")

st.markdown("### 🔍 Purpose")
st.write("""
Baseball Search was created to make data exploration in baseball accessible and intuitive — 
even for users without SQL or coding experience. With natural language search and daily-updated stats, 
you can instantly retrieve career trends, player comparisons, and stat breakdowns across decades of data.
""")

st.markdown("###  Tech Stack")
st.write("""
- **Streamlit** for UI
- **PostgreSQL (AWS RDS)** for backend storage
- **pybaseball & Lahman DB** for stats
- **GitHub Actions** for daily updates
- **Google Gemini/OpenAI** for NLP query generation
""")

st.markdown("### About the Creator")
st.write("""
Created by **Cameron J. Walker**, a data scientist and baseball fan.  
Check out more of my work:
- [GitHub](https://github.com/txcwalker)
- [LinkedIn](https://www.linkedin.com/in/cameronjwalker9/)
- [Website](https://txcwalker.github.io/)
""")
