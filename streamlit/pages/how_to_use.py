import streamlit as st
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from render_sidebar import render_sidebar

st.set_page_config(page_title="How to Use", page_icon="❓")
render_sidebar()

st.markdown("""
    <div style='text-align: center; padding: 2rem 0;'>
        <h1>❓ How to Use Baseball Search</h1>
        <p style='font-size: 1.1em;'>A quick guide to help you get the most out of the app.</p>
    </div>
""", unsafe_allow_html=True)

st.markdown("---")

st.markdown("### 🔎 What You Can Ask")
st.write("""
You can ask natural language questions like:
- "How many home runs did Aaron Judge hit in 2022?"
- "Top 10 pitchers by strikeouts since 2010"
- "Compare Mike Trout and Mookie Betts in 2023"
- "Show Shohei Ohtani's WAR by season"

You can also get team stats, leaderboards, or filtered stats by year and position.

""")

st.markdown("### 🗃️ Data Sources")
st.write("""
- **FanGraphs**: advanced stats like WAR, wOBA, ISO, etc.
- **Lahman Database**: historical MLB data
- **Statcast (soon)**: detailed pitch and batted ball data
""")

st.markdown("### 💡 Tips")
st.write("""
- Use full player names when possible.
- Specify a year if you're looking for seasonal stats.
- If the result looks odd, try rephrasing or being more specific.
- This is a beta version — we’re still improving how questions are interpreted!
""")

st.markdown("### 🛠️ Coming Soon")
st.write("""
- Interactive visualizations
- Statcast support
- Search by team, position, or era
- Suggested queries + quick links
""")