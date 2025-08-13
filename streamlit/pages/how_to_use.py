import streamlit as st
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from render_sidebar import render_sidebar

st.set_page_config(page_title="How to Use", page_icon="❓")
render_sidebar()

st.markdown("""
    <div style='text-align: center; padding: 2rem 0;'>
        <h1>How to Use Baseball Search</h1>
        <p style='font-size: 1.1em;'>A quick guide to help you get the most out of the app.</p>
    </div>
""", unsafe_allow_html=True)

st.markdown("---")

st.markdown("### What You Can Ask")
st.write("""
You can ask natural language questions like:
- "How many home runs did Aaron Judge hit in 2022?"
- "Top 10 pitchers by strikeouts since 2010"
- "Compare Mike Trout and Mookie Betts in 2023"
- "Show Shohei Ohtani's WAR by season"

You can also get team stats, leaderboards, or filtered stats by year and position.

""")

st.markdown("### What You Cannot Ask")
st.write("""
The database does not have access to individual game or pitch data (yet) so any questions dealing with game to game data will not be answered
- Show me the ten longest hitting streaks since 2015
- What players have had the most home runs in a month since 2018
- What Left handed hitters hit left handed pitchers the best with respect to OPS (Pretty much and handedness question)
""")

st.markdown("### Data Sources")
st.write("""
- **FanGraphs**: advanced stats like WAR, wOBA, ISO, etc.
- **Lahman Database**: historical MLB data
- **Statcast (soon)**: detailed pitch and batted ball data
- The database is updated daily at 9 AM EST, so stats are not live until the next morning
""")

st.markdown("### Tips")
st.write("""
- Use full player names when possible.
- Specify a year if you're looking for seasonal stats.
- Be as specific as possible when asking questions. 
- "Who led or Who leads" will give only the top result. For a list of top results use something like "What Players"
- When both pitchers and hitters can have the same statistic (like strikeout) be sure to specify the position!
- If the model fails but you think it should not have, try rephrasing the question!
- Type in complete sentences wit the proper grammar
- This is a beta version — we’re still improving how questions are interpreted!
- The app should infer only qualified players but sometimes, it forgets, so if you see weird results, add " Among qualified players/pitchers/hitters/etc"
""")

st.markdown("### Coming Soon")
st.write("""
- Interactive visualizations
- Statcast support
- Suggested queries + quick links
""")