import streamlit as st
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from render_sidebar import render_sidebar

st.set_page_config(page_title="How to Use · Databaseball", page_icon="⚾")
render_sidebar()

st.markdown("""
    <div style='text-align: center; padding: 2rem 0 1rem 0;'>
        <h1 style='font-size: 2.2em; letter-spacing: -1px;'>How to Use Databaseball</h1>
        <p style='font-size: 1.05em; color: #666;'>
            A quick guide to asking great questions and getting the best results.
        </p>
    </div>
""", unsafe_allow_html=True)

st.markdown("---")

# --- What You Can Ask ---
with st.expander("✅ What You Can Ask", expanded=True):
    st.markdown("""
    Databaseball handles seasonal and career baseball stats. These types of questions work well:

    | Category | Example |
    |---|---|
    | Player seasons | *"What were Shohei Ohtani's counting stats in 2023?"* |
    | Leaderboards | *"Top 10 pitchers by strikeouts since 2010"* |
    | Comparisons | *"Compare Mike Trout and Mookie Betts in 2023"* |
    | Advanced stats | *"Which pitchers had the biggest FIP vs ERA gap since 2018?"* |
    | Team stats | *"Show me the Yankees' team ERA by year since 2015"* |
    | Career totals | *"Show Aaron Judge's HR totals by season"* |
    """)

# --- What You Can't Ask (Yet) ---
with st.expander("❌ What Doesn't Work (Yet)", expanded=False):
    st.markdown("""
    The database currently contains season-level stats only — not game-by-game or pitch-level data.
    The following types of questions **will not return results**:

    - Game-by-game stats or hitting streaks
    - Monthly splits (*"most HRs in August since 2018"*)
    - Batter vs. pitcher handedness splits
    - Live or in-progress game data
    - Play-by-play or pitch trajectory data

    > **Coming soon:** Statcast integration and more granular game-level data.
    """)

# --- Tips for Best Results ---
with st.expander("💡 Tips for Best Results", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        **Writing good queries**
        - Use full player names (*"Aaron Judge"*, not *"Judge"*)
        - Specify a year when possible
        - Write in complete sentences with correct grammar
        - *"Who leads"* → returns 1 result; *"What players lead(s)"* → returns a list
        - When strikeouts could mean hitter **or** pitcher, specify which!
        """)
    with col2:
        st.markdown("""
        **Getting qualified results**
        - The app tries to infer qualified players automatically
        - If you see unexpected results, add: *"among qualified players"*
        - For pitchers, try: *"among qualified starters"*

        **If it fails**
        - Try rephrasing the question
        - Simplify — break complex questions into parts
        - Add more specifics (year, position, league)
        """)

# --- Data Sources ---
with st.expander("📚 Data Sources", expanded=False):
    st.markdown("""
    | Source | Contents |
    |---|---|
    | **FanGraphs** | Advanced batting & pitching stats (WAR, wOBA, FIP, ISO, xFIP, etc.) |
    | **Lahman Database** | Historical MLB stats going back to 1871 |
    | **Statcast** *(coming soon)* | Pitch-level and batted-ball data via `pybaseball` |

    The database is updated **daily at 9 AM EST**.
    """)

# --- Example Queries ---
with st.expander("📋 More Example Queries to Try", expanded=False):
    st.markdown("""
    Copy any of these into the search box on the home page:

    - *"What were the top 5 WAR seasons for position players since 2000?"*
    - *"Which teams had the best bullpen ERA in 2022?"*
    - *"List the top 10 OPS seasons since 2015 among qualified hitters"*
    - *"How many strikeouts did Gerrit Cole have in each season since 2018?"*
    - *"What hitters had the highest Isolated Power in 2021?"*
    - *"Compare the batting averages of all AL East teams in 2023"*
    """)

st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #888; font-size: 0.85em; padding: 0.5rem 0;'>
        This is a beta version — we're still improving how questions are interpreted!<br>
        Questions or feedback? <a href='mailto:cwalkerprojects7@gmail.com' style='color: #c0392b;'>cwalkerprojects7@gmail.com</a>
    </div>
""", unsafe_allow_html=True)