# render_sidebar.py
import streamlit as st

def render_sidebar():
    with st.sidebar:
        st.markdown("""
            <div style='text-align: center; padding: 1rem 0 0.5rem 0;'>
                <div style='font-size: 2em;'>⚾</div>
                <div style='font-size: 1.3em; font-weight: 700; letter-spacing: -0.5px; color: #1a1a2e;'>Databaseball</div>
                <div style='font-size: 0.75em; color: #888; margin-top: 2px;'>Natural Language Baseball Stats</div>
            </div>
        """, unsafe_allow_html=True)

        st.markdown("---")

        st.markdown("""
            <div style='font-size: 0.8em; color: #555; line-height: 1.8;'>
                <div>📊 <b>Data:</b> FanGraphs + Lahman DB</div>
                <div>🔄 <b>Updated:</b> Daily at 9 AM EST</div>
                <div>🤖 <b>AI:</b> Google Gemini NL→SQL</div>
                <div>☁️ <b>DB:</b> PostgreSQL on AWS RDS</div>
            </div>
        """, unsafe_allow_html=True)

        st.markdown("---")

        st.markdown("""
            <div style='font-size: 0.82em; color: #666;'>
                Built by <a href='https://www.linkedin.com/in/cameronjwalker9/' target='_blank' style='color: #c0392b; text-decoration: none; font-weight: 600;'>Cameron Walker</a>
            </div>
        """, unsafe_allow_html=True)

        st.markdown("""
            <div style='margin-top: 0.4rem; font-size: 0.82em;'>
                <a href='https://github.com/txcwalker' target='_blank' style='color: #555; text-decoration: none;'>🐙 GitHub</a>
                &nbsp;&nbsp;
                <a href='mailto:cwalkerprojects7@gmail.com' style='color: #555; text-decoration: none;'>✉️ Email</a>
            </div>
        """, unsafe_allow_html=True)

        st.markdown("<div style='margin-top: 1.5rem; font-size: 0.72em; color: #bbb;'>Beta v0.1 · Data not live until next morning</div>", unsafe_allow_html=True)