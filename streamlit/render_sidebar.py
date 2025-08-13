# render_sidebar.py
import streamlit as st

def render_sidebar():
    with st.sidebar:
        # Branding
        st.markdown("## Baseball Search")
        st.markdown("Built by [Cameron Walker](https://www.linkedin.com/in/cameronjwalker9/)")
        st.markdown("[View Project on GitHub](https://github.com/txcwalker)")
        st.markdown("Questions? [cwalkerprojects7@gmail.com](mailto:cwalkerprojects7@gmail.com)")
        st.markdown("---")

