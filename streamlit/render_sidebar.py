# sidebar.py

import streamlit as st

def render_sidebar():
    with st.sidebar:
        # Branding
        st.markdown("## ⚾ Baseball Search")
        st.markdown("Built by [Cameron Walker](https://www.linkedin.com/in/cameronjwalker9/)")
        st.markdown("[View Project on GitHub](https://github.com/txcwalker)")
        st.markdown("📮 Questions? [txcwalker@gmail.com](mailto:txcwalker@gmail.com)")
        st.markdown("---")

        # Navigation
        st.markdown("### Navigation")
        st.page_link("app.py", label="🏠 Home")
        st.page_link("pages/about.py", label="📘 About")
        st.page_link("pages/contact.py", label="✉️ Contact")
        st.page_link("pages/how_to_use.py", label = "❓ How to Use")
