import streamlit as st
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from render_sidebar import render_sidebar

st.set_page_config(page_title="Contact")
render_sidebar()

st.markdown("""
    <div style='text-align: center; padding: 2rem 0;'>
        <h1>Contact</h1>
        <p style='font-size: 1.1em;'>Have feedback, a feature request, or want to collaborate? Let’s connect!</p>
    </div>
""", unsafe_allow_html=True)

st.markdown("---")

st.markdown("### Reach Out")
st.write("""
- Email: **cwalkerproejcts7@gmail.com**
- LinkedIn: [Cameron J. Walker](https://www.linkedin.com/in/cameronjwalker9/)
- GitHub: [@txcwalker](https://github.com/txcwalker)
""")

st.markdown("### Feedback Form (coming soon)")
st.write("A form for submitting suggestions or bug reports will be added here in a future version.")
