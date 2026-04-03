import streamlit as st
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from render_sidebar import render_sidebar

st.set_page_config(page_title="Contact · Databaseball", page_icon="⚾")
render_sidebar()

st.markdown("""
    <div style='text-align: center; padding: 2rem 0 1rem 0;'>
        <h1 style='font-size: 2.2em; letter-spacing: -1px;'>Contact & Feedback</h1>
        <p style='font-size: 1.05em; color: #666;'>
            Have feedback, found a bug, or want to collaborate? Let's connect.
        </p>
    </div>
""", unsafe_allow_html=True)

st.markdown("---")

col1, col2 = st.columns([1.2, 1])

with col1:
    st.markdown("### 💬 Send a Message")
    st.markdown("Use this form to send feedback, bug reports, or feature requests directly.")

    with st.form("contact_form", clear_on_submit=True):
        name = st.text_input("Your Name", placeholder="e.g. Aaron Judge")
        email = st.text_input("Your Email", placeholder="e.g. you@example.com")
        topic = st.selectbox(
            "Topic",
            ["General Feedback", "Bug Report", "Feature Request", "Data Issue", "Other"]
        )
        message = st.text_area(
            "Message",
            placeholder="Describe your feedback, bug, or idea in detail...",
            height=140,
        )
        submitted = st.form_submit_button("Send Message", use_container_width=True)

    if submitted:
        if name and email and message:
            st.success(f"Thanks, {name}! Your message has been noted. (Note: this form does not yet send email — please use the direct email below for now.)")
        else:
            st.warning("Please fill in all fields before submitting.")

with col2:
    st.markdown("### 📬 Direct Contact")
    st.markdown("""
    Prefer to reach out directly? Here's where to find me:

    **Email**  
    [cwalkerprojects7@gmail.com](mailto:cwalkerprojects7@gmail.com)

    **LinkedIn**  
    [Cameron J. Walker](https://www.linkedin.com/in/cameronjwalker9/)

    **GitHub**  
    [@txcwalker](https://github.com/txcwalker)

    **Website**  
    [txcwalker.github.io](https://txcwalker.github.io/)
    """)

    st.markdown("---")

    st.markdown("### 🐛 Found a Bug?")
    st.markdown("""
    If a question returned wrong results or failed unexpectedly, the most helpful thing 
    you can include is:

    - The exact question you typed
    - What result you got (or that it errored)
    - What result you expected

    You can paste this in the form or email it directly.
    """)