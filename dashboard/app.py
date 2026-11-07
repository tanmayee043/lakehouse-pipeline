"""Lakehouse dashboard: reads the dbt marts from DuckDB (read-only).

Run from the repo root:  streamlit run dashboard/app.py
"""

import streamlit as st

st.set_page_config(page_title="Lakehouse", page_icon="📊", layout="wide")

st.navigation(
    [
        st.Page("weather.py", title="Weather", icon="🌦️", default=True),
        st.Page("crypto.py", title="Crypto", icon="📈"),
    ]
).run()
