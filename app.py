import os
import streamlit as st
import pandas as pd

# Local pages
from trackers.steth import get_steth_rebases
from trackers.aave  import get_aave_interest

st.set_page_config(page_title="DeFi Center", layout="wide")
st.title("💸 DeFi Center")

# Secrets / env check for Infura URL
INFURA_URL = st.secrets.get("INFURA_URL", os.getenv("INFURA_URL", "")).strip()
if not INFURA_URL:
    st.warning("Set INFURA_URL in Streamlit Secrets (or environment) before using the app.")

wallet = st.text_input("Enter your Ethereum wallet address:", help="0x… checksummed or lowercased is fine")
days   = st.slider("Days back", min_value=7, max_value=365, value=30, step=1)

tab1, tab2 = st.tabs(["stETH Rebases", "Aave USDC Interest"])

with tab1:
    st.subheader("stETH Daily Rebases")
    if wallet and INFURA_URL:
        with st.spinner("Crunching daily balances, transfers, and rebases…"):
            df = get_steth_rebases(wallet, days_back=days, infura_url=INFURA_URL)
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "Download stETH CSV",
            df.to_csv(index=False),
            file_name="steth_rebases.csv",
            mime="text/csv"
        )

with tab2:
    st.subheader("Aave USDC Interest (placeholder)")
    if wallet:
        df = get_aave_interest(wallet, days_back=days)
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "Download Aave CSV",
            df.to_csv(index=False),
            file_name="aave_usdc.csv",
            mime="text/csv"
        )

st.caption("Tip: Free Infura is rate-limited; try fewer days if requests time out.")
