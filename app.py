# app.py

import os
import streamlit as st
from trackers.steth import get_steth_rebases_from_first_activity

st.set_page_config(page_title="DeFi Center", layout="wide")
st.title("💸 DeFi Center")

INFURA_URL = st.secrets.get("INFURA_URL", os.getenv("INFURA_URL", "")).strip()
if not INFURA_URL:
    st.error("INFURA_URL is missing. Add it in Streamlit Secrets or as an environment variable.")

wallet = st.text_input("Enter your Ethereum wallet address:")

@st.cache_data(show_spinner=False, ttl=900)
def _cached_rebases(wallet_addr: str, infura_url: str):
    return get_steth_rebases_from_first_activity(wallet_addr, infura_url=infura_url)

tab1, tab2 = st.tabs(["stETH Rebases", "Aave USDC Interest"])

with tab1:
    st.subheader("stETH Daily Rebases — from first activity → today")
    run = st.button("Compute stETH Rebases")
    if wallet and INFURA_URL and run:
        try:
            with st.spinner("Locating first stETH activity and computing daily balances, transfers, and rebases…"):
                df = _cached_rebases(wallet, INFURA_URL)   # ✅ called only after click
        except Exception as e:
            st.error(f"Failed to compute rebases: {e}")
        else:
            if df is None or df.empty:
                st.info("No stETH activity found for this wallet.")
            else:
                st.dataframe(df, use_container_width=True)
                st.download_button(
                    "Download stETH CSV",
                    df.to_csv(index=False),
                    file_name="steth_rebases.csv",
                    mime="text/csv"
                )

    st.caption("Note: Free Infura can be slow; this computes the full history once and caches it for ~15 minutes.")
