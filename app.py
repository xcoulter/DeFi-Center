import os
import streamlit as st
import pandas as pd

from trackers.steth import get_steth_rebases_from_first_activity
from trackers.aave  import get_aave_interest

st.set_page_config(page_title="DeFi Center", layout="wide")
st.title("💸 DeFi Center")

# Secrets / env for Infura URL
INFURA_URL = st.secrets.get("INFURA_URL", os.getenv("INFURA_URL", "")).strip()
if not INFURA_URL:
    st.warning("Set INFURA_URL in Streamlit Secrets (or environment). Example: https://mainnet.infura.io/v3/<KEY>")

wallet = st.text_input("Enter your Ethereum wallet address:", help="0x… (checksum or lowercase is fine)")

# Cache results so reruns don’t hammer Infura
@st.cache_data(show_spinner=False, ttl=900)
def _cached_rebases(wallet_addr: str, infura_url: str):
    return get_steth_rebases_from_first_activity(wallet_addr, infura_url=infura_url)

tab1, tab2 = st.tabs(["stETH Rebases", "Aave USDC Interest"])

with tab1:
    st.subheader("stETH Daily Rebases — from first activity → today")
    run = st.button("Compute stETH Rebases")
    if wallet and INFURA_URL and run:
        with st.spinner("Locating first stETH activity and computing daily balances, transfers, and rebases…"):
            df = _cached_rebases(wallet, INFURA_URL)
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
    st.caption("Note: Free Infura can be slow; this computes the full history once and caches it for 15 minutes.")

with tab2:
    st.subheader("Aave USDC Interest (placeholder)")
    if wallet:
        df = get_aave_interest(wallet)
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "Download Aave CSV",
            df.to_csv(index=False),
            file_name="aave_usdc.csv",
            mime="text/csv"
        )
