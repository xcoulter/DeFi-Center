import os
import streamlit as st
import pandas as pd

from trackers.steth import get_steth_rebases_from_first_activity
from trackers.aave  import get_aave_interest

st.set_page_config(page_title="DeFi Center", layout="wide")
st.title("💸 DeFi Center")

# Read Infura URL from Secrets or env (no network calls at import time)
INFURA_URL = st.secrets.get("INFURA_URL", os.getenv("INFURA_URL", "")).strip()
if not INFURA_URL:
    st.warning("Set INFURA_URL in Streamlit Secrets or environment. Example: https://mainnet.infura.io/v3/<KEY>")

wallet = st.text_input("Enter your Ethereum wallet address:", help="0x… (checksum or lowercase is fine)")

# Cache so reruns don’t hammer Infura
@st.cache_data(show_spinner=False, ttl=900)
def _cached_rebases(wallet_addr: str, infura_url: str) -> pd.DataFrame:
    return get_steth_rebases_from_first_activity(wallet_addr, infura_url=infura_url)

tab1, tab2 = st.tabs(["stETH Rebases", "Aave USDC Interest"])

with tab1:
    st.subheader("stETH Daily Rebases — from first activity → today")
    run = st.button("Compute stETH Rebases")
    if wallet and INFURA_URL and run:
        try:
            with st.spinner("Locating first stETH activity and computing daily balances, transfers, and rebases…"):
                df = _cached_rebases(wallet, INFURA_URL)  # ✅ only runs after button click
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
    st.caption("Tip: Free Infura can be slow. Results are cached for ~15 minutes.")

with tab2:
    st.subheader("Aave USDC Interest (placeholder)")
    if wallet:
        df2 = get_aave_interest(wallet)
        st.dataframe(df2, use_container_width=True)
        st.download_button(
            "Download Aave CSV",
            df2.to_csv(index=False),
            file_name="aave_usdc.csv",
            mime="text/csv"
        )

# Optional: quick RPC health check (no heavy calls)
with st.expander("🔧 Provider health check"):
    if st.button("Test RPC"):
        import requests
        try:
            r = requests.post(
                INFURA_URL,
                json={"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},
                timeout=10
            )
            r.raise_for_status()
            js = r.json()
            if "result" in js:
                st.success(f"OK — latest block: {int(js['result'],16)}")
            else:
                st.error(f"RPC error: {js.get('error')}")
        except Exception as e:
            st.error(f"HTTP error: {e}")
