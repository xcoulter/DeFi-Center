import streamlit as st
from trackers.steth import get_steth_rebases
from trackers.aave import get_aave_interest

st.set_page_config(page_title="DeFi Center", layout="wide")
st.title("💸 DeFi Center")

wallet = st.text_input("Enter your Ethereum wallet address:")

tab1, tab2 = st.tabs(["stETH Rebases", "Aave USDC Interest"])

with tab1:
    st.subheader("stETH Daily Rebases")
    if wallet:
        df = get_steth_rebases(wallet, days_back=30)  # default 30 days
        st.dataframe(df, use_container_width=True)
        st.download_button("Download stETH CSV",
                           df.to_csv(index=False),
                           "steth_rebases.csv",
                           "text/csv")

with tab2:
    st.subheader("Aave USDC Interest (stub)")
    if wallet:
        df = get_aave_interest(wallet)
        st.dataframe(df, use_container_width=True)
        st.download_button("Download Aave CSV",
                           df.to_csv(index=False),
                           "aave_usdc.csv",
                           "text/csv")
