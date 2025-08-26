import os
import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime

from trackers.steth import (
    get_steth_rebases_range,
    get_first_activity_date,   # UTC date helper
)

st.set_page_config(page_title="DeFi Center", layout="wide")
st.title("💸 DeFi Center")

INFURA_URL = st.secrets.get("INFURA_URL", os.getenv("INFURA_URL", "")).strip()
if not INFURA_URL:
    st.warning("Set INFURA_URL in Streamlit Secrets or env. Example: https://mainnet.infura.io/v3/<KEY>")

wallet = st.text_input("Enter your Ethereum wallet address:", help="0x… (checksum or lowercase is fine)")

# Session accumulator
if "steth_accum" not in st.session_state:
    st.session_state["steth_accum"] = pd.DataFrame()
if "steth_first_activity" not in st.session_state:
    st.session_state["steth_first_activity"] = None

st.markdown("### stETH rebases — run by date range (≤ 180 days per run)")

# --- Locate first activity (fast, no logs) ---
left, right = st.columns([1,3])
with left:
    if st.button("Find first stETH activity"):
        if not wallet or not INFURA_URL:
            st.error("Enter a wallet and ensure INFURA_URL is set.")
        else:
            try:
                fa = get_first_activity_date(wallet, INFURA_URL)
                st.session_state["steth_first_activity"] = fa
                if fa is None:
                    st.info("No stETH activity found for this wallet.")
                else:
                    st.success(f"First activity (UTC): {fa.isoformat()}")
            except Exception as e:
                st.error(f"Failed to locate first activity: {e}")

# Defaults: from first activity (if known) or last 180d → yesterday
today = date.today()
yday = today - timedelta(days=1)
accum = st.session_state["steth_accum"]
first_activity = st.session_state["steth_first_activity"]

def day_after_last_accum():
    if accum.empty:
        return first_activity or (yday - timedelta(days=179))
    last_iso = str(accum["date"].max())
    try:
        last_d = datetime.strptime(last_iso, "%Y-%m-%d").date()
    except Exception:
        last_d = yday - timedelta(days=179)
    return min(last_d + timedelta(days=1), yday)

suggested_start = day_after_last_accum()
suggested_end   = min(suggested_start + timedelta(days=179), yday)

c1, c2 = st.columns(2)
with c1:
    start_dt = st.date_input("Start date (UTC)", value=suggested_start, max_value=yday)
with c2:
    end_dt = st.date_input("End date (UTC)", value=suggested_end, min_value=start_dt, max_value=yday)

st.caption("Tip: Run multiple adjacent windows (≤ 180 days each). Results accumulate below and can be downloaded as one CSV.")

# Cache per (wallet, start_iso, end_iso)
@st.cache_data(show_spinner=False, ttl=900)
def _cached_range(wallet_addr: str, start_iso: str, end_iso: str, rpc_url: str) -> pd.DataFrame:
    return get_steth_rebases_range(wallet_addr, start_iso, end_iso, infura_url=rpc_url)

# --- Actions ---
col_run, col_next, col_clear = st.columns([1,1,1])
with col_run:
    run = st.button("Compute this range")
with col_next:
    run_next = st.button("Compute next window")
with col_clear:
    if st.button("Clear accumulated results"):
        st.session_state["steth_accum"] = pd.DataFrame()
        st.success("Cleared.")
        accum = st.session_state["steth_accum"]

# If “Compute next window”, advance to the next suggested 180-day window automatically
if run_next:
    # choose from day after last row (or first activity) for up to 180 days
    s = day_after_last_accum()
    e = min(s + timedelta(days=179), yday)
    start_dt, end_dt = s, e
    run = True  # fall through to the same pipeline

# --- Execute with live updates (slice into ~30-day chunks for UI feedback) ---
def run_window_and_stream(start_dt: date, end_dt: date):
    """Fetch the window in ~30-day slices, updating the table as rows arrive."""
    total_days = (end_dt - start_dt).days + 1
    if total_days <= 0:
        st.info("Empty window.")
        return

    if total_days > 180:
        st.error(f"Window too large: {total_days} days. Please run ≤ 180 days per call.")
        return

    placeholder = st.empty()
    prog = st.progress(0, text="Starting…")

    # Choose slice size to balance speed/feedback
    SLICE_DAYS = 30
    done = 0
    cur_start = start_dt
    while cur_start <= end_dt:
        cur_end = min(cur_start + timedelta(days=SLICE_DAYS - 1), end_dt)
        try:
            df_slice = _cached_range(wallet, cur_start.isoformat(), cur_end.isoformat(), INFURA_URL)
        except Exception as e:
            prog.empty()
            st.error(f"Failed on slice {cur_start} → {cur_end}: {e}")
            return

        if df_slice is not None and not df_slice.empty:
            acc = st.session_state["steth_accum"]
            acc = pd.concat([acc, df_slice], ignore_index=True)
            acc.drop_duplicates(subset=["date","start_block","end_block"], keep="last", inplace=True)
            acc.sort_values("date", inplace=True)
            acc.reset_index(drop=True, inplace=True)
            st.session_state["steth_accum"] = acc

            # Show the updated accumulator immediately
            placeholder.dataframe(acc, use_container_width=True)

        # update progress
        done += (cur_end - cur_start).days + 1
        pct = min(int(done / total_days * 100), 100)
        prog.progress(pct, text=f"Processed {done}/{total_days} day(s)…")

        # next slice
        cur_start = cur_end + timedelta(days=1)

    prog.empty()
    st.success(f"Added window: {start_dt} → {end_dt}. Total rows: {len(st.session_state['steth_accum'])}")

if run:
    if not wallet or not INFURA_URL:
        st.error("Please enter a wallet and ensure INFURA_URL is set.")
    else:
        run_window_and_stream(start_dt, end_dt)

# --- Output / download ---
accum = st.session_state["steth_accum"]
st.markdown("### Accumulated results")
if not accum.empty:
    st.dataframe(accum, use_container_width=True)
    st.download_button(
        "Download accumulated CSV",
        accum.to_csv(index=False),
        file_name="steth_rebases_accumulated.csv",
        mime="text/csv",
    )
else:
    st.info("No results yet. Choose a date window and click **Compute this range** (or **Compute next window**).")

st.divider()
st.subheader("Aave USDC Interest (placeholder)")
st.write("Coming soon.")
