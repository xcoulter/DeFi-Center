import os
import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime

from trackers.steth import (
    get_steth_rebases_range,
    get_first_activity_date,   # fast helper (binary search on balanceOf)
)
from trackers.ausdc import (
    get_atoken_interest_range,
    get_first_activity_date_atoken,
)


st.set_page_config(page_title="DeFi Center", layout="wide")
st.title("💸 DeFi Center")

# ---- Provider secret/env (no network at import) ----
INFURA_URL = st.secrets.get("INFURA_URL", os.getenv("INFURA_URL", "")).strip()
if not INFURA_URL:
    st.warning("Set INFURA_URL in Streamlit Secrets or env. Example: https://mainnet.infura.io/v3/<KEY>")

# ---- Global inputs (shared across tabs) ----
with st.container():
    wallet = st.text_input("Wallet address (0x…)", help="Checksum or lowercase is fine", key="wallet_input")

# ---- Session state ----
if "steth_accum" not in st.session_state:
    st.session_state["steth_accum"] = pd.DataFrame()
if "steth_first_activity" not in st.session_state:
    st.session_state["steth_first_activity"] = None

# =========================
#      PROTOCOL TABS
# =========================
proto_tabs = st.tabs(["Lido", "Aave", "Settings"])

# ======================================================
#                        LIDO
# ======================================================
with proto_tabs[0]:
    st.header("Lido")

    # Sub-tabs inside Lido
    lido_tabs = st.tabs(["stETH Rebasing Rewards"])

    with lido_tabs[0]:
        st.subheader("stETH Rebasing Rewards — run by date range (≤ 180 days per run)")

        # Controls row
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            stream_rows = st.toggle("Stream rows live", value=True, help="Update UI after each day finishes")
        with c2:
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
        with c3:
            st.caption("Tip: Run multiple adjacent windows (≤ 180 days). Results accumulate below and can be downloaded as one CSV.")

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

        r1, r2 = st.columns(2)
        with r1:
            start_dt = st.date_input("Start date (UTC)", value=suggested_start, max_value=yday, key="steth_start")
        with r2:
            end_dt = st.date_input("End date (UTC)", value=suggested_end, min_value=start_dt, max_value=yday, key="steth_end")

        # Cache per (wallet, start_iso, end_iso)
        @st.cache_data(show_spinner=False, ttl=900)
        def _cached_range(wallet_addr: str, start_iso: str, end_iso: str, rpc_url: str) -> pd.DataFrame:
            return get_steth_rebases_range(wallet_addr, start_iso, end_iso, infura_url=rpc_url)

        # Actions
        a1, a2, a3 = st.columns([1, 1, 1])
        with a1:
            run = st.button("Compute this range", key="run_range")
        with a2:
            run_next = st.button("Compute next window", key="run_next")
        with a3:
            if st.button("Clear accumulated results", key="clear_accum"):
                st.session_state["steth_accum"] = pd.DataFrame()
                st.success("Cleared.")
                accum = st.session_state["steth_accum"]

        # Auto-advance to next window
        if run_next:
            s = day_after_last_accum()
            e = min(s + timedelta(days=179), yday)
            start_dt, end_dt = s, e
            run = True

        # Executor with live updates
        def run_window_and_stream(start_dt: date, end_dt: date):
            total_days = (end_dt - start_dt).days + 1
            if total_days <= 0:
                st.info("Empty window.")
                return
            if total_days > 180:
                st.error(f"Window too large: {total_days} days. Please run ≤ 180 days per call.")
                return

            table_ph = st.empty()
            status_ph = st.empty()
            prog = st.progress(0, text="Starting…")

            if stream_rows:
                # Day-by-day streaming
                done = 0
                cur = start_dt
                while cur <= end_dt:
                    try:
                        df_day = _cached_range(wallet, cur.isoformat(), cur.isoformat(), INFURA_URL)
                    except Exception as e:
                        prog.empty()
                        status_ph.error(f"Failed on {cur}: {e}")
                        return

                    if df_day is not None and not df_day.empty:
                        acc = st.session_state["steth_accum"]
                        acc = pd.concat([acc, df_day], ignore_index=True)
                        acc.drop_duplicates(subset=["date","start_block","end_block"], keep="last", inplace=True)
                        acc.sort_values("date", inplace=True)
                        acc.reset_index(drop=True, inplace=True)
                        st.session_state["steth_accum"] = acc

                        table_ph.dataframe(acc, use_container_width=True)
                        status_ph.info(f"Fetched {cur} ({len(df_day)} row)")

                    done += 1
                    prog.progress(min(int(done / total_days * 100), 100),
                                  text=f"Processed {done}/{total_days} day(s)…")
                    cur = cur + timedelta(days=1)

                prog.empty()
                status_ph.success(f"Added window: {start_dt} → {end_dt}. Total rows: {len(st.session_state['steth_accum'])}")
                return

            # Slice updates (fewer UI refreshes)
            SLICE_DAYS = 30
            done = 0
            cur_start = start_dt
            while cur_start <= end_dt:
                cur_end = min(cur_start + timedelta(days=SLICE_DAYS - 1), end_dt)
                try:
                    df_slice = _cached_range(wallet, cur_start.isoformat(), cur_end.isoformat(), INFURA_URL)
                except Exception as e:
                    prog.empty()
                    status_ph.error(f"Failed on slice {cur_start} → {cur_end}: {e}")
                    return

                if df_slice is not None and not df_slice.empty:
                    acc = st.session_state["steth_accum"]
                    acc = pd.concat([acc, df_slice], ignore_index=True)
                    acc.drop_duplicates(subset=["date","start_block","end_block"], keep="last", inplace=True)
                    acc.sort_values("date", inplace=True)
                    acc.reset_index(drop=True, inplace=True)
                    st.session_state["steth_accum"] = acc
                    table_ph.dataframe(acc, use_container_width=True)

                done += (cur_end - cur_start).days + 1
                prog.progress(min(int(done / total_days * 100), 100),
                              text=f"Processed {done}/{total_days} day(s)…")
                cur_start = cur_end + timedelta(days=1)

            prog.empty()
            status_ph.success(f"Added window: {start_dt} → {end_dt}. Total rows: {len(st.session_state['steth_accum'])}")

        # Run if requested
        if run:
            if not wallet or not INFURA_URL:
                st.error("Please enter a wallet and ensure INFURA_URL is set.")
            else:
                run_window_and_stream(start_dt, end_dt)

        # Output / download
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

# ======================================================
#                        AAVE
# ======================================================
with proto_tabs[1]:
    st.header("Aave")

    # ------- Presets (Aave v3 Ethereum mainnet) -------
    AAVE_V3_PRESETS = {
        "aUSDC v3 (6)":   {"address": "0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c", "decimals": 6},
        "aDAI v3 (18)":   {"address": "0x018008bfb33d285247A21d44E50697654f754e63", "decimals": 18},
        "aUSDT v3 (6)":   {"address": "0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a", "decimals": 6},
        "aWETH v3 (18)":  {"address": "0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8", "decimals": 18},
        "awstETH v3 (18)":{"address": "0x0B925Ed163218f6662a35e0F0371Ac234f9E9371", "decimals": 18},
        "aWBTC v3 (8)":   {"address": "0x078f358208685046a11C85e8ad32895DED33A249", "decimals": 8},
        "aLINK v3 (18)":  {"address": "0x191c10Aa4AF7C30e871E70C95dB0E4eb77237530", "decimals": 18},
        "aCRV v3 (18)":   {"address": "0x8Eb270e296023E9d92081fDF967ddd7878724424", "decimals": 18},
        "aLUSD v3 (18)":  {"address": "0x8ffDf2DE812095b1D19CB146E4c004587C0A0692", "decimals": 18},
        "aFRAX v3 (18)":  {"address": "0x0d3890F5dC5fFd3F2eB3C4350e6c8bD97d9eF80D", "decimals": 18},
        "aUNI v3 (18)":   {"address": "0xB3C8e5534F007eD0e2eB5cc3A0b8242bdC036903", "decimals": 18},
        "aENS v3 (18)":   {"address": "0x1c60D7F49CFFe8831c6C47C76C097cEA251fE627", "decimals": 18},
    }

    # ------- User's saved tokens (session-scoped) -------
    if "aave_my_tokens" not in st.session_state:
        st.session_state["aave_my_tokens"] = {k: v.copy() for k, v in AAVE_V3_PRESETS.items()}

    aave_tabs = st.tabs(["aToken Interest (v3)"])

    with aave_tabs[0]:
        st.subheader("aToken Interest — daily accrual (non-rebasing)")

        col0, col1, col2 = st.columns([2, 2, 2])
        with col0:
            preset_label = st.selectbox("Pick an aToken (v3 mainnet presets)", list(AAVE_V3_PRESETS.keys()))
        with col1:
            if st.button("➕ Add to My tokens"):
                st.session_state["aave_my_tokens"][preset_label] = AAVE_V3_PRESETS[preset_label]
                st.success(f"Added {preset_label} to My tokens")
        with col2:
            my_labels = list(st.session_state["aave_my_tokens"].keys())
            active_label = st.selectbox("My tokens", my_labels, index=my_labels.index(preset_label) if preset_label in my_labels else 0)

        active = st.session_state["aave_my_tokens"][active_label]
        atoken_addr = st.text_input("aToken contract", value=active["address"])
        token_decimals = st.number_input("Token decimals", value=active["decimals"], min_value=0, max_value=36, step=1)

        # Save edits back to "My tokens"
        st.session_state["aave_my_tokens"][active_label] = {"address": atoken_addr, "decimals": int(token_decimals)}

        # Session accumulators
        if "atoken_accum" not in st.session_state:
            st.session_state["atoken_accum"] = pd.DataFrame()
        if "atoken_first_activity" not in st.session_state:
            st.session_state["atoken_first_activity"] = None

        # First-activity finder
        if st.button("Find first activity for selected aToken"):
            if not wallet or not INFURA_URL:
                st.error("Enter wallet + INFURA_URL.")
            else:
                try:
                    fa = get_first_activity_date_atoken(wallet, atoken_addr, INFURA_URL)
                    st.session_state["atoken_first_activity"] = fa
                    if fa is None:
                        st.info("No activity found for this aToken.")
                    else:
                        st.success(f"First activity (UTC): {fa.isoformat()}")
                except Exception as e:
                    st.error(f"Error: {e}")

        # Date range
        today = date.today()
        yday = today - timedelta(days=1)
        accum = st.session_state["atoken_accum"]
        first_activity = st.session_state["atoken_first_activity"]

        def day_after_last_accum_any():
            if accum.empty:
                return first_activity or (yday - timedelta(days=179))
            last_iso = str(accum["date"].max())
            try:
                last_d = datetime.strptime(last_iso, "%Y-%m-%d").date()
            except Exception:
                last_d = yday - timedelta(days=179)
            return min(last_d + timedelta(days=1), yday)

        suggested_start = day_after_last_accum_any()
        suggested_end   = min(suggested_start + timedelta(days=179), yday)

        c1, c2 = st.columns(2)
        with c1:
            start_dt = st.date_input("Start date (UTC)", value=suggested_start, max_value=yday, key="atoken_start")
        with c2:
            end_dt = st.date_input("End date (UTC)", value=suggested_end, min_value=start_dt, max_value=yday, key="atoken_end")

        stream_rows_atoken = st.toggle("Stream rows live", value=True, key="atoken_stream")
        st.caption("Interest = (end_balance − start_balance) − withdrawals + deposits. Deposits/withdrawals come from mint/burn events.")

        @st.cache_data(show_spinner=False, ttl=900)
        def _cached_atoken(wallet_addr: str, token: str, start_iso: str, end_iso: str, rpc_url: str, dec: int) -> pd.DataFrame:
            return get_atoken_interest_range(wallet_addr, token, start_iso, end_iso, infura_url=rpc_url, decimals=dec)

        # Actions
        a1, a2, a3 = st.columns([1, 1, 1])
        with a1:
            run = st.button("Compute this range", key="run_atoken_range")
        with a2:
            run_next = st.button("Compute next window", key="run_atoken_next")
        with a3:
            if st.button("Clear accumulated results", key="clear_atoken_accum"):
                st.session_state["atoken_accum"] = pd.DataFrame()
                st.success("Cleared.")
                accum = st.session_state["atoken_accum"]

        if run_next:
            s = day_after_last_accum_any()
            e = min(s + timedelta(days=179), yday)
            start_dt, end_dt = s, e
            run = True

        # Executor (row-by-row like stETH)
        def run_window_and_stream_atoken(start_dt: date, end_dt: date):
            total_days = (end_dt - start_dt).days + 1
            if total_days <= 0:
                st.info("Empty window."); return
            if total_days > 180:
                st.error(f"Window too large: {total_days} days. Please run ≤ 180 days."); return

            table_ph = st.empty()
            status_ph = st.empty()
            prog = st.progress(0, text="Starting…")

            done = 0
            cur = start_dt
            while cur <= end_dt:
                try:
                    df_day = _cached_atoken(wallet, atoken_addr, cur.isoformat(), cur.isoformat(), INFURA_URL, int(token_decimals))
                except Exception as e:
                    prog.empty(); status_ph.error(f"Failed on {cur}: {e}"); return

                if df_day is not None and not df_day.empty:
                    df_day["token_address"] = atoken_addr
                    acc = st.session_state["atoken_accum"]
                    acc = pd.concat([acc, df_day], ignore_index=True)
                    acc.drop_duplicates(subset=["date","start_block","end_block","token_address"], keep="last", inplace=True)
                    acc.sort_values(["token_address","date"], inplace=True)
                    acc.reset_index(drop=True, inplace=True)
                    st.session_state["atoken_accum"] = acc

                    table_ph.dataframe(acc, use_container_width=True)
                    status_ph.info(f"Fetched {cur} ({len(df_day)} row)")

                done += 1
                prog.progress(min(int(done / total_days * 100), 100), text=f"Processed {done}/{total_days} day(s)…")
                cur = cur + timedelta(days=1)

            prog.empty()
            status_ph.success(f"Added window: {start_dt} → {end_dt}. Total rows: {len(st.session_state['atoken_accum'])}")

        if run:
            if not wallet or not INFURA_URL or not atoken_addr:
                st.error("Enter wallet + INFURA_URL + aToken address.")
            else:
                run_window_and_stream_atoken(start_dt, end_dt)

        # Output
        accum = st.session_state["atoken_accum"]
        st.markdown("### Accumulated results (Aave aTokens)")
        if not accum.empty:
            st.dataframe(accum, use_container_width=True)
            st.download_button(
                "Download accumulated CSV",
                accum.to_csv(index=False),
                file_name="aave_atokens_interest_accumulated.csv",
                mime="text/csv",
            )
        else:
            st.info("No results yet. Choose a token, a date window, then run.")

# ======================================================
#                      SETTINGS
# ======================================================
with proto_tabs[2]:
    st.header("Settings & Diagnostics")
    st.write("Use this to sanity-check your provider without running the full job.")
    with st.expander("🔧 Test RPC"):
        if st.button("Ping provider"):
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
