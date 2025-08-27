# trackers/ausdc.py
import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Tuple

# This module is token-agnostic (works for any Aave aToken: aUSDC, aWETH, etc.)
TRANSFER_SIG = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_ADDR_TOPIC = "0x" + "0" * 64  # 32-byte topic for address(0)

# ============================ Utils ============================

def _int_hex_safe(x: Optional[str]) -> int:
    """Parse hex like '0x0' safely; treat bare '0x' / None as 0."""
    if not x or x == "0x":
        return 0
    return int(x, 16)

# ============================ RPC ==============================

def _rpc(infura_url: str, method: str, params, tries: int = 5, timeout: float = 12.0):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    backoff = 0.7
    last_err = None
    for _ in range(tries):
        try:
            r = requests.post(infura_url, json=payload, timeout=timeout)
            if r.status_code == 429:
                # Respect rate limiting
                ra = r.headers.get("Retry-After")
                sleep_s = float(ra) if ra else backoff
                time.sleep(sleep_s)
                backoff = min(backoff * 1.8, 10.0)
                continue
            r.raise_for_status()
            js = r.json()
            if "error" in js:
                last_err = js["error"]
                time.sleep(backoff)
                backoff = min(backoff * 1.8, 10.0)
                continue
            return js["result"]
        except requests.exceptions.HTTPError as e:
            last_err = f"HTTP {getattr(e.response,'status_code',None)}"
            time.sleep(backoff); backoff = min(backoff * 1.8, 10.0)
        except Exception as e:
            last_err = str(e)
            time.sleep(backoff); backoff = min(backoff * 1.8, 10.0)
    raise RuntimeError(f"RPC failed: {method} ({last_err})")

def _assert_token_on_network(infura_url: str, token: str):
    """Raise if the token has no bytecode on the connected chain."""
    code = _rpc(infura_url, "eth_getCode", [token, "latest"])
    if not code or code == "0x":
        raise RuntimeError(f"Token {token} has no bytecode on this RPC network (wrong chain?).")

def _latest_block(infura_url: str) -> int:
    return _int_hex_safe(_rpc(infura_url, "eth_blockNumber", []))

def _block_by_time(infura_url: str, ts: int, mode: str = "before") -> int:
    """
    Binary search by timestamp:
      - 'before': max block with ts <= target
      - 'after' : min block with ts >= target
    """
    latest = _latest_block(infura_url)
    lo, hi = 0, latest
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        blk = _rpc(infura_url, "eth_getBlockByNumber", [hex(mid), False])
        tsm = _int_hex_safe(blk.get("timestamp"))
        if tsm == ts:
            best = mid
            if mode == "before":
                lo = mid + 1
            else:
                hi = mid - 1
        elif tsm < ts:
            if mode == "before":
                best = mid
            lo = mid + 1
        else:
            if mode == "after":
                best = mid
            hi = mid - 1
    return 0 if best is None else best

def _balance_of(infura_url: str, token: str, wallet: str, block_num: int) -> int:
    # balanceOf(address) selector 0x70a08231 + 12-byte pad + address (lowercased)
    selector = "0x70a08231" + "0"*24 + wallet.lower()[2:]
    res = _rpc(infura_url, "eth_call", [{"to": token, "data": selector}, hex(block_num)])
    return _int_hex_safe(res)

# ============================ Logs =============================

def _get_logs_chunked(
    infura_url: str,
    token: str,
    start_block: int,
    end_block: int,
    topics,
    stop_at_first: bool = False,
    chunk_size: int = 1000
):
    """Gentle, chunked log retrieval with optional early stop."""
    out = []
    b = start_block
    INTER_CHUNK_SLEEP = 0.35
    while b <= end_block:
        e = min(end_block, b + chunk_size - 1)
        params = [{
            "fromBlock": hex(b),
            "toBlock":   hex(e),
            "address":   token,
            "topics":    topics
        }]
        logs = _rpc(infura_url, "eth_getLogs", params) or []
        if logs:
            if stop_at_first:
                return [logs[0]]
            out.extend(logs)
        b = e + 1
        time.sleep(INTER_CHUNK_SLEEP)
    return out

# ====================== First-activity search ===================

def _find_first_nonzero_balance_block(infura_url: str, token: str, wallet: str) -> Optional[int]:
    latest = _latest_block(infura_url)
    if latest == 0:
        return None
    # Start around 2020 (covers Aave v2+ launch era on Ethereum)
    genesis_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
    lo = _block_by_time(infura_url, genesis_ts, "after")
    if lo <= 0:
        lo = 1
    hi = latest
    ans = None
    while lo <= hi:
        mid = (lo + hi) // 2
        bal = _balance_of(infura_url, token, wallet, mid)
        if bal > 0:
            ans = mid
            hi = mid - 1
        else:
            lo = mid + 1
    return ans

def get_first_activity_date_atoken(wallet: str, token: str, infura_url: Optional[str] = None) -> Optional[date]:
    """
    Returns the UTC date of earliest interaction with the aToken:
      1) Try earliest non-zero balance via binary search.
      2) If none found (e.g., position opened & closed quickly), fall back to earliest Transfer log.
    """
    infura_url = (infura_url or os.getenv("INFURA_URL", "")).strip()
    if not infura_url:
        raise RuntimeError("Missing INFURA_URL")

    # Ensure token exists on this chain
    _assert_token_on_network(infura_url, token)

    # 1) Balance-based earliest block
    fb = _find_first_nonzero_balance_block(infura_url, token, wallet)
    if fb is not None:
        blk_obj = _rpc(infura_url, "eth_getBlockByNumber", [hex(fb), False])
        ts = _int_hex_safe(blk_obj.get("timestamp"))
        return datetime.fromtimestamp(ts, tz=timezone.utc).date()

    # 2) Fallback: earliest Transfer involving wallet (to or from)
    latest = _latest_block(infura_url)
    genesis_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
    start_blk = _block_by_time(infura_url, genesis_ts, "after")
    if start_blk <= 0:
        start_blk = 1

    wallet_topic = "0x" + wallet.lower()[2:].rjust(64, "0")

    # Try "to = wallet" first
    logs_in = _get_logs_chunked(
        infura_url, token, start_blk, latest,
        [TRANSFER_SIG, None, wallet_topic],
        stop_at_first=True, chunk_size=50_000
    )
    first_log = logs_in[0] if logs_in else None

    if not first_log:
        # Try "from = wallet"
        logs_out = _get_logs_chunked(
            infura_url, token, start_blk, latest,
            [TRANSFER_SIG, wallet_topic, None],
            stop_at_first=True, chunk_size=50_000
        )
        first_log = logs_out[0] if logs_out else None

    if not first_log:
        return None  # truly no activity on this chain/token

    blk_num = _int_hex_safe(first_log.get("blockNumber"))
    blk_obj = _rpc(infura_url, "eth_getBlockByNumber", [hex(blk_num), False])
    ts = _int_hex_safe(blk_obj.get("timestamp"))
    return datetime.fromtimestamp(ts, tz=timezone.utc).date()

# ====================== Daily interest range ====================

def _deposits_withdrawals_for_day(
    infura_url: str, token: str, wallet: str, start_block: int, end_block: int
) -> Tuple[int, int]:
    """
    Deposits = mints to wallet (Transfer from 0x0 -> wallet).
    Withdrawals = burns from wallet (Transfer from wallet -> 0x0).
    Other aToken transfers are ignored by spec.
    """
    wtopic = "0x" + "0"*24 + wallet.lower()[2:]

    # deposits: from = zero, to = wallet
    dep_logs = _get_logs_chunked(infura_url, token, start_block, end_block,
                                 [TRANSFER_SIG, ZERO_ADDR_TOPIC, wtopic]) or []
    # withdrawals: from = wallet, to = zero
    wdr_logs = _get_logs_chunked(infura_url, token, start_block, end_block,
                                 [TRANSFER_SIG, wtopic, ZERO_ADDR_TOPIC]) or []

    deposits = sum(_int_hex_safe(l.get("data")) for l in dep_logs)
    withdrawals = sum(_int_hex_safe(l.get("data")) for l in wdr_logs)
    return deposits, withdrawals

def _iterate_days(start_dt: date, end_dt: date):
    cur = start_dt
    one = timedelta(days=1)
    while cur <= end_dt:
        yield cur
        cur = cur + one

def get_atoken_interest_range(
    wallet: str,
    token: str,
    start_iso: str,
    end_iso: str,
    infura_url: Optional[str] = None,
    decimals: int = 18,
) -> pd.DataFrame:
    """
    Generic aToken daily interest calculator (aUSDC, aWETH, etc.) for [start_iso, end_iso] inclusive (UTC).
    balanceOf returns underlying-equivalent amount (accrues via index).
    interest = (end_balance - start_balance) - withdrawals + deposits
    Returns neutral (token-agnostic) column names.
    """
    infura_url = (infura_url or os.getenv("INFURA_URL", "")).strip()
    if not infura_url:
        raise RuntimeError("Missing INFURA_URL")

    UNIT = 10 ** int(decimals)

    start_dt = datetime.strptime(start_iso, "%Y-%m-%d").date()
    end_dt   = datetime.strptime(end_iso,   "%Y-%m-%d").date()
    if end_dt < start_dt:
        return pd.DataFrame([])

    latest = _latest_block(infura_url)
    latest_ts = _int_hex_safe(_rpc(infura_url, "eth_getBlockByNumber", [hex(latest), False]).get("timestamp"))

    rows = []
    for d in _iterate_days(start_dt, end_dt):
        # UTC day bounds
        start_ts = int(datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc).timestamp())
        end_ts   = int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        if start_ts > latest_ts:
            break
        if end_ts > latest_ts:
            end_ts = latest_ts

        start_blk = _block_by_time(infura_url, start_ts, "after")
        end_blk   = _block_by_time(infura_url, end_ts,   "before")
        if end_blk < start_blk:
            end_blk = start_blk

        start_bal = _balance_of(infura_url, token, wallet, start_blk)
        end_bal   = _balance_of(infura_url, token, wallet, end_blk)

        deposits_wei, withdrawals_wei = _deposits_withdrawals_for_day(
            infura_url, token, wallet, start_blk, end_blk
        )

        # interest = Δbalance − withdrawals + deposits
        interest_wei = (end_bal - start_bal) - withdrawals_wei + deposits_wei

        rows.append({
            "date": d.isoformat(),
            "start_block": start_blk,
            "end_block": end_blk,
            "start_balance":   start_bal / UNIT,
            "end_balance":     end_bal   / UNIT,
            "deposits":        deposits_wei    / UNIT,
            "withdrawals":     withdrawals_wei / UNIT,
            "net_transfers":   (withdrawals_wei - deposits_wei) / UNIT,  # positive reduces position
            "daily_interest":  interest_wei / UNIT,
        })

    return pd.DataFrame(rows)
