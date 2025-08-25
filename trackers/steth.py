import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

STETH_CONTRACT = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
TRANSFER_SIG   = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
WEI_PER_STETH  = 10**18

# Use a conservative lower bound near stETH launch to avoid scanning from genesis.
STETH_GENESIS_UTC = datetime(2020, 12, 1, tzinfo=timezone.utc)

# ---------------- RPC helpers (with light retry/backoff) ----------------

def _rpc(infura_url: str, method: str, params, tries: int = 5, timeout: float = 18.0):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    backoff = 0.6
    for _ in range(tries):
        try:
            r = requests.post(infura_url, json=payload, timeout=timeout)
            r.raise_for_status()
            js = r.json()
            if "error" in js:
                time.sleep(backoff); backoff = min(backoff * 1.8, 8.0); continue
            return js["result"]
        except Exception:
            time.sleep(backoff); backoff = min(backoff * 1.8, 8.0)
    raise RuntimeError(f"RPC failed: {method}")

def _latest_block(infura_url: str) -> int:
    return int(_rpc(infura_url, "eth_blockNumber", []), 16)

def _block_by_time(infura_url: str, ts: int, mode: str = "before") -> int:
    """
    Binary search: mode='before' => max block with ts <= target
                   mode='after'  => min block with ts >= target
    """
    latest = _latest_block(infura_url)
    lo, hi = 0, latest
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        blk = _rpc(infura_url, "eth_getBlockByNumber", [hex(mid), False])
        tsm = int(blk["timestamp"], 16)
        if tsm == ts:
            if mode == "before":
                best = mid; lo = mid + 1
            else:
                best = mid; hi = mid - 1
        elif tsm < ts:
            if mode == "before": best = mid
            lo = mid + 1
        else:
            if mode == "after": best = mid
            hi = mid - 1
    return 0 if best is None else best

def _balance_of(infura_url: str, wallet: str, block_num: int) -> int:
    # balanceOf(address) selector 0x70a08231 + 12-byte pad + address
    selector = "0x70a08231" + "0"*24 + wallet.lower()[2:]
    res = _rpc(infura_url, "eth_call", [{"to": STETH_CONTRACT, "data": selector}, hex(block_num)])
    return int(res, 16)

# ---------------- Logs (chunked to avoid Infura free-tier limits) ----------------

def _get_logs_chunked(infura_url: str, start_block: int, end_block: int, topics,
                      stop_at_first: bool = False, chunk_size: int = 5000):
    out = []
    b = start_block
    while b <= end_block:
        e = min(end_block, b + chunk_size - 1)
        params = [{
            "fromBlock": hex(b),
            "toBlock":   hex(e),
            "address":   STETH_CONTRACT,
            "topics":    topics
        }]
        logs = _rpc(infura_url, "eth_getLogs", params) or []
        if stop_at_first and logs:
            return [logs[0]]
        if logs:
            out.extend(logs)
        b = e + 1
    return out

def _first_log_in_range(infura_url: str, wallet: str, lo: int, hi: int):
    """Return earliest block (int) in [lo,hi] with a Transfer to/from wallet, else None."""
    wtopic = "0x" + "0"*24 + wallet.lower()[2:]
    cand = None

    # inbound
    li = _get_logs_chunked(infura_url, lo, hi, [TRANSFER_SIG, None, wtopic], stop_at_first=True)
    if li:
        cand = int(li[0]["blockNumber"], 16)

    # outbound
    lo2 = _get_logs_chunked(infura_url, lo, hi, [TRANSFER_SIG, wtopic], stop_at_first=True)
    if lo2:
        b2 = int(lo2[0]["blockNumber"], 16)
        cand = b2 if cand is None else min(cand, b2)

    return cand

# ------------- Find earliest stETH activity for a wallet (chunked + refine) -------------

def _find_first_activity_block(infura_url: str, wallet: str) -> int | None:
    latest = _latest_block(infura_url)
    if latest == 0:
        return None

    # Start scanning near stETH launch to reduce range.
    start_ts = int(STETH_GENESIS_UTC.timestamp())
    lo = _block_by_time(infura_url, start_ts, "after")
    if lo <= 0: lo = 1

    SPAN = 50000  # scan window
    cur = lo
    while cur <= latest:
        hi = min(latest, cur + SPAN - 1)
        first = _first_log_in_range(infura_url, wallet, cur, hi)
        if first is not None:
            # refine with binary search inside [cur, first]
            ans, L, R = first, cur, first
            while L <= R:
                mid = (L + R) // 2
                hit = _first_log_in_range(infura_url, wallet, L, mid)
                if hit is not None:
                    ans = min(ans, hit)
                    R = hit - 1
                else:
                    L = mid + 1
            return ans
        cur = hi + 1

    return None

# ------------- Sum transfers for a day (same math as your Apps Script) -------------

def _sum_transfers(infura_url: str, wallet: str, start_block: int, end_block: int):
    wtopic = "0x" + "0"*24 + wallet.lower()[2:]
    in_logs  = _get_logs_chunked(infura_url, start_block, end_block, [TRANSFER_SIG, None, wtopic]) or []
    out_logs = _get_logs_chunked(infura_url, start_block, end_block, [TRANSFER_SIG, wtopic]) or []

    in_wei  = 0
    out_wei = 0
    for l in in_logs:
        in_wei  += int(l["data"], 16)
    for l in out_logs:
        out_wei += int(l["data"], 16)
    return in_wei, out_wei

# ------------- Public function: full history from first activity -------------

def get_steth_rebases_from_first_activity(wallet: str, infura_url: str | None = None) -> pd.DataFrame:
    infura_url = (infura_url or os.getenv("INFURA_URL", "")).strip()
    if not infura_url:
        raise RuntimeError("Missing INFURA_URL")

    first_block = _find_first_activity_block(infura_url, wallet)
    if first_block is None:
        return pd.DataFrame([])

    # Day 0 = UTC date of first activity
    first_blk_obj = _rpc(infura_url, "eth_getBlockByNumber", [hex(first_block), False])
    first_ts = int(first_blk_obj["timestamp"], 16)
    start_date = datetime.fromtimestamp(first_ts, tz=timezone.utc).date()
    today = datetime.now(timezone.utc).date()

    rows = []
    latest = _latest_block(infura_url)
    latest_ts = int(_rpc(infura_url, "eth_getBlockByNumber", [hex(latest), False])["timestamp"], 16)

    d = start_date
    while d <= today:
        # UTC day window
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

        start_bal = _balance_of(infura_url, wallet, start_blk)
        end_bal   = _balance_of(infura_url, wallet, end_blk)

        in_wei, out_wei = _sum_transfers(infura_url, wallet, start_blk, end_blk)
        net_wei    = in_wei - out_wei
        rebase_wei = (end_bal - start_bal) - net_wei

        rows.append({
            "date": d.isoformat(),
            "start_block": start_blk,
            "end_block": end_blk,
            "start_balance_steth": start_bal / WEI_PER_STETH,
            "end_balance_steth":   end_bal   / WEI_PER_STETH,
            "transfers_in_steth":  in_wei    / WEI_PER_STETH,
            "transfers_out_steth": out_wei   / WEI_PER_STETH,
            "net_transfers_steth": net_wei   / WEI_PER_STETH,
            "daily_rebase_reward_steth": rebase_wei / WEI_PER_STETH,
        })

        d = d + timedelta(days=1)

    return pd.DataFrame(rows)
