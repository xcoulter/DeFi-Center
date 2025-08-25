import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

STETH_CONTRACT = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
TRANSFER_SIG   = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
WEI_PER_STETH  = 10**18

# ---------------- RPC helpers (with light retry/backoff) ----------------

def _rpc(infura_url: str, method: str, params, tries: int = 5, timeout: float = 20.0):
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
                # advance right to include this block
                best = mid
                lo = mid + 1
            else:
                # advance left to find earliest >=
                best = mid
                hi = mid - 1
        elif tsm < ts:
            if mode == "before":
                best = mid
            lo = mid + 1
        else:
            if mode == "after":
                best = mid
            hi = mid - 1
    if best is None:
        return 0
    return best

def _balance_of(infura_url: str, wallet: str, block_num: int) -> int:
    # balanceOf(address) selector 0x70a08231 + 12-byte pad + address
    selector = "0x70a08231" + "0"*24 + wallet.lower()[2:]
    res = _rpc(infura_url, "eth_call", [{"to": STETH_CONTRACT, "data": selector}, hex(block_num)])
    return int(res, 16)

def _get_logs(infura_url: str, start_block: int, end_block: int, topics):
    params = [{
        "fromBlock": hex(start_block),
        "toBlock":   hex(end_block),
        "address":   STETH_CONTRACT,
        "topics":    topics
    }]
    return _rpc(infura_url, "eth_getLogs", params)

# ------------- Find earliest stETH activity for a wallet -------------

def _has_activity_in_range(infura_url: str, wallet: str, lo: int, hi: int) -> int | None:
    """Return first block number within [lo,hi] that has a Transfer involving wallet, else None."""
    wtopic = "0x" + "0"*24 + wallet.lower()[2:]
    # Try inbound first
    logs_in = _get_logs(infura_url, lo, hi, [TRANSFER_SIG, None, wtopic]) or []
    if logs_in:
        return int(logs_in[0]["blockNumber"], 16)
    # Then outbound
    logs_out = _get_logs(infura_url, lo, hi, [TRANSFER_SIG, wtopic]) or []
    if logs_out:
        return int(logs_out[0]["blockNumber"], 16)
    return None

def _find_first_activity_block(infura_url: str, wallet: str) -> int | None:
    latest = _latest_block(infura_url)
    if latest == 0:
        return None

    # Exponential scan to find a window that contains activity
    step = max(1, latest // 32)
    found_lo, found_hi, hit = 1, latest, False
    for start in range(1, latest + 1, step):
        end = min(latest, start + step - 1)
        b = _has_activity_in_range(infura_url, wallet, start, end)
        if b is not None:
            found_lo, found_hi, hit = start, end, True
            break
    if not hit:
        return None

    # Binary narrow to earliest
    ans = found_hi
    lo, hi = found_lo, found_hi
    while lo <= hi:
        mid = (lo + hi) // 2
        b = _has_activity_in_range(infura_url, wallet, lo, mid)
        if b is not None:
            ans = min(ans, b)
            hi = mid - 1
        else:
            lo = mid + 1
    return ans

# ------------- Sum transfers for a day (same as your Apps Script) -------------

def _sum_transfers(infura_url: str, wallet: str, start_block: int, end_block: int):
    wallet_topic = "0x" + "0"*24 + wallet.lower()[2:]
    in_logs  = _get_logs(infura_url, start_block, end_block, [TRANSFER_SIG, None, wallet_topic]) or []
    out_logs = _get_logs(infura_url, start_block, end_block, [TRANSFER_SIG, wallet_topic]) or []

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
    first_block_info = _rpc(infura_url, "eth_getBlockByNumber", [hex(first_block), False])
    first_ts = int(first_block_info["timestamp"], 16)
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
        net_wei = in_wei - out_wei
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

    df = pd.DataFrame(rows)
    return df
