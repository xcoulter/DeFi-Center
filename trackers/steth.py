import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

STETH_CONTRACT = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
TRANSFER_SIG   = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
WEI_PER_STETH  = 10**18

# ---- Minimal retry wrapper (helps on Streamlit Cloud with free Infura) ----
def _rpc(infura_url: str, method: str, params, tries: int = 6, timeout: float = 20.0):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    backoff = 0.6
    for i in range(tries):
        try:
            r = requests.post(infura_url, json=payload, timeout=timeout)
            r.raise_for_status()
            js = r.json()
            if "error" in js:
                # backoff and retry on provider-side errors
                time.sleep(backoff)
                backoff = min(backoff * 1.8, 8.0)
                continue
            return js["result"]
        except Exception:
            time.sleep(backoff)
            backoff = min(backoff * 1.8, 8.0)
    raise RuntimeError(f"RPC failed: {method}")

def _block_by_time(infura_url: str, ts: int, mode: str = "before") -> int:
    latest = int(_rpc(infura_url, "eth_blockNumber", []), 16)
    lo, hi = 0, latest
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        blk = _rpc(infura_url, "eth_getBlockByNumber", [hex(mid), False])
        tsm = int(blk["timestamp"], 16)
        if tsm == ts:
            return mid
        elif tsm < ts:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return best if mode == "before" else (best + 1 if best is not None else 0)

def _balance_of(infura_url: str, wallet: str, block_num: int) -> int:
    # balanceOf(address) selector 0x70a08231 + 12-byte pad + address
    selector = "0x70a08231" + "0"*24 + wallet.lower()[2:]
    res = _rpc(infura_url, "eth_call", [{"to": STETH_CONTRACT, "data": selector}, hex(block_num)])
    return int(res, 16)

def _get_logs(infura_url: str, start_block: int, end_block: int, topics):
    # Single-range getLogs; for free tier keep ranges per-day to avoid pagination issues
    params = [{
        "fromBlock": hex(start_block),
        "toBlock":   hex(end_block),
        "address":   STETH_CONTRACT,
        "topics":    topics
    }]
    return _rpc(infura_url, "eth_getLogs", params)

def _sum_transfers(infura_url: str, wallet: str, start_block: int, end_block: int):
    # Two-query approach (to mirror your Apps Script exactly)
    wallet_topic = "0x" + "0"*24 + wallet.lower()[2:]

    in_logs  = _get_logs(infura_url, start_block, end_block, [TRANSFER_SIG, None, wallet_topic])
    out_logs = _get_logs(infura_url, start_block, end_block, [TRANSFER_SIG, wallet_topic])

    in_wei  = 0
    out_wei = 0
    for l in in_logs:
        in_wei  += int(l["data"], 16)
    for l in out_logs:
        out_wei += int(l["data"], 16)
    return in_wei, out_wei

def get_steth_rebases(wallet: str, days_back: int = 30, infura_url: str | None = None) -> pd.DataFrame:
    infura_url = infura_url or os.getenv("INFURA_URL", "").strip()
    if not infura_url:
        raise RuntimeError("Missing INFURA_URL (set Streamlit secret or environment variable).")

    today = datetime.now(timezone.utc).date()
    rows = []

    for d in range(days_back):
        day = today - timedelta(days=d+1)

        # UTC day boundaries (you can swap to Europe/Zurich by changing tz if desired)
        start_ts = int(datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc).timestamp())
        end_ts   = int(datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())

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
            "date": day.isoformat(),
            "start_block": start_blk,
            "end_block": end_blk,
            "start_balance_steth": start_bal / WEI_PER_STETH,
            "end_balance_steth":   end_bal   / WEI_PER_STETH,
            "transfers_in_steth":  in_wei    / WEI_PER_STETH,
            "transfers_out_steth": out_wei   / WEI_PER_STETH,
            "net_transfers_steth": net_wei   / WEI_PER_STETH,
            "daily_rebase_reward_steth": rebase_wei / WEI_PER_STETH,
        })

    df = pd.DataFrame(rows).iloc[::-1].reset_index(drop=True)
    return df
