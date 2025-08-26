import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone, date

STETH_CONTRACT = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
TRANSFER_SIG   = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
WEI_PER_STETH  = 10**18
STETH_GENESIS_UTC = datetime(2020, 12, 1, tzinfo=timezone.utc)  # search lower bound

# ---------------- RPC helper (with 429 handling) ----------------

def _rpc(infura_url: str, method: str, params, tries: int = 5, timeout: float = 12.0):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    backoff = 0.7
    last_err = None
    for _ in range(tries):
        try:
            r = requests.post(infura_url, json=payload, timeout=timeout)
            if r.status_code == 429:
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

# ---------------- Logs (chunked & gentle pacing) ----------------

def _get_logs_chunked(infura_url: str, start_block: int, end_block: int, topics,
                      stop_at_first: bool = False, chunk_size: int = 1000):
    out = []
    b = start_block
    INTER_CHUNK_SLEEP = 0.35
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
        time.sleep(INTER_CHUNK_SLEEP)
    return out

# ---------------- First activity helpers ----------------

def _find_first_nonzero_balance_block(infura_url: str, wallet: str) -> int | None:
    latest = _latest_block(infura_url)
    if latest == 0:
        return None
    genesis_ts = int(STETH_GENESIS_UTC.timestamp())
    lo = _block_by_time(infura_url, genesis_ts, "after")
    if lo <= 0: lo = 1
    hi = latest
    ans = None
    while lo <= hi:
        mid = (lo + hi) // 2
        bal = _balance_of(infura_url, wallet, mid)
        if bal > 0:
            ans = mid
            hi = mid - 1
        else:
            lo = mid + 1
    return ans

def get_first_activity_date(wallet: str, infura_url: str) -> date | None:
    """UTC date of the earliest non-zero stETH balance; None if never held."""
    fb = _find_first_nonzero_balance_block(infura_url, wallet)
    if fb is None:
        return None
    blk_obj = _rpc(infura_url, "eth_getBlockByNumber", [hex(fb), False])
    ts = int(blk_obj["timestamp"], 16)
    return datetime.fromtimestamp(ts, tz=timezone.utc).date()

# ---------------- Daily math ----------------

def _sum_transfers(infura_url: str, wallet: str, start_block: int, end_block: int):
    wtopic = "0x" + "0"*24 + wallet.lower()[2:]
    in_logs  = _get_logs_chunked(infura_url, start_block, end_block, [TRANSFER_SIG, None, wtopic]) or []
    out_logs = _get_logs_chunked(infura_url, start_block, end_block, [TRANSFER_SIG, wtopic]) or []
    in_wei  = sum(int(l["data"], 16) for l in in_logs)
    out_wei = sum(int(l["data"], 16) for l in out_logs)
    return in_wei, out_wei

def _iterate_days(start_dt: date, end_dt: date):
    cur = start_dt
    one = timedelta(days=1)
    while cur <= end_dt:
        yield cur
        cur = cur + one

# ---------------- Public API ----------------

def get_steth_rebases_range(wallet: str, start_iso: str, end_iso: str, infura_url: str | None = None) -> pd.DataFrame:
    """
    Compute daily stETH rebases for [start_iso, end_iso] inclusive (UTC dates, 'YYYY-MM-DD').
    Math: rebase = (end_bal - start_bal) - (sum_in - sum_out)
    """
    infura_url = (infura_url or os.getenv("INFURA_URL", "")).strip()
    if not infura_url:
        raise RuntimeError("Missing INFURA_URL")

    start_dt = datetime.strptime(start_iso, "%Y-%m-%d").date()
    end_dt   = datetime.strptime(end_iso,   "%Y-%m-%d").date()
    if end_dt < start_dt:
        return pd.DataFrame([])

    latest = _latest_block(infura_url)
    latest_ts = int(_rpc(infura_url, "eth_getBlockByNumber", [hex(latest), False])["timestamp"], 16)

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

    return pd.DataFrame(rows)
