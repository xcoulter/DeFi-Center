import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Tuple, Iterable, Set

# ─────────────────────────── Constants ───────────────────────────
TRANSFER_SIG = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_ADDR_TOPIC = "0x" + "0" * 64  # 32-byte topic for address(0)

# Ethereum mainnet Aave v3 default counterparties (underlying flows)
AAVE_ETH_V3_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2".lower()
AAVE_ETH_V3_WRAPPED_TOKEN_GATEWAY = "0xd01607c3C5eCABa394D8be377a08590149325722".lower()
DEFAULT_AAVE_ETH_V3_COUNTERPARTIES: Set[str] = {
    AAVE_ETH_V3_POOL,
    AAVE_ETH_V3_WRAPPED_TOKEN_GATEWAY,
}

# ───────────────────────────── Utils ─────────────────────────────
def _int_hex_safe(x: Optional[str]) -> int:
    if not x or x == "0x":
        return 0
    return int(x, 16)

def _addr_topic(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").rjust(64, "0")

def _topics_to_addresses(topics):
    # returns (from, to)
    def _t2a(t):
        return "0x" + (t[-40:] if isinstance(t, str) and t.startswith("0x") else "").lower()
    if not topics or len(topics) < 3:
        return None, None
    return _t2a(topics[1]), _t2a(topics[2])

# ───────────────────────────── RPC ──────────────────────────────
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
                time.sleep(backoff); backoff = min(backoff * 1.8, 10.0)
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
    code = _rpc(infura_url, "eth_getCode", [token, "latest"])
    if not code or code == "0x":
        raise RuntimeError(f"Token {token} has no bytecode on this RPC network (wrong chain?).")

def _latest_block(infura_url: str) -> int:
    return _int_hex_safe(_rpc(infura_url, "eth_blockNumber", []))

def _block_by_time(infura_url: str, ts: int, mode: str = "before") -> int:
    """Binary search: 'before' => max block ts<=target, 'after' => min block ts>=target"""
    latest = _latest_block(infura_url)
    lo, hi = 0, latest
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        blk = _rpc(infura_url, "eth_getBlockByNumber", [hex(mid), False])
        tsm = _int_hex_safe(blk.get("timestamp"))
        if tsm == ts:
            best = mid
            if mode == "before": lo = mid + 1
            else: hi = mid - 1
        elif tsm < ts:
            if mode == "before": best = mid
            lo = mid + 1
        else:
            if mode == "after": best = mid
            hi = mid - 1
    return 0 if best is None else best

def _balance_of(infura_url: str, token: str, wallet: str, block_num: int) -> int:
    # balanceOf(address) selector 0x70a08231 + 12-byte pad + address
    selector = "0x70a08231" + "0"*24 + wallet.lower()[2:]
    res = _rpc(infura_url, "eth_call", [{"to": token, "data": selector}, hex(block_num)])
    return _int_hex_safe(res)

# ───────────────────────────── Logs ─────────────────────────────
def _get_logs_chunked(infura_url: str, token: str, start_block: int, end_block: int, topics,
                      stop_at_first: bool = False, chunk_size: int = 50_000):
    out = []
    b = start_block
    INTER_CHUNK_SLEEP = 0.25
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

# ────────────────── First-activity (aToken) ────────────────────
def _find_first_nonzero_balance_block(infura_url: str, token: str, wallet: str) -> Optional[int]:
    latest = _latest_block(infura_url)
    if latest == 0:
        return None
    genesis_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
    lo = _block_by_time(infura_url, genesis_ts, "after")
    if lo <= 0: lo = 1
    hi = latest
    ans = None
    while lo <= hi:
        mid = (lo + hi) // 2
        bal = _balance_of(infura_url, token, wallet, mid)
        if bal > 0:
            ans = mid; hi = mid - 1
        else:
            lo = mid + 1
    return ans

def get_first_activity_date_atoken(wallet: str, token: str, infura_url: Optional[str] = None) -> Optional[date]:
    """UTC date of earliest non-zero aToken balance; fallback to earliest Transfer if none found."""
    infura_url = (infura_url or os.getenv("INFURA_URL","")).strip()
    if not infura_url: raise RuntimeError("Missing INFURA_URL")
    _assert_token_on_network(infura_url, token)

    fb = _find_first_nonzero_balance_block(infura_url, token, wallet)
    if fb is not None:
        blk_obj = _rpc(infura_url, "eth_getBlockByNumber", [hex(fb), False])
        ts = _int_hex_safe(blk_obj.get("timestamp"))
        return datetime.fromtimestamp(ts, tz=timezone.utc).date()

    latest = _latest_block(infura_url)
    genesis_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
    start_blk = _block_by_time(infura_url, genesis_ts, "after") or 1
    wallet_topic = _addr_topic(wallet)

    first_log = None
    logs_in = _get_logs_chunked(infura_url, token, start_blk, latest,
                                [TRANSFER_SIG, None, wallet_topic],
                                stop_at_first=True)
    if logs_in: first_log = logs_in[0]
    if not first_log:
        logs_out = _get_logs_chunked(infura_url, token, start_blk, latest,
                                     [TRANSFER_SIG, wallet_topic, None],
                                     stop_at_first=True)
        if logs_out: first_log = logs_out[0]
    if not first_log: return None

    blk_num = _int_hex_safe(first_log.get("blockNumber"))
    blk_obj = _rpc(infura_url, "eth_getBlockByNumber", [hex(blk_num), False])
    ts = _int_hex_safe(blk_obj.get("timestamp"))
    return datet
