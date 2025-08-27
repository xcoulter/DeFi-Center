# trackers/ausdc.py
import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Tuple, Iterable, Set

# ─────────────────────────── Constants ───────────────────────────

TRANSFER_SIG = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_ADDR_TOPIC = "0x" + "0" * 64  # 32-byte topic for address(0)

# Ethereum mainnet Aave v3 default counterparties (you can add more at call time)
AAVE_ETH_V3_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2".lower()
AAVE_ETH_V3_WRAPPED_TOKEN_GATEWAY = "0xd01607c3C5eCABa394D8be377a08590149325722".lower()

DEFAULT_AAVE_ETH_V3_COUNTERPARTIES: Set[str] = {
    AAVE_ETH_V3_POOL,
    AAVE_ETH_V3_WRAPPED_TOKEN_GATEWAY,
}

# ───────────────────────────── Utils ─────────────────────────────

def _int_hex_safe(x: Optional[str]) -> int:
    """Parse hex like '0x0' safely; treat bare '0x'/None as 0."""
    if not x or x == "0x":
        return 0
    return int(x, 16)

def _addr_topic(addr: str) -> str:
    """Address -> topic32 (lowercased, left-padded)."""
    return "0x" + addr.lower().replace("0x", "").rjust(64, "0")

def _topics_to_addresses(topics):
    """
    Transfer(address indexed from, address indexed to, uint256 value)
    topics[1] = from, topics[2] = to
    Returns (from_addr, to_addr) lowercased '0x...' (or (None, None)).
    """
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

# ───────────────────────────── Logs ─────────────────────────────

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

# ────────────────── First-activity (aToken) ────────────────────

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
    Earliest date the wallet had a non-zero aToken (or any Transfer involving the wallet if balance search fails).
    """
    infura_url = (infura_url or os.getenv("INFURA_URL", "")).strip()
    if not infura_url:
        raise RuntimeError("Missing INFURA_URL")

    _assert_token_on_network(infura_url, token)

    fb = _find_first_nonzero_balance_block(infura_url, token, wallet)
    if fb is not None:
        blk_obj = _rpc(infura_url, "eth_getBlockByNumber", [hex(fb), False])
        ts = _int_hex_safe(blk_obj.get("timestamp"))
        return datetime.fromtimestamp(ts, tz=timezone.utc).date()

    # Fallback: earliest Transfer involving wallet (to or from)
    latest = _latest_block(infura_url)
    genesis_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
    start_blk = _block_by_time(infura_url, genesis_ts, "after")
    if start_blk <= 0:
        start_blk = 1

    wallet_topic = _addr_topic(wallet)

    logs_in = _get_logs_chunked(
        infura_url, token, start_blk, latest,
        [TRANSFER_SIG, None, wallet_topic],
        stop_at_first=True, chunk_size=50_000
    )
    first_log = logs_in[0] if logs_in else None

    if not first_log:
        logs_out = _get_logs_chunked(
            infura_url, token, start_blk, latest,
            [TRANSFER_SIG, wallet_topic, None],
            stop_at_first=True, chunk_size=50_000
        )
        first_log = logs_out[0] if logs_out else None

    if not first_log:
        return None

    blk_num = _int_hex_safe(first_log.get("blockNumber"))
    blk_obj = _rpc(infura_url, "eth_getBlockByNumber", [hex(blk_num), False])
    ts = _int_hex_safe(blk_obj.get("timestamp"))
    return datetime.fromtimestamp(ts, tz=timezone.utc).date()

# ─────────────── aToken daily interest (agnostic) ───────────────

def _deposits_withdrawals_for_day(
    infura_url: str, token: str, wallet: str, start_block: int, end_block: int
) -> Tuple[int, int]:
    """
    Deposits = mints to wallet (Transfer from 0x0 -> wallet).
    Withdrawals = burns from wallet (Transfer from wallet -> 0x0).
    Fetch broader logs and classify locally (avoids zero-address topic quirks).
    """
    wtopic = _addr_topic(wallet)

    logs_to = _get_logs_chunked(
        infura_url, token, start_block, end_block,
        [TRANSFER_SIG, None, wtopic], stop_at_first=False, chunk_size=50_000
    ) or []
    logs_from = _get_logs_chunked(
        infura_url, token, start_block, end_block,
        [TRANSFER_SIG, wtopic, None], stop_at_first=False, chunk_size=50_000
    ) or []

    deposits_wei = 0
    withdrawals_wei = 0
    ZERO = ZERO_ADDR_TOPIC

    for l in logs_to:  # mints
        topics = l.get("topics") or []
        if len(topics) >= 3 and topics[1].lower() == ZERO and topics[2].lower() == wtopic:
            deposits_wei += _int_hex_safe(l.get("data"))

    for l in logs_from:  # burns
        topics = l.get("topics") or []
        if len(topics) >= 3 and topics[1].lower() == wtopic and topics[2].lower() == ZERO:
            withdrawals_wei += _int_hex_safe(l.get("data"))

    return deposits_wei, withdrawals_wei

def _iterate_days(start_dt: date, end_dt: date):
    cur = start_dt
    one = timedelta(days=1)
    while cur <= end_dt:
        yield cur
        cur = cur + one

def get_atoken_interest_range(
    wallet: str,
    token: str,       # aToken address (e.g., aUSDC, aWETH)
    start_iso: str,
    end_iso: str,
    infura_url: Optional[str] = None,
    decimals: int = 18,  # decimals of the UNDERLYING (aToken mirrors underlying units)
) -> pd.DataFrame:
    """
    Generic aToken daily interest for [start_iso, end_iso] inclusive (UTC).
    interest = (end_balance - start_balance) - (deposits - withdrawals)
             = (end_balance - start_balance) + (withdrawals - deposits)
    Columns: date, start_block, end_block, start_balance, end_balance, deposits, withdrawals, net_transfers, daily_interest
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
        start_ts = int(datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc).timestamp())
        end_ts   = int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        if start_ts > latest_ts: break
        if end_ts > latest_ts: end_ts = latest_ts

        start_blk = _block_by_time(infura_url, start_ts, "after")
        end_blk   = _block_by_time(infura_url, end_ts,   "before")
        if end_blk < start_blk: end_blk = start_blk

        start_bal = _balance_of(infura_url, token, wallet, start_blk)
        end_bal   = _balance_of(infura_url, token, wallet, end_blk)

        deposits_wei, withdrawals_wei = _deposits_withdrawals_for_day(
            infura_url, token, wallet, start_blk, end_blk
        )

        net_transfers_wei = withdrawals_wei - deposits_wei  # + reduces position
        interest_wei = (end_bal - start_bal) + net_transfers_wei

        rows.append({
            "date": d.isoformat(),
            "start_block": start_blk,
            "end_block": end_blk,
            "start_balance":   start_bal / UNIT,
            "end_balance":     end_bal   / UNIT,
            "deposits":        deposits_wei    / UNIT,
            "withdrawals":     withdrawals_wei / UNIT,
            "net_transfers":   net_transfers_wei / UNIT,
            "daily_interest":  interest_wei / UNIT,
        })

    return pd.DataFrame(rows)

# ─────── Underlying ERC-20 flows vs counterparties (agnostic) ───────

def get_underlying_flows_range(
    wallet: str,
    underlying_token: str,                  # ERC-20 (e.g., USDC for aUSDC, WETH for aWETH)
    start_iso: str,
    end_iso: str,
    infura_url: Optional[str] = None,
    decimals: int = 18,
    counterparties: Optional[Iterable[str]] = None,  # extra contracts (routers, pools, gateways)
    include_default_aave_eth_v3: bool = True,        # add Aave v3 Pool + WETH Gateway by default
) -> pd.DataFrame:
    """
    Per-day flows of the UNDERLYING token between wallet and a whitelist of counterparties.
    Returns columns:
      date, start_block, end_block,
      to_counterparties, from_counterparties, net_with_counterparties

    - to_counterparties: wallet -> counterparty (outflow)
    - from_counterparties: counterparty -> wallet (inflow)
    - net_with_counterparties = inflow - outflow
    """
    infura_url = (infura_url or os.getenv("INFURA_URL","")).strip()
    if not infura_url:
        raise RuntimeError("Missing INFURA_URL")

    cp: Set[str] = set(a.lower() for a in (counterparties or []))
    if include_default_aave_eth_v3:
        cp |= DEFAULT_AAVE_ETH_V3_COUNTERPARTIES

    UNIT = 10 ** int(decimals)

    # Sanity: token exists here
    _assert_token_on_network(infura_url, underlying_token)

    start_dt = datetime.strptime(start_iso, "%Y-%m-%d").date()
    end_dt   = datetime.strptime(end_iso,   "%Y-%m-%d").date()
    if end_dt < start_dt:
        return pd.DataFrame([])

    latest = _latest_block(infura_url)
    latest_ts = _int_hex_safe(_rpc(infura_url, "eth_getBlockByNumber", [hex(latest), False]).get("timestamp"))

    rows = []
    for d in _iterate_days(start_dt, end_dt):
        start_ts = int(datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc).timestamp())
        end_ts   = int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        if start_ts > latest_ts: break
        if end_ts > latest_ts: end_ts = latest_ts

        start_blk = _block_by_time(infura_url, start_ts, "after")
        end_blk   = _block_by_time(infura_url, end_ts,   "before")
        if end_blk < start_blk: end_blk = start_blk

        wtopic = _addr_topic(wallet)

        # Fetch all transfers where wallet is sender OR recipient, then filter by counterparty
        logs_from = _get_logs_chunked(infura_url, underlying_token, start_blk, end_blk,
                                      [TRANSFER_SIG, wtopic, None], stop_at_first=False, chunk_size=50_000) or []
        logs_to   = _get_logs_chunked(infura_url, underlying_token, start_blk, end_blk,
                                      [TRANSFER_SIG, None, wtopic], stop_at_first=False, chunk_size=50_000) or []

        to_cp_wei = 0      # wallet -> counterparty (outflow)
        from_cp_wei = 0    # counterparty -> wallet (inflow)

        for l in logs_from:
            frm, to = _topics_to_addresses(l.get("topics"))
            if frm == wallet.lower() and (to in cp):
                to_cp_wei += _int_hex_safe(l.get("data"))

        for l in logs_to:
            frm, to = _topics_to_addresses(l.get("topics"))
            if to == wallet.lower() and (frm in cp):
                from_cp_wei += _int_hex_safe(l.get("data"))

        rows.append({
            "date": d.isoformat(),
            "start_block": start_blk,
            "end_block": end_blk,
            "to_counterparties":       to_cp_wei   / UNIT,
            "from_counterparties":     from_cp_wei / UNIT,
            "net_with_counterparties": (from_cp_wei - to_cp_wei) / UNIT,
        })

    return pd.DataFrame(rows)
