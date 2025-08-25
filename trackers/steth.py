import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

INFURA_URL = os.getenv("INFURA_URL")
STETH_CONTRACT = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
TRANSFER_SIG = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
WEI_PER_STETH = 10**18

# ------------------ RPC helpers ------------------

def rpc(method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(INFURA_URL, json=payload)
    r.raise_for_status()
    res = r.json()
    if "error" in res:
        raise Exception(res["error"])
    return res["result"]

def block_by_time(ts, mode="before"):
    latest = int(rpc("eth_blockNumber", []), 16)
    lo, hi = 0, latest
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        blk = rpc("eth_getBlockByNumber", [hex(mid), False])
        ts_mid = int(blk["timestamp"], 16)
        if ts_mid == ts:
            return mid
        elif ts_mid < ts:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return best if mode == "before" else (best + 1 if best else None)

def balance_of(wallet, block):
    selector = "0x70a08231" + "0"*24 + wallet.lower()[2:]
    res = rpc("eth_call", [{"to": STETH_CONTRACT, "data": selector}, hex(block)])
    return int(res, 16)

def get_logs(start_block, end_block, topics):
    params = [{
        "fromBlock": hex(start_block),
        "toBlock": hex(end_block),
        "address": STETH_CONTRACT,
        "topics": topics
    }]
    logs = rpc("eth_getLogs", params)
    return logs

# ------------------ Core calculation ------------------

def sum_transfers(wallet, start_block, end_block):
    wallet_topic = "0x" + "0"*24 + wallet.lower()[2:]
    in_logs  = get_logs(start_block, end_block, [TRANSFER_SIG, None, wallet_topic])
    out_logs = get_logs(start_block, end_block, [TRANSFER_SIG, wallet_topic])

    in_wei, out_wei = 0, 0
    for l in in_logs:
        in_wei += int(l["data"], 16)
    for l in out_logs:
        out_wei += int(l["data"], 16)

    return in_wei, out_wei

def get_steth_rebases(wallet, days_back=30):
    today = datetime.now(timezone.utc).date()
    rows = []
    for d in range(days_back):
        day = today - timedelta(days=d+1)
        start_ts = int(datetime(day.year, day.month, day.day, 0, 0, tzinfo=timezone.utc).timestamp())
        end_ts   = int(datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())

        start_blk = block_by_time(start_ts, "after")
        end_blk   = block_by_time(end_ts, "before")

        start_bal = balance_of(wallet, start_blk)
        end_bal   = balance_of(wallet, end_blk)

        in_wei, out_wei = sum_transfers(wallet, start_blk, end_blk)
        net_wei = in_wei - out_wei

        rebase_wei = (end_bal - start_bal) - net_wei

        rows.append({
            "date": day.isoformat(),
            "start_block": start_blk,
            "end_block": end_blk,
            "start_balance": start_bal/WEI_PER_STETH,
            "end_balance": end_bal/WEI_PER_STETH,
            "transfers_in": in_wei/WEI_PER_STETH,
            "transfers_out": out_wei/WEI_PER_STETH,
            "net_transfers": net_wei/WEI_PER_STETH,
            "daily_rebase_reward": rebase_wei/WEI_PER_STETH
        })

    df = pd.DataFrame(rows).iloc[::-1].reset_index(drop=True)
    return df

