import pandas as pd
from datetime import datetime, timedelta

def get_aave_interest(wallet, days_back=30):
    today = datetime.utcnow().date()
    rows = []
    for d in range(days_back):
        day = today - timedelta(days=d+1)
        rows.append({
            "date": day.isoformat(),
            "start_balance": 1000,
            "end_balance": 1000.5,
            "daily_interest": 0.5
        })
    return pd.DataFrame(rows).iloc[::-1].reset_index(drop=True)

