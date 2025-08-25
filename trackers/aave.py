import pandas as pd
from datetime import datetime, timedelta

def get_aave_interest(wallet: str, days_back: int = 30) -> pd.DataFrame:
    today = datetime.utcnow().date()
    rows = []
    for d in range(days_back):
        day = today - timedelta(days=d+1)
        rows.append({
            "date": day.isoformat(),
            "start_balance": 0.0,
            "end_balance": 0.0,
            "daily_interest": 0.0
        })
    return pd.DataFrame(rows).iloc[::-1].reset_index(drop=True)
