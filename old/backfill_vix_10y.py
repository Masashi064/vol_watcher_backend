# backfill_vix_10y.py

from pathlib import Path
from datetime import date
import os

import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client, Client


def create_supabase_client() -> Client:
    env_path = Path(__file__).with_name(".env")
    load_dotenv(env_path)

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SECRET_KEY")

    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SECRET_KEY が .env にありません。")

    return create_client(url, key)


def main() -> None:
    supabase = create_supabase_client()

    ticker = yf.Ticker("^VIX")

    # 過去10年分のデイリーデータを取得
    hist = ticker.history(period="10y", interval="1d")

    if hist.empty:
        raise RuntimeError("VIX のヒストリカルデータが取得できませんでした。")

    rows = []
    for idx, row in hist.iterrows():
        d: date = idx.date()
        rows.append(
            {
                "symbol": "VIX",
                "date": d.isoformat(),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
            }
        )

    # 既存の main.py と同じく upsert
    batch_size = 200
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        res = (
            supabase.table("volatility_prices")
            .upsert(chunk, on_conflict="symbol,date")
            .execute()
        )
        print(f"batch {i // batch_size + 1}: {len(res.data)} rows upserted")

    print("DONE: VIX 10-year daily history imported.")


if __name__ == "__main__":
    main()
