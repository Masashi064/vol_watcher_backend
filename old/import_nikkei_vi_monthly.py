# import_nikkei_vi_monthly.py

from pathlib import Path
from datetime import datetime
import os

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client


def create_supabase_client() -> Client:
    """同じフォルダの .env から Supabase クライアントを作成"""
    env_path = Path(__file__).with_name(".env")
    load_dotenv(env_path)

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SECRET_KEY")

    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SECRET_KEY が .env にありません。")

    return create_client(url, key)


def main() -> None:
    supabase = create_supabase_client()

    # CSV パス（同じフォルダに置いた前提）
    csv_path = Path(__file__).with_name("nikkei_vi_monthly.csv")

    # 日経のCSVは Shift-JIS 系なので cp932 で読む
    df = pd.read_csv(csv_path, encoding="cp932")

    # デバッグ用：カラム名を確認したいとき
    # print(df.head())

    # 「データ日付」を datetime に変換。変換できなかった行（注意書き）は NaT になる
    df["parsed_date"] = pd.to_datetime(
        df["データ日付"], format="%Y/%m/%d", errors="coerce"
    )

    # 日付に変換できた行だけ残す（注意書き行を削除）
    df = df[df["parsed_date"].notna()]

    rows = []
    for _, r in df.iterrows():
        d = r["parsed_date"].date().isoformat()

        rows.append(
            {
                "symbol": "NIKKEI_VI",
                "date": d,
                "open": float(r["始値"]),
                "high": float(r["高値"]),
                "low": float(r["安値"]),
                "close": float(r["終値"]),
            }
        )

    # まとめて upsert（symbol + date でユニーク）
    batch_size = 200
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        res = (
            supabase.table("volatility_prices")
            .upsert(chunk, on_conflict="symbol,date")
            .execute()
        )
        print(f"batch {i // batch_size + 1}: {len(res.data)} rows upserted")

    print("DONE: Nikkei VI monthly history imported.")


if __name__ == "__main__":
    main()
