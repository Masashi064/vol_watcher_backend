# backfill_nikkei_vi_10y.py
# NIKKEI_VI を yfinance から period="max" で取得し、
# Supabase の VIX データ期間に合わせてトリミングして upsert する

from pathlib import Path
from datetime import date, datetime
import os

import yfinance as yf
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


def get_vix_date_range(supabase: Client) -> tuple[date | None, date | None]:
    """
    volatility_prices から VIX の日付範囲 (min_date, max_date) を取得。
    VIX が 1 行もなければ (None, None) を返す。
    """
    # 最古の日付（昇順で最初の1件）
    res_min = (
        supabase.table("volatility_prices")
        .select("date")
        .eq("symbol", "VIX")
        .order("date")             # ← ascending=True を削除
        .limit(1)
        .execute()
    )

    # 最新の日付（降順で最初の1件）
    res_max = (
        supabase.table("volatility_prices")
        .select("date")
        .eq("symbol", "VIX")
        .order("date", desc=True)  # ← ascending=False の代わりに desc=True
        .limit(1)
        .execute()
    )

    if not res_min.data or not res_max.data:
        return None, None

    min_str = res_min.data[0]["date"]  # "YYYY-MM-DD"
    max_str = res_max.data[0]["date"]

    min_date = datetime.fromisoformat(min_str).date()
    max_date = datetime.fromisoformat(max_str).date()
    return min_date, max_date



def main() -> None:
    supabase = create_supabase_client()

    # 1. まず VIX の日付範囲を取得
    vix_min, vix_max = get_vix_date_range(supabase)
    if vix_min is None or vix_max is None:
        print("VIX のデータが volatility_prices に存在しないため、範囲を揃えられません。")
        print("先に VIX をバックフィルしてから再実行してください。")
        return

    print(f"VIX date range: {vix_min} 〜 {vix_max}")

    # 2. 日経平均ボラティリティ・インデックス（Osaka）を period="max" で取得
    ticker = yf.Ticker("^NKVI.OS")
    hist = ticker.history(period="max", interval="1d")

    if hist.empty:
        raise RuntimeError("NIKKEI_VI のヒストリカルデータが取得できませんでした。")

    # 3. VIX の日付範囲でトリミング
    # hist.index は DatetimeIndex なので、index.date を使ってフィルタ
    idx_dates = hist.index.date
    mask = (idx_dates >= vix_min) & (idx_dates <= vix_max)
    hist_trimmed = hist.loc[mask]

    if hist_trimmed.empty:
        raise RuntimeError("VIX と重なる日付範囲に NIKKEI_VI のデータがありません。")

    print(f"Trimmed NIKKEI_VI rows: {len(hist_trimmed)}")

    # 4. Supabase に upsert
    rows: list[dict] = []
    for idx, row in hist_trimmed.iterrows():
        d: date = idx.date()

        close_val = row["Close"]

        # 終値が本当に NaN / None の日はスキップ
        if close_val is None or (isinstance(close_val, float) and close_val != close_val):
            continue

        # 他のOHLCが NaN なら close で埋める
        open_val = row["Open"]
        high_val = row["High"]
        low_val = row["Low"]

        if isinstance(open_val, float) and open_val != open_val:
            open_val = close_val
        if isinstance(high_val, float) and high_val != high_val:
            high_val = close_val
        if isinstance(low_val, float) and low_val != low_val:
            low_val = close_val

        rows.append(
            {
                "symbol": "NIKKEI_VI",
                "date": d.isoformat(),
                "open": float(open_val),
                "high": float(high_val),
                "low": float(low_val),
                "close": float(close_val),
            }
        )

    if not rows:
        raise RuntimeError("有効な NIKKEI_VI 行がありませんでした。")

    batch_size = 200
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        res = (
            supabase.table("volatility_prices")
            .upsert(chunk, on_conflict="symbol,date")
            .execute()
        )
        print(f"batch {i // batch_size + 1}: {len(res.data)} rows upserted")

    print("DONE: NIKKEI_VI daily history (aligned with VIX range) imported.")


if __name__ == "__main__":
    main()
