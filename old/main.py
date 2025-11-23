import os
from datetime import date
from typing import Dict, Any
from pathlib import Path

import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client, Client

# ---------- Supabase クライアント初期化 ----------

def create_supabase_client() -> Client:
    """
    main.py と同じフォルダの .env から
    SUPABASE_URL / SUPABASE_SECRET_KEY を読み込んでクライアントを生成
    """
    env_path = Path(__file__).with_name(".env")
    # OS環境変数より .env を優先させる
    load_dotenv(dotenv_path=env_path, override=True)

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SECRET_KEY")

    # デバッグ用（確認できたらコメントアウトでOK）
    print("DEBUG URL:", url)
    print("DEBUG KEY length:", len(key) if key else 0)

    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL / SUPABASE_SECRET_KEY が .env に設定されていません。"
        )

    return create_client(url, key)

# ★★★ ここを追加：Supabase クライアントを実際に作成 ★★★
supabase: Client = create_supabase_client()

# ---------- 設定 ----------

# ティッカー対応表（Yahoo Finance）
INDEX_TICKERS = {
    "VIX": "^VIX",
    "NIKKEI_VI": "^NKVI.OS",  # 日経平均ボラティリティ
    #"VDAX": "V1X.DE",         # VDAX-NEW Index（Yahoo Finance シンボル）
}

TABLE_NAME = "volatility_prices"   # Supabase のテーブル名

# ---------- データ取得部分 (yfinance) ----------

def fetch_latest_ohlc(symbol_name: str, yf_symbol: str) -> Dict[str, Any]:
    """
    指定シンボルの直近1日の OHLC を取得して、Supabase に入れやすい dict にして返す。
    取得できなければ RuntimeError を投げる。
    """
    t = yf.Ticker(yf_symbol)

    # 直近数日取って、最後の1日分を使う
    hist = t.history(period="5d", interval="1d")

    if hist.empty:
        raise RuntimeError(
            f"{symbol_name} ({yf_symbol}) のヒストリーデータが取得できませんでした。"
        )

    latest = hist.iloc[-1]
    d: date = latest.name.date()

    ohlc = {
        "symbol": symbol_name,
        "date": d.isoformat(),       # テーブルは date 型なので YYYY-MM-DD 文字列でOK
        "open": float(latest["Open"]),
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "close": float(latest["Close"]),
    }
    return ohlc

# ---------- Supabase 保存部分 ----------

def upsert_ohlc(row: Dict[str, Any]) -> None:
    """
    volatility_prices に upsert。
    symbol + date は UNIQUE 制約を付けている前提なので、
    on_conflict に "symbol,date" を指定します。
    """
    response = (
        supabase
        .table(TABLE_NAME)
        .upsert(row, on_conflict="symbol,date")
        .execute()
    )

    # デバッグ用：挿入された（or 更新された）行が返ってくる
    print("    upsert result:", response.data)

# ---------- メイン処理 ----------

def main() -> None:
    print("=== Volatility fetch & save ===")

    for logical_name, yf_symbol in INDEX_TICKERS.items():
        print(f"\n[+] Fetching {logical_name} ({yf_symbol}) ...")

        try:
            ohlc = fetch_latest_ohlc(logical_name, yf_symbol)
        except Exception as e:
            # 1つ失敗しても、他の指数処理は続ける
            print(f"    [ERROR] {logical_name} の取得に失敗しました: {e}")
            continue

        print("    latest OHLC:", ohlc)

        print("    -> Upserting into Supabase ...")
        upsert_ohlc(ohlc)
        print("    Done.")

    print("\nAll symbols processed (success or skipped).")

if __name__ == "__main__":
    main()
