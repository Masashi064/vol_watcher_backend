import os
import smtplib
from datetime import date, datetime, timezone
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Dict, Any
from pathlib import Path

import yfinance as yf
from dotenv import load_dotenv
from supabase import create_client, Client

# =========================================================
# Supabase クライアント初期化
# =========================================================

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

# グローバル Supabase クライアント
supabase: Client = create_supabase_client()

# =========================================================
# 設定
# =========================================================

# ティッカー対応表（Yahoo Finance）
INDEX_TICKERS = {
    "VIX": "^VIX",
    "NIKKEI_VI": "^NKVI.OS",  # 日経平均ボラティリティ
    # 将来増やしたくなったらここに追加
}

VOL_TABLE = "volatility_prices"   # 価格保存先テーブル
ALERT_TABLE = "alert_rules"       # アラートルールテーブル

# =========================================================
# yfinance から OHLC 取得
# =========================================================

def fetch_latest_ohlc(symbol_name: str, yf_symbol: str) -> Dict[str, Any]:
    """
    指定シンボルの直近1日の OHLC を取得して、
    Supabase に入れやすい dict にして返す。
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

# =========================================================
# Supabase 保存
# =========================================================

def upsert_ohlc(row: Dict[str, Any]) -> None:
    """
    volatility_prices に upsert。
    symbol + date は UNIQUE 制約を付けている前提なので、
    on_conflict に "symbol,date" を指定します。
    """
    response = (
        supabase
        .table(VOL_TABLE)
        .upsert(row, on_conflict="symbol,date")
        .execute()
    )

    # デバッグ用：挿入された（or 更新された）行が返ってくる
    print("    upsert result:", response.data)

# =========================================================
# メール送信ヘルパー
# =========================================================

def send_alert_email(to_email: str, subject: str, body: str) -> bool:
    """
    SMTP を使ってシンプルなテキストメールを送信。
    .env から以下を読む想定：
      SMTP_HOST (省略時: smtp.gmail.com)
      SMTP_PORT (省略時: 587)
      SMTP_USER
      SMTP_PASS
      FROM_EMAIL (省略時: SMTP_USER)
    """
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    from_email = os.getenv("FROM_EMAIL") or user

    if not user or not password or not from_email:
        print("    [WARN] SMTP_USER / SMTP_PASS / FROM_EMAIL が設定されていないため、メール送信をスキップします。")
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = formataddr(("Volatility Alert", from_email))
    msg["To"] = to_email

    try:
        with smtplib.SMTP(host, port) as server:
            server.starttls()
            server.login(user, password)
            server.send_message(msg)
        print(f"    [MAIL] Sent to {to_email}")
        return True
    except Exception as e:
        print(f"    [MAIL ERROR] {e}")
        return False

# =========================================================
# アラートルール判定
# =========================================================

def load_enabled_alert_rules() -> list[Dict[str, Any]]:
    """
    alert_rules テーブルから enabled = true のルールを全件取得。
    """
    res = (
        supabase
        .table(ALERT_TABLE)
        .select("*")
        .eq("enabled", True)
        .execute()
    )
    rules: list[Dict[str, Any]] = res.data or []
    print(f"=== Loaded {len(rules)} enabled alert rules ===")
    return rules

def evaluate_alerts(latest_close: Dict[str, float]) -> None:
    """
    latest_close: {"VIX": 23.4, "NIKKEI_VI": 37.2, ...}
    を使って alert_rules を評価し、
    False → True に変わったルールだけメール送信する（エッジトリガ）。
    """
    rules = load_enabled_alert_rules()
    if not rules:
        print("=== No enabled alert rules. Skipping alert check. ===")
        return

    for rule in rules:
        rule_id = rule["id"]
        symbol = rule["symbol_code"]      # 'VIX' / 'NIKKEI_VI'
        direction = rule["direction"]     # いまは '>=' 前提
        threshold = float(rule["threshold"])
        severity = rule.get("severity") or "notice"
        email = rule["email"]
        last_result = rule.get("last_result")  # True / False / None

        price = latest_close.get(symbol)
        if price is None:
            # まだこの銘柄の価格を取っていない場合
            print(f"[RULE {rule_id}] {symbol}: 最新価格がないためスキップ")
            continue

        # いまの判定
        if direction == ">=":
            now_result = price >= threshold
        else:
            # 将来 '<=' など増やしたくなった場合の保険
            print(f"[RULE {rule_id}] 未対応の direction: {direction} -> スキップ")
            continue

        prev_result = bool(last_result) if last_result is not None else False

        print(
            f"[RULE {rule_id}] {symbol} {direction} {threshold}?"
            f" price={price:.2f} prev={prev_result} now={now_result}"
        )

        # 更新内容はとりあえず現在の判定を保存
        update_fields: Dict[str, Any] = {"last_result": now_result}

        # False -> True になった瞬間だけメール送信
        if now_result and not prev_result:
            # 簡単な文面
            subject = f"[{severity.upper()}] {symbol} が閾値 {threshold} を超えました"
            body_lines = [
                f"このメールはボラティリティ・アラートサービスから自動送信されています。",
                "",
                f"銘柄: {symbol}",
                f"現在の終値: {price:.2f}",
                f"条件: {symbol} {direction} {threshold}",
                f"重要度: {severity}",
                "",
                f"トリガー時刻 (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
            ]
            body = "\n".join(body_lines)

            sent = send_alert_email(email, subject, body)
            if sent:
                update_fields["last_triggered_at"] = datetime.now(timezone.utc).isoformat()
        # True -> False / False -> False のときは last_result だけ更新

        # ルール行を更新
        try:
            supabase.table(ALERT_TABLE).update(update_fields).eq("id", rule_id).execute()
        except Exception as e:
            print(f"    [RULE {rule_id} UPDATE ERROR] {e}")

# =========================================================
# メイン処理
# =========================================================

def main() -> None:
    print("=== Volatility fetch & save & alert ===")

    # 各シンボルの最新終値を集める
    latest_close: Dict[str, float] = {}

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

        latest_close[logical_name] = ohlc["close"]

    print("\n=== Checking alert rules ===")
    evaluate_alerts(latest_close)

    print("\nAll symbols processed & alerts checked.")

if __name__ == "__main__":
    main()
