import os
import duckdb
import requests
import pandas as pd
import datetime

# --- 設定（GitHub Secretsから取得） ---
R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET_NAME = os.environ["R2_BUCKET_NAME"]
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# R2接続用エンドポイント（ドメインのみを指定するのがDuckDBのコツです）
ENDPOINT_DOMAIN = f"{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

def send_discord_notify(message):
    """Discordに通知を送る関数"""
    if not DISCORD_WEBHOOK_URL:
        print(f"[Log] 通知先未設定のためスキップ: {message}")
        return
    
    data = {"content": message}
    response = requests.post(DISCORD_WEBHOOK_URL, json=data)
    
    if response.status_code != 204:
        print(f"Discord通知エラー: {response.status_code} {response.text}")

def main():
    print("🚀 処理を開始します...")

    # DuckDBのセットアップ
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    
    # Cloudflare R2接続のための重要設定
    con.execute(f"""
        SET s3_region='auto';
        SET s3_endpoint='{ENDPOINT_DOMAIN}';
        SET s3_access_key_id='{R2_ACCESS_KEY_ID}';
        SET s3_secret_access_key='{R2_SECRET_ACCESS_KEY}';
        SET s3_url_style='path';
        SET s3_use_ssl=true;
    """)

    try:
        print(f"📂 バケット '{BUCKET_NAME}' からデータを読み込み中...")
        
        # R2上のParquetファイルをスキャン
        # ※ファイルが大量にある場合は 'daily_quotes/2026/*.parquet' のように絞ると高速です
        target_path = f"s3://{BUCKET_NAME}/daily_quotes/*.parquet"
        
        # データを取得（最新の5件をサンプルとして取得）
        # J-Quants V2のカラム名（Date, Code, Cなど）に合わせています
        df = con.sql(f"""
            SELECT 
                Date, 
                Code, 
                C as Close, 
                Vo as Volume
            FROM read_parquet('{target_path}')
            ORDER BY Date DESC, Code ASC
            LIMIT 5
        """).df()

        if df.empty:
            msg = "⚠️ データが見つかりませんでした。パスを確認してください。"
        else:
            print(f"✅ データ取得成功: {len(df)}件")
            # Discord用のメッセージ整形
            msg = (
                "**【J-Quants Daily Screener】**\n"
                "R2からのデータ取得に成功しました！\n"
                "```\n"
                f"{df.to_string(index=False)}\n"
                "```"
            )
        
        send_discord_notify(msg)

    except Exception as e:
        # エラーが発生した場合はDiscordに詳細を投げる
        error_msg = f"⚠️ **システムエラーが発生しました**:\n```\n{str(e)}\n```"
        print(error_msg)
        send_discord_notify(error_msg)
        exit(1)

if __name__ == "__main__":
    main()
