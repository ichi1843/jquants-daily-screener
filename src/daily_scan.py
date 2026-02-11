import os
import duckdb
import requests
import pandas as pd
import datetime

# --- 設定 ---
R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET_NAME = os.environ["R2_BUCKET_NAME"]

# LINE_TOKEN の代わりに DiscordのWebhook URLを取得
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# R2接続用エンドポイント
ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

def send_discord_notify(message):
    """Discordに通知を送る関数"""
    if not DISCORD_WEBHOOK_URL:
        print(f"[Log] 通知先未設定のためスキップ: {message}")
        return
    
    # Discordは JSON形式で content キーにメッセージを入れる
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
    con.execute(f"""
        SET s3_region='auto';
        SET s3_endpoint='{ENDPOINT_URL}';
        SET s3_access_key_id='{R2_ACCESS_KEY_ID}';
        SET s3_secret_access_key='{R2_SECRET_ACCESS_KEY}';
    """)

    try:
        # 例: 今日の日付のファイルを狙い撃ちする場合（高速化）
        # today_str = datetime.datetime.now().strftime('%Y%m%d')
        # target_path = f"s3://{BUCKET_NAME}/daily_quotes/*{today_str}.parquet"
        
        # テスト用に全検索（※ファイル数が多い場合は注意）
        target_path = f"s3://{BUCKET_NAME}/daily_quotes/*.parquet"
        
        # データを取得（例：最新5件）
        df = con.sql(f"""
            SELECT Date, Code, C, Vo 
            FROM read_parquet('{target_path}')
            ORDER BY Date DESC
            LIMIT 5
        """).df()

        print(f"✅ データ取得成功: {len(df)}件")

        # 通知メッセージ作成（Discordは見やすいのでMarkdownが使えます）
        msg = (
            "**【J-Quants Analysis Bot】**\n"
            "処理が完了しました！\n"
            "```\n"  # コードブロックで見やすく整形
            f"{df.to_string(index=False)}\n"
            "```"
        )
        send_discord_notify(msg)

    except Exception as e:
        error_msg = f"⚠️ **エラーが発生しました**:\n{str(e)}"
        print(error_msg)
        send_discord_notify(error_msg)
        exit(1)

if __name__ == "__main__":
    main()
