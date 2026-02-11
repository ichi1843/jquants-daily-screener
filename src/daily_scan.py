import os
import duckdb
import requests
import pandas as pd
import pandas_ta as ta  # テクニカル分析用
import datetime

# --- 設定 ---
R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET_NAME = os.environ["R2_BUCKET_NAME"]
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

ENDPOINT_DOMAIN = f"{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

def send_discord_notify(message):
    if not DISCORD_WEBHOOK_URL: return
    # Discordの文字数制限(2000文字)対策
    if len(message) > 1900: message = message[:1900] + "\n...(省略)"
    requests.post(DISCORD_WEBHOOK_URL, json={"content": message})

def main():
    print("🚀 スクリーニングを開始します（条件：時価総額300億以下 & RSI30以下）")

    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='auto';
        SET s3_endpoint='{ENDPOINT_DOMAIN}';
        SET s3_access_key_id='{R2_ACCESS_KEY_ID}';
        SET s3_secret_access_key='{R2_SECRET_ACCESS_KEY}';
        SET s3_url_style='path';
        SET s3_use_ssl=true;
    """)

    try:
        # 1. 過去40日分の株価と銘柄マスタを結合して取得
        # RSI(14)を計算するため、最低でも20〜30日分の連続したデータが必要です
        print("📥 データを読み込み中...")
        
        # パス設定
        quotes_path = f"s3://{BUCKET_NAME}/raw/daily_quotes/**/*.parquet"
        master_path = f"s3://{BUCKET_NAME}/raw/equities_master/*.parquet"

        # DuckDBで時価総額を計算しつつデータを抽出
        # 時価総額 = 終値(C) * 発行済株式数(IssuedShares)
        df_all = con.sql(f"""
            WITH base AS (
                SELECT 
                    q.Date, 
                    q.Code, 
                    q.C,
                    m.CompanyName,
                    (q.C * CAST(m.IssuedShares AS LLONG)) as MarketCap
                FROM read_parquet('{quotes_path}') q
                LEFT JOIN read_parquet('{master_path}') m ON q.Code = m.Code
                WHERE q.Date >= (CURRENT_DATE - INTERVAL 40 DAY)
            )
            SELECT * FROM base 
            WHERE MarketCap <= 30000000000 -- 300億円以下
            ORDER BY Code, Date
        """).df()

        if df_all.empty:
            send_discord_notify("⚠️ 条件に合う銘柄（時価総額300億以下）が見つかりませんでした。")
            return

        print(f"🔍 分析対象：{df_all['Code'].nunique()} 銘柄")

        # 2. RSIを計算してフィルタリング
        result_list = []
        for code, group in df_all.groupby('Code'):
            if len(group) < 15: continue  # データ不足はスキップ
            
            # RSI(14)を計算
            rsi_series = ta.rsi(group['C'], length=14)
            if rsi_series is None or rsi_series.empty: continue
            
            latest_rsi = rsi_series.iloc[-1]
            latest_price = group['C'].iloc[-1]
            latest_mcap = group['MarketCap'].iloc[-1]
            latest_name = group['CompanyName'].iloc[-1]
            
            # RSIが30以下のものを抽出
            if latest_rsi <= 30:
                result_list.append({
                    "Code": code,
                    "Name": latest_name[:10], # 10文字に短縮
                    "Price": latest_price,
                    "M-Cap": f"{latest_mcap/100000000:.1f}億",
                    "RSI": round(latest_rsi, 1)
                })

        # 3. 結果の通知
        if result_list:
            res_df = pd.DataFrame(result_list).sort_values("RSI")
            msg = (
                "**🔥 【逆張りチャンス】小型株×RSI30以下 **\n"
                f"取得日: {df_all['Date'].max()}\n"
                "```\n"
                f"{res_df.to_string(index=False)}\n"
                "```"
            )
        else:
            msg = f"✅ {df_all['Date'].max()} : 条件に合致する「売られすぎ小型株」はありませんでした。"

        send_discord_notify(msg)
        print("✅ 通知完了")

    except Exception as e:
        send_discord_notify(f"⚠️ エラー発生:\n{str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
