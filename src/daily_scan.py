import os
import duckdb
import requests
import pandas as pd
import pandas_ta as ta
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
    if len(message) > 1900: message = message[:1900] + "\n...(省略)"
    requests.post(DISCORD_WEBHOOK_URL, json={"content": message})

def main():
    print("🚀 スクリーニング開始（最新V2リファレンス完全準拠版）")

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
        # 1. パスの設定
        quotes_path = f"s3://{BUCKET_NAME}/raw/daily_quotes/**/*.parquet"
        master_path = f"s3://{BUCKET_NAME}/raw/equities_master/**/*.parquet"
        fins_path = f"s3://{BUCKET_NAME}/raw/fins_summary/**/*.parquet"

        print("🔍 3種類のデータを読み込み、時価総額を計算中...")

        # SQL修正ポイント（ご提示のリファレンスに完全準拠）:
        # - fins_summary: 開示日は 'DiscDate'、発行済株式数は 'ShOutFY'
        # - equities_master: 会社名は 'CoName'、適用日は 'Date'
        # - daily_quotes: 日付は 'Date'、終値は 'C'
        df_all = con.sql(f"""
            WITH LatestShares AS (
                SELECT 
                    Code, 
                    CAST(NULLIF(ShOutFY, '') AS DOUBLE) as IssuedShares
                FROM read_parquet('{fins_path}')
                -- リファレンスに基づき 'DiscDate' で最新を特定
                QUALIFY ROW_NUMBER() OVER (PARTITION BY Code ORDER BY DiscDate DESC) = 1
            ),
            LatestMaster AS (
                SELECT 
                    Code, 
                    CoName AS CompanyName 
                FROM read_parquet('{master_path}')
                -- リファレンスに基づき 'Date' で最新を特定
                QUALIFY ROW_NUMBER() OVER (PARTITION BY Code ORDER BY Date DESC) = 1
            )
            SELECT 
                CAST(q.Date AS DATE) as Date, 
                q.Code, 
                q.C,
                m.CompanyName,
                (q.C * s.IssuedShares) as MarketCap
            FROM read_parquet('{quotes_path}') q
            INNER JOIN LatestShares s ON q.Code = s.Code
            LEFT JOIN LatestMaster m ON q.Code = m.Code
            WHERE CAST(q.Date AS DATE) >= (CURRENT_DATE - INTERVAL 40 DAY)
            ORDER BY q.Code, q.Date
        """).df()

        if df_all.empty:
            send_discord_notify("✅ スクリーニング対象のデータがR2内に見つかりませんでした。")
            return

        print(f"🔍 分析対象：{df_all['Code'].nunique()} 銘銘柄")

        result_list = []
        for code, group in df_all.groupby('Code'):
            if len(group) < 15: continue
            
            # RSI(14)計算
            rsi_series = ta.rsi(group['C'], length=14)
            if rsi_series is None or rsi_series.empty: continue
            
            latest_rsi = rsi_series.iloc[-1]
            latest_price = group['C'].iloc[-1]
            latest_mcap = group['MarketCap'].iloc[-1]
            latest_name = str(group['CompanyName'].iloc[-1]) if group['CompanyName'].iloc[-1] else str(code)
            
            # 条件判定: 時価総額300億以下 かつ RSI30以下
            if latest_mcap <= 30000000000 and latest_rsi <= 30:
                result_list.append({
                    "Code": code,
                    "Name": latest_name[:10],
                    "Price": int(latest_price),
                    "M-Cap": f"{latest_mcap/100000000:.1f}億",
                    "RSI": round(latest_rsi, 1)
                })

        # 3. 通知
        if result_list:
            res_df = pd.DataFrame(result_list).sort_values("RSI")
            msg = (
                "**🔥 【逆張りチャンス】小型株×RSI30以下 **\n"
                f"データ日付: {df_all['Date'].max().strftime('%Y-%m-%d')}\n"
                "```\n"
                f"{res_df.to_string(index=False)}\n"
                "```"
            )
        else:
            msg = f"✅ {df_all['Date'].max().strftime('%Y-%m-%d')} : 条件に合致する銘柄はありませんでした。"

        send_discord_notify(msg)
        print("✅ 全工程完了")

    except Exception as e:
        error_details = str(e)
        print(f"❌ エラー発生: {error_details}")
        send_discord_notify(f"⚠️ エラー発生:\n```\n{error_details}\n```")
        exit(1)

if __name__ == "__main__":
    main()
