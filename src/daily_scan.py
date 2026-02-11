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
    print("🚀 スクリーニング開始（メモリ最適化版）")

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
        # 1. 最新の銘柄マスタファイルを1つだけ特定する（重複防止）
        print("🔍 最新の銘柄マスタを探索中...")
        master_files = con.sql(f"SELECT name FROM glob('s3://{BUCKET_NAME}/raw/equities_master/**/*.parquet') ORDER BY name DESC LIMIT 1").df()
        
        if master_files.empty:
            raise Exception("銘柄マスタファイルが見つかりません。")
        
        latest_master_path = master_files.iloc[0]['name']
        print(f"📍 使用するマスタ: {latest_master_path}")

        # 2. 直近40日分の株価だけを読み込む（メモリ節約）
        print("📥 株価データを読み込み中...")
        quotes_path = f"s3://{BUCKET_NAME}/raw/daily_quotes/**/*.parquet"

        # SQLの修正ポイント:
        # - IssuedShares が文字列として保存されている可能性があるため CAST する
        # - master を最新の1ファイルに固定して結合
        df_all = con.sql(f"""
            SELECT 
                CAST(q.Date AS DATE) as Date, 
                q.Code, 
                q.C,
                m.CompanyName,
                (q.C * CAST(m.IssuedShares AS DOUBLE)) as MarketCap
            FROM read_parquet('{quotes_path}') q
            INNER JOIN read_parquet('{latest_master_path}') m ON q.Code = m.Code
            WHERE CAST(q.Date AS DATE) >= (CURRENT_DATE - INTERVAL 40 DAY)
            ORDER BY q.Code, q.Date
        """).df()

        if df_all.empty:
            send_discord_notify("✅ 条件に合うデータがR2内にありませんでした。")
            return

        print(f"🔍 分析対象：{df_all['Code'].nunique()} 銘柄")

        result_list = []
        for code, group in df_all.groupby('Code'):
            if len(group) < 15: continue
            
            # RSI計算
            rsi_series = ta.rsi(group['C'], length=14)
            if rsi_series is None or rsi_series.empty: continue
            
            latest_rsi = rsi_series.iloc[-1]
            latest_price = group['C'].iloc[-1]
            latest_mcap = group['MarketCap'].iloc[-1]
            latest_name = str(group['CompanyName'].iloc[-1]) if group['CompanyName'].iloc[-1] else str(code)
            
            # 条件判定
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
        print("✅ 完了")

    except Exception as e:
        error_details = str(e)
        print(f"❌ エラー発生: {error_details}")
        send_discord_notify(f"⚠️ エラー発生:\n```\n{error_details}\n```")
        exit(1)

if __name__ == "__main__":
    main()
