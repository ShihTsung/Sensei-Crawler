import requests
import pandas as pd
from io import StringIO
from database import get_connection

def sync_tdcc_weekly():
    """
    集保戶股權分散表
    正確 API: https://opendata.tdcc.com.tw/getOD.ashx?id=1-5
    欄位: 資料日期, 證券代號, 持股分級, 人數, 股數, 占集保庫存數比例%
    """
    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept": "text/csv, */*",
    }

    print(f"📡 正在連線集保開放資料: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'utf-8-sig'  # 處理 BOM (\ufeff)

        if response.status_code != 200:
            print(f"❌ 伺服器回傳狀態碼: {response.status_code}")
            return

        df = pd.read_csv(StringIO(response.text))
        df.columns = ['date', 'stock_id', 'level', 'holders', 'shares', 'rate']
        print(f"✅ 下載成功，共 {len(df)} 筆，日期: {df['date'].iloc[0]}")

    except Exception as e:
        print(f"💥 下載失敗: {e}")
        return

    print("📝 開始寫入資料庫...")
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                count = 0
                skip = 0
                for _, row in df.iterrows():
                    sid = str(row['stock_id']).strip()
                    # 只取純數字股票代碼（4~6碼）
                    if not sid.isdigit() or not (4 <= len(sid) <= 6):
                        skip += 1
                        continue
                    try:
                        cur.execute("""
                            INSERT INTO twse_weekly_concentration
                                (stock_id, date, level, holders, shares, rate)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (stock_id, date, level) DO NOTHING
                        """, (
                            sid,
                            str(row['date']).strip(),
                            int(row['level']),
                            int(row['holders']),
                            int(row['shares']),
                            float(row['rate'])
                        ))
                        count += 1
                    except Exception as row_err:
                        print(f"⚠️ 跳過異常資料 {sid}: {row_err}")
                        continue

                conn.commit()
        print(f"🎉 同步完成！寫入 {count} 筆，跳過 {skip} 筆（非標準股票代碼）。")

    except Exception as e:
        print(f"💥 資料庫寫入失敗: {e}")


if __name__ == "__main__":
    sync_tdcc_weekly()
