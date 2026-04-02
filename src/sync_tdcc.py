import requests
import pandas as pd
from io import StringIO
from database import get_connection

def sync_tdcc_weekly():
    url = "https://data.tdcc.com.tw/getOD?format=csv&odId=4-5"
    print("📡 正在從集保中心下載最新週資料 (CSV)...")
    
    try:
        # 加入 Headers 模擬瀏覽器連線
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'utf-8'
        
        # 讀取 CSV 並設定正確欄位
        df = pd.read_csv(StringIO(response.text))
        df.columns = ['date', 'stock_id', 'level', 'holders', 'shares', 'rate']
        
        print(f"✅ 下載成功，準備寫入 {len(df)} 筆資料...")

        with get_connection() as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    sid = str(row['stock_id']).strip()
                    if not sid.isdigit() or len(sid) != 4: continue 
                    
                    cur.execute("""
                        INSERT INTO twse_weekly_concentration (stock_id, date, level, holders, shares, rate)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (stock_id, date, level) DO NOTHING
                    """, (sid, str(row['date']), int(row['level']), int(row['holders']), 
                          int(row['shares']), float(row['rate'])))
                conn.commit()
        print("🎉 集保週資料同步完成！")
    except Exception as e:
        print(f"💥 同步失敗: {e}")

if __name__ == "__main__":
    sync_tdcc_weekly()