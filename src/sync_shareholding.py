import requests
import pandas as pd
import time
import re
from io import StringIO
from database import get_connection

def clean(val):
    if val is None or pd.isna(val): return 0
    val = re.sub(r'<[^>]+>', '', str(val))
    val = val.replace(',', '').strip()
    return 0 if val in ['--', '---', ''] else val

def sync_insider_holding(year, month):
    roc_year = year - 1911 if year > 1911 else year
    date_str = f"{year}{str(month).zfill(2)}"
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Referer": "https://mops.twse.com.tw/mops/web/t08get02",
    }
    print(f"📡 正在請求 {year}年{month}月 董監持股...")
    url = "https://mops.twse.com.tw/mops/web/ajax_t08get02"
    payload = {'encodeutf8': '1', 'step': '1', 'firstin': '1', 'off': '1', 'TYPEK': 'all', 'year': str(roc_year), 'month': str(month).zfill(2)}

    try:
        session.get("https://mops.twse.com.tw/mops/web/t08get02", headers=headers, timeout=20)
        time.sleep(2)
        response = session.post(url, data=payload, headers=headers, timeout=30)
        response.encoding = 'utf-8'

        if "查詢無資料" in response.text:
            print(f"⚠️ 查無 {date_str} 資料。")
            return

        dfs = pd.read_html(StringIO(response.text))
        df = next((t for t in dfs if '公司代號' in t.columns and '姓名' in t.columns), None)
        
        if df is None:
            with open("mops_debug.html", "w", encoding="utf-8") as f: f.write(response.text)
            print("❌ 找不到表格，已存入 mops_debug.html。")
            return

        with get_connection() as conn:
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    sid = str(row['公司代號']).split('.')[0].strip()
                    if not sid.isdigit() or len(sid) < 4: continue
                    cur.execute("""
                        INSERT INTO twse_insider_holding (stock_id, date, title, name, held_shares, pledged_shares, pledge_rate)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (stock_id, date, name) 
                        DO UPDATE SET held_shares = EXCLUDED.held_shares, pledged_shares = EXCLUDED.pledged_shares;
                    """, (sid, date_str, str(row['職稱']), str(row['姓名']), 
                          clean(row['目前持股']), clean(row['質押股數']), clean(row['質押比例'])))
                conn.commit()
        print(f"🎉 {date_str} 導入成功！")
    except Exception as e:
        print(f"💥 執行錯誤: {e}")

if __name__ == "__main__":
    sync_insider_holding(2025, 12)