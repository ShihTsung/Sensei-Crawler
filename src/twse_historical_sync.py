import requests
import time
import re
from datetime import datetime, timedelta
from database import get_connection

def get_last_trading_date():
    dt = datetime.now()
    if dt.hour < 16:
        dt -= timedelta(days=1)
    while dt.weekday() > 4:
        dt -= timedelta(days=1)
    return dt.strftime("%Y%m%d")

def sync_historical():
    date_str = get_last_trading_date()
    print(f"📅 準備處理歷史交易日: {date_str}")

    price_url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALL"
    chip_url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # --- 1. 抓取每日行情 ---
                print(f"📡 請求 {date_str} 行情資料...")
                p_resp = requests.get(price_url, headers=headers, timeout=30)
                p_data = p_resp.json()

                if p_data.get('stat') == 'OK':
                    items = None
                    if 'tables' in p_data:
                        for table in p_data['tables']:
                            if "證券代號" in str(table.get('fields', [])) and len(table.get('data', [])) > 500:
                                items = table['data']
                                break
                    
                    if not items:
                        for key, value in p_data.items():
                            if key.startswith('data') and isinstance(value, list) and len(value) > 800:
                                items = value
                                break

                    if items:
                        count = 0
                        for row in items:
                            if len(row) < 12: continue
                            sid, sname = row[0].strip(), row[1].strip()
                            if not re.match(r'^[A-Z0-9]{4,}$', sid): continue
                                
                            # --- 修正處：確保 try/except 縮排正確且邏輯完整 ---
                            try:
                                vol = int(row[2].replace(',', ''))
                                op  = row[5].replace(',', '').replace('--', '')  # 開盤
                                hi  = row[6].replace(',', '').replace('--', '')  # 最高
                                lo  = row[7].replace(',', '').replace('--', '')  # 最低
                                cl  = row[11].replace(',', '').replace('--', '') # 收盤
                                
                                if cl:
                                    cur.execute("""
                                        INSERT INTO twse_prices (stock_id, stock_name, date, open_price, high_price, low_price, close_price, volume)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (stock_id, date) 
                                        DO UPDATE SET 
                                            open_price = EXCLUDED.open_price,
                                            high_price = EXCLUDED.high_price,
                                            low_price = EXCLUDED.low_price,
                                            close_price = EXCLUDED.close_price,
                                            volume = EXCLUDED.volume,
                                            stock_name = EXCLUDED.stock_name
                                    """, (sid, sname, date_str, 
                                          float(op) if op else None, 
                                          float(hi) if hi else None, 
                                          float(lo) if lo else None, 
                                          float(cl), vol))
                                    count += 1
                            except (ValueError, IndexError):
                                continue
                        conn.commit()
                        print(f"✅ {date_str} 行情導入完成 (共 {count} 筆)。")
                    else:
                        print(f"❌ 依然找不到行情資料。")
                else:
                    print(f"⚠️ 證交所回傳 Stat 非 OK。")

                time.sleep(5)

                # --- 2. 抓取三大法人 ---
                print(f"📡 請求 {date_str} 三大法人籌碼...")
                c_resp = requests.get(chip_url, headers=headers, timeout=30)
                c_data = c_resp.json()
                if c_data.get('stat') == 'OK' and 'data' in c_data:
                    c_items = c_data['data']
                    for row in c_items:
                        sid = row[0].strip()
                        f_buy = int(row[4].replace(',', ''))
                        t_buy = int(row[10].replace(',', ''))
                        d_buy = int(row[11].replace(',', ''))
                        cur.execute("""
                            INSERT INTO twse_institutional (stock_id, date, foreign_buy, trust_buy, dealer_buy)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (stock_id, date) 
                            DO UPDATE SET 
                                foreign_buy = EXCLUDED.foreign_buy, 
                                trust_buy = EXCLUDED.trust_buy, 
                                dealer_buy = EXCLUDED.dealer_buy
                        """, (sid, date_str, f_buy, t_buy, d_buy))
                    conn.commit()
                    print(f"✅ {date_str} 籌碼導入完成。")
    except Exception as e:
        print(f"❌ 執行發生錯誤: {e}")

if __name__ == "__main__":
    sync_historical()