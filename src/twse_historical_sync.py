import requests
import time
import re
from datetime import datetime, timedelta
from database import get_connection

def get_last_trading_date():
    # 強制使用台北時間 (假設容器內有 pytz) 或簡單取消 hour < 16 的嚴格限制
    dt = datetime.now() + timedelta(hours=8) # 補回 UTC 差值，確保 hour 正確
    if dt.hour < 15: # 縮短等待時間，15:00 通常就有了
        dt -= timedelta(days=1)
    while dt.weekday() > 4:
        dt -= timedelta(days=1)
    return dt.strftime("%Y%m%d")

def clean(val):
    if val is None: return None
    val = re.sub(r'<[^>]+>', '', str(val))
    val = val.replace(',', '').strip()
    return None if val in ['--', '---', ''] else val

def sync_historical():
    date_str = get_last_trading_date()
    print(f"📅 準備處理歷史交易日: {date_str}")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # --- 1. 行情部分 (剛才已成功) ---
                price_url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALL"
                print(f"📡 請求 {date_str} 行情資料...")
                p_resp = requests.get(price_url, headers=headers, timeout=30)
                p_data = p_resp.json()

                if p_data.get('stat') == 'OK' and 'tables' in p_data:
                    items = next((t['data'] for t in p_data['tables'] if t.get('data') and len(t['data']) > 100 and len(t['data'][0]) >= 15), None)
                    if items:
                        for row in items:
                            if len(row) < 15 or len(row[0].strip()) > 6: continue
                            cur.execute("""
                                INSERT INTO twse_prices (
                                    stock_id, stock_name, date, trade_volume, transaction_count, trade_value, 
                                    open_price, high_price, low_price, close_price, price_change_dir, price_change, 
                                    last_buy_price, last_buy_volume, last_sell_price, last_sell_volume, pe_ratio
                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (stock_id, date) DO UPDATE SET close_price = EXCLUDED.close_price
                            """, (row[0].strip(), row[1].strip(), date_str, clean(row[2]), clean(row[3]), clean(row[4]),
                                  clean(row[5]), clean(row[6]), clean(row[7]), clean(row[8]), clean(row[9]), clean(row[10]),
                                  clean(row[11]), clean(row[12]), clean(row[13]), clean(row[14]), clean(row[15])))
                        conn.commit()
                        print(f"✅ 行情導入完成 ({len(items)} 筆)。")

                # --- 2. 籌碼部分 (補強邏輯) ---
                print("⏳ 等待 8 秒抓取籌碼...")
                time.sleep(8)
                chip_url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
                print(f"📡 請求 {date_str} 三大法人籌碼...")
                
                c_resp = requests.get(chip_url, headers=headers, timeout=30)
                c_data = c_resp.json()
                
                if c_data.get('stat') == 'OK':
                    # 三大法人資料可能在 data 或 tables[0].data
                    c_items = c_data.get('data') or (c_data.get('tables')[0].get('data') if c_data.get('tables') else None)
                    
                    if c_items:
                        for row in c_items:
                            sid = row[0].strip()
                            if len(sid) > 6: continue
                            cur.execute("""
                                INSERT INTO twse_institutional (
                                    stock_id, date, foreign_buy, foreign_sell, foreign_net, 
                                    trust_buy, trust_sell, trust_net, dealer_net
                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (stock_id, date) DO UPDATE SET foreign_net = EXCLUDED.foreign_net
                            """, (sid, date_str, clean(row[2]), clean(row[3]), clean(row[4]), 
                                  clean(row[8]), clean(row[9]), clean(row[10]), clean(row[11])))
                        conn.commit()
                        print(f"✅ 籌碼導入完成 ({len(c_items)} 筆)。")
                    else:
                        print("⚠️ 證交所回應 OK 但找不到籌碼數據內容。")
                else:
                    print(f"❌ 籌碼請求失敗，原因: {c_data.get('stat')}")

    except Exception as e:
        print(f"💥 程式崩潰: {e}")

if __name__ == "__main__":
    sync_historical()