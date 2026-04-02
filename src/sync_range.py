import requests
import time
import re
from datetime import datetime, timedelta
from database import get_connection

def clean(val):
    if val is None: return None
    val = re.sub(r'<[^>]+>', '', str(val))
    val = val.replace(',', '').strip()
    return None if val in ['--', '---', ''] else val

def run_sync_for_date(date_str):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # --- 1. 行情部分 ---
                price_url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALL"
                p_resp = requests.get(price_url, headers=headers, timeout=30)
                p_data = p_resp.json()

                if p_data.get('stat') == 'OK' and 'tables' in p_data:
                    # 尋找含有個股收盤行情的大表
                    items = next((t['data'] for t in p_data['tables'] if t.get('data') and len(t['data']) > 100), None)
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
                        print(f"✅ {date_str} 行情導入完成")
                    else:
                        return False
                else:
                    return False

                # --- 2. 籌碼部分 (冷卻 8 秒) ---
                time.sleep(8)
                chip_url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
                c_resp = requests.get(chip_url, headers=headers, timeout=30)
                c_data = c_resp.json()
                
                if c_data.get('stat') == 'OK':
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
                        print(f"✅ {date_str} 籌碼導入完成")
                return True
    except Exception as e:
        print(f"💥 {date_str} 執行出錯: {e}")
        return False

def sync_march_data():
    # 設定補抓 2026 年 3 月份
    current_date = datetime(2026, 3, 1)
    end_date = datetime(2026, 3, 31)
    
    print(f"📅 啟動補抓任務：從 {current_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    
    while current_date <= end_date:
        # 跳過週末
        if current_date.weekday() < 5:
            date_str = current_date.strftime("%Y%m%d")
            print(f"\n🔎 檢查日期: {date_str}")
            
            success = run_sync_for_date(date_str)
            
            if success:
                print(f"🍺 {date_str} 處理完畢，休息一下...")
                time.sleep(10) # 任務間的額外安全等待
            else:
                print(f"⏭️ {date_str} 無交易數據，跳過。")
        
        current_date += timedelta(days=1)

if __name__ == "__main__":
    sync_march_data()