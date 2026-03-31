import requests
import time
from datetime import datetime, timedelta
from database import get_connection

def get_latest_trading_date(target_date=None):
    """取得最近一個可能的交易日 (週一至週五)"""
    dt = target_date or datetime.now()
    # 如果是週六(5)或週日(6)，退回到週五
    while dt.weekday() > 4:
        dt -= timedelta(days=1)
    return dt.strftime("%Y%m%d")

def sync_twse_historical(target_date=None):
    # 取得要抓取的日期字串 (YYYYMMDD)
    date_str = get_latest_trading_date(target_date)
    print(f"📅 準備處理日期: {date_str}")

    # 傳統 Web API URL
    price_url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALL"
    chip_url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    with get_connection() as conn:
        with conn.cursor() as cur:
            # --- 1. 同步每日行情 ---
            print(f"📡 抓取 {date_str} 行情資料...")
            resp = requests.get(price_url, headers=headers, timeout=30)
            data = resp.json()

            if data.get('stat') == 'OK':
                # MI_INDEX 的資料通常在 'tables' 或 'data9' (視日期而定，現代多在 data9)
                # 這裡抓取「所有證券」表格
                items = data.get('data9') or data.get('tables', [{}])[8].get('data')
                if items:
                    for row in items:
                        sid = row[0]   # 證券代號
                        sname = row[1] # 證券名稱
                        vol = int(row[2].replace(',', '')) # 成交股數
                        close = row[11].replace(',', '')   # 收盤價
                        
                        if close and close != '--':
                            cur.execute("""
                                INSERT INTO twse_prices (stock_id, stock_name, date, close_price, volume)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (stock_id, date) 
                                DO UPDATE SET 
                                    close_price = EXCLUDED.close_price,
                                    volume = EXCLUDED.volume,
                                    stock_name = EXCLUDED.stock_name
                            """, (sid, sname, date_str, float(close), vol))
                    conn.commit()
                    print(f"✅ {date_str} 行情導入完成。")
            else:
                print(f"⚠️ {date_str} 查無行情資料 (可能是休市日)。")

            # 頻率保護：證交所對傳統 API 抓取很嚴格，建議間隔
            print("⏳ 休息 5 秒避開頻率限制...")
            time.sleep(5)

            # --- 2. 同步三大法人買賣超 ---
            print(f"📡 抓取 {date_str} 三大法人籌碼...")
            resp = requests.get(chip_url, headers=headers, timeout=30)
            data = resp.json()

            if data.get('stat') == 'OK':
                items = data.get('data')
                if items:
                    for row in items:
                        sid = row[0].strip()
                        f_buy = int(row[4].replace(',', '')) # 外資買賣超
                        t_buy = int(row[10].replace(',', '')) # 投信買賣超
                        d_buy = int(row[11].replace(',', '')) # 自營商買賣超
                        
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
            else:
                print(f"⚠️ {date_str} 查無籌碼資料。")

if __name__ == "__main__":
    # 如果你想抓昨天 (假設今天是 2026/03/31，會抓 2026/03/30)
    yesterday = datetime.now() - timedelta(days=1)
    sync_twse_historical(yesterday)