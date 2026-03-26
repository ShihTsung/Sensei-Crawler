import requests
from database import get_connection

def sync_all_prices():
    # 1. 上市股價來源 (證交所)
    twse_url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL"
    # 2. 上櫃/興櫃股價來源 (櫃買中心)
    tpex_url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
    
    updated = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            # --- 處理上市股價 ---
            print("📈 抓取上市行情...")
            try:
                res = requests.get(twse_url)
                for item in res.json():
                    sid = item.get('Code', '').strip()
                    price = item.get('ClosingPrice')
                    if sid and price and price != '--':
                        cur.execute("UPDATE companies SET current_price = %s WHERE stock_id = %s", (float(price), sid))
                        updated += cur.rowcount
            except: print("⚠️ 上市行情更新跳過")

            # --- 處理上櫃行情 ---
            print("📈 抓取上櫃/興櫃行情...")
            try:
                res = requests.get(tpex_url)
                for item in res.json():
                    sid = item.get('SecuritiesCompanyCode', '').strip()
                    price = item.get('ClosePrice')
                    if sid and price and price != 0:
                        cur.execute("UPDATE companies SET current_price = %s WHERE stock_id = %s", (float(price), sid))
                        updated += cur.rowcount
            except: print("⚠️ 上櫃行情更新跳過")
            
            conn.commit()
    print(f"🚀 全市場同步完成！共更新 {updated} 家股價。")

if __name__ == "__main__":
    sync_all_prices()