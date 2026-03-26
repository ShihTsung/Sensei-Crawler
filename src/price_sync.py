import requests
import urllib3
from database import get_connection

# 1. 核心修正：停用 SSL 安全警告 (針對政府網站憑證問題)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def sync_all_prices():
    # 1. 上市股價來源 (證交所)
    twse_url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_AVG_ALL"
    # 2. 上櫃/興櫃股價來源 (櫃買中心)
    tpex_url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }

    updated = 0
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # --- 處理上市股價 ---
                print("📈 正在接入 證交所 上市行情...")
                try:
                    # 修正點：加入 verify=False 與 headers
                    res = requests.get(twse_url, headers=headers, verify=False, timeout=30)
                    res.raise_for_status()
                    data = res.json()
                    for item in data:
                        sid = item.get('Code', '').strip()
                        price = item.get('ClosingPrice')
                        # 排除未成交 '--' 的情況
                        if sid and price and price != '--':
                            cur.execute("UPDATE companies SET current_price = %s WHERE stock_id = %s", (float(price), sid))
                            updated += cur.rowcount
                except Exception as e: 
                    print(f"⚠️ 上市行情更新跳過: {e}")

                # --- 處理上櫃行情 ---
                print("📈 正在接入 櫃買中心 上櫃/興櫃行情...")
                try:
                    # 修正點：加入 verify=False 與 headers
                    res = requests.get(tpex_url, headers=headers, verify=False, timeout=30)
                    res.raise_for_status()
                    data = res.json()
                    for item in data:
                        sid = item.get('SecuritiesCompanyCode', '').strip()
                        price = item.get('ClosePrice')
                        # 櫃買中心若未成交 price 可能為 0
                        if sid and price and float(price) > 0:
                            cur.execute("UPDATE companies SET current_price = %s WHERE stock_id = %s", (float(price), sid))
                            updated += cur.rowcount
                except Exception as e: 
                    print(f"⚠️ 上櫃行情更新跳過: {e}")

                conn.commit()
                print(f"✅ 行情同步完成，資料庫已更新 {updated} 筆股價數據。")
                
    except Exception as e:
        print(f"❌ 資料庫連線或更新失敗: {e}")

if __name__ == "__main__":
    sync_all_prices()