import requests
from database import get_connection

def patch_missing_markets():
    # 分開請求，避免一次請求過多被擋
    targets = [
        {"name": "上櫃", "url": "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_list"},
        {"name": "興櫃", "url": "https://www.tpex.org.tw/openapi/v1/tpex_esb_list"}
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            for target in targets:
                print(f"📡 正在單獨補全 {target['name']} 數據...")
                try:
                    res = requests.get(target['url'], headers=headers, timeout=30)
                    res.encoding = 'utf-8-sig'
                    data = res.json()
                    
                    count = 0
                    for item in data:
                        # 櫃買中心欄位：SecuritiesCompanyCode, CompanyName
                        sid = (item.get('SecuritiesCompanyCode') or '').strip()
                        name = (item.get('CompanyName') or '').strip()
                        ind = (item.get('IndustryType') or '').strip()
                        
                        if len(sid) == 4 and sid.isdigit():
                            cur.execute("""
                                INSERT INTO companies (stock_id, company_name, industry, market_type)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (stock_id)
                                DO UPDATE SET
                                    market_type = EXCLUDED.market_type,
                                    industry = EXCLUDED.industry
                            """, (sid, name, ind, target['name']))
                            count += 1
                    print(f"✅ {target['name']} 補全成功：{count} 家")
                except Exception as e:
                    print(f"❌ {target['name']} 補全失敗：內容可能非 JSON 或被擋。")
            
            conn.commit()

if __name__ == "__main__":
    patch_missing_markets()