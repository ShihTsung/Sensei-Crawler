import requests
from database import get_connection

def init_markets():
    # 專注上市：使用最穩定的證交所 OpenAPI
    target = {
        "name": "上市", 
        "url": "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print(f"📡 正在從政府數據中樞接入 {target['name']} 名單...")
    
    try:
        res = requests.get(target['url'], headers=headers, timeout=30)
        res.raise_for_status()
        
        # 強制處理編碼，排除開頭的 BOM 標籤 (解決昨天的解析錯誤)
        res.encoding = 'utf-8-sig'
        data = res.json()
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                count = 0
                for item in data:
                    # 昨天的爬蟲欄位對齊
                    stock_id = (item.get('公司代號') or '').strip()
                    name = (item.get('公司名稱') or '').strip()
                    industry = (item.get('產業別') or '').strip()
                    
                    # 過濾：只要 4 碼純數字股票
                    if stock_id and name and len(stock_id) == 4 and stock_id.isdigit():
                        cur.execute("""
                            INSERT INTO companies (stock_id, company_name, industry_type, market_type)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (stock_id) 
                            DO UPDATE SET 
                                company_name = EXCLUDED.company_name,
                                industry_type = EXCLUDED.industry_type,
                                market_type = EXCLUDED.market_type,
                                last_updated = CURRENT_TIMESTAMP
                        """, (stock_id, name, industry, target['name']))
                        count += 1
                
                conn.commit()
                print(f"✅ {target['name']} 整合同步成功：共 {count} 家企業入庫")
                
    except Exception as e:
        print(f"❌ 整合失敗: {e}")

if __name__ == "__main__":
    init_markets()