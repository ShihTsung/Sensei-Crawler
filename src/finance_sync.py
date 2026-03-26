import requests
import sys
import os
from database import init_db, upsert_companies

# 確保路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 定義電子產業代碼 (證交所標準)
ELECTRONIC_CODES = ["24", "25", "26", "27", "28", "29", "30", "31", "33"]
FINANCIAL_CODES = ["17", "23"] # 金融、保險

def fetch_and_sync():
    stable_url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    print("📡 啟動代碼級校正：正在根據產業代碼重新歸類...")
    
    try:
        response = requests.get(stable_url, timeout=30)
        raw_data = response.json()
        formatted_list = []
        
        for item in raw_data:
            # 取得代碼 (例如 '01', '24')
            ind_code = item.get('產業別', '').strip()
            
            # --- 基於代碼的強效分類 ---
            sector_group = "傳產產業"
            if ind_code in ELECTRONIC_CODES:
                sector_group = "電子產業"
            elif ind_code in FINANCIAL_CODES:
                sector_group = "金融產業"
            
            formatted_list.append((
                item.get('公司代號', '').strip(),
                item.get('公司名稱'),
                ind_code, # 存入 '24'
                item.get('主要業務內容') or f"產業代碼：{ind_code}", 
                item.get('董事長'),
                item.get('住址'),
                "上市",
                sector_group
            ))
        
        if upsert_companies(formatted_list):
            print(f"🚀 校正成功！電子產業分類已根據代碼重新標記。")
            
    except Exception as e:
        print(f"❌ 同步失敗: {e}")

if __name__ == "__main__":
    fetch_and_sync()