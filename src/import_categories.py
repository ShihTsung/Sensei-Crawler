import sys
import os
import pandas as pd

# 將 src 目錄加入 Python 搜尋路徑，以便引用 database
sys.path.append(os.path.dirname(__file__))
from database import get_connection

def import_stock_categories():
    # 因為你在 Docker 外把檔案放在根目錄，我們讓腳本去找上一層目錄的檔案
    file_path = os.path.join(os.path.dirname(__file__), "..", "960803-0960203558-2.csv")
    
    if not os.path.exists(file_path):
        # 備用路徑 (如果直接在根目錄執行的話)
        file_path = "960803-0960203558-2.csv"
        
    if not os.path.exists(file_path):
        print(f"❌ 找不到檔案: {file_path}")
        return

    print(f"📖 正在讀取檔案: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        
        # 資料清洗：處理 1702.0 變成 "1702"
        def clean_stock_id(val):
            try:
                if pd.isna(val): return None
                return str(int(float(val)))
            except:
                return None

        print("🧹 正在清洗資料...")
        df_clean = pd.DataFrame({
            'stock_id': df['代號'].apply(clean_stock_id),
            'company_name': df['公司名稱'],
            'category_name': df['新產業類別']
        }).dropna(subset=['stock_id'])

        print(f"🚀 準備寫入 {len(df_clean)} 筆產業資料...")
        
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 建立資料表 (如果還沒建的話)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stock_category (
                        stock_id VARCHAR(10) PRIMARY KEY,
                        category_name VARCHAR(50),
                        company_name VARCHAR(100)
                    );
                """)
                
                # 寫入邏輯
                insert_sql = """
                    INSERT INTO stock_category (stock_id, company_name, category_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (stock_id) 
                    DO UPDATE SET 
                        category_name = EXCLUDED.category_name,
                        company_name = EXCLUDED.company_name;
                """
                batch_data = [
                    (row['stock_id'], row['company_name'], row['category_name'])
                    for _, row in df_clean.iterrows()
                ]
                cur.executemany(insert_sql, batch_data)
                conn.commit()
                
        print("🎉 產業類別資料匯入成功！")

    except Exception as e:
        print(f"💥 匯入失敗: {e}")

if __name__ == "__main__":
    import_stock_categories()