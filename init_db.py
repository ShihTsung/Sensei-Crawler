import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def init_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "db"),
            database=os.getenv("DB_NAME", "sensei_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD")
        )
        cur = conn.cursor()

        # 1. 建立科技情報表 (Tab 1 使用)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS news_summaries (
                id SERIAL PRIMARY KEY,
                title TEXT,
                company TEXT,
                summary TEXT[],
                sentiment TEXT,
                url TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        # 2. 建立企業數據表 (這是解決 Tab 2 KeyError 的關鍵！)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                stock_id TEXT PRIMARY KEY,
                company_name TEXT,
                market_type TEXT,
                ai_relevance FLOAT DEFAULT 0.0,
                industry TEXT
            );
        ''')
        
        # 3. 插入一筆測試資料，避免 DataFrame 變成空的
        cur.execute('''
            INSERT INTO companies (stock_id, company_name, market_type, ai_relevance)
            VALUES ('2330', '台積電', '上市', 0.9)
            ON CONFLICT DO NOTHING;
        ''')
        
        conn.commit()
        cur.close()
        conn.close()
        print("✅ 所有資料表（含 companies）建立成功！")

    except Exception as e:
        print(f"❌ 建立失敗: {e}")

if __name__ == "__main__":
    init_db()