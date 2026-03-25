import psycopg2
from datetime import datetime

# 資料庫連線配置
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "2rligaoi",  # <-- 這裡請修改
    "port": "5432"
}

def init_db():
    """初始化資料表"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 建立資料表 (使用 TEXT[] 存儲摘要陣列)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news_summaries (
                id SERIAL PRIMARY KEY,
                title TEXT,
                company TEXT,
                summary TEXT[],
                sentiment TEXT,
                url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ PostgreSQL 資料表檢查/初始化成功")
    except Exception as e:
        print(f"❌ 資料庫初始化失敗: {e}")

def save_summary(data, url):
    """將 AI 結果存入資料庫"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO news_summaries (title, company, summary, sentiment, url)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            data.get('title'),
            data.get('company'),
            data.get('summary'), # psycopg2 會自動處理 Python list 轉 Postgres Array
            data.get('sentiment'),
            url
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("💾 [Success] 資料已存入 PostgreSQL！")
    except Exception as e:
        print(f"❌ 寫入資料庫失敗: {e}")