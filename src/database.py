import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432")
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    """初始化資料表"""
    try:
        # 使用 with 語法，會自動關閉 conn 與 cursor
        with get_connection() as conn:
            with conn.cursor() as cursor:
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
        print("✅ PostgreSQL 資料表檢查/初始化成功")
    except Exception as e:
        print(f"❌ 資料庫初始化失敗: {e}")

def save_summary(data, url):
    """將 AI 結果存入資料庫"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO news_summaries (title, company, summary, sentiment, url)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (
                    data.get('title'),
                    data.get('company'),
                    data.get('summary'),
                    data.get('sentiment'),
                    url
                ))
                conn.commit()
        print("💾 [Success] 資料已存入 PostgreSQL！")
    except Exception as e:
        print(f"❌ 寫入資料庫失敗: {e}")