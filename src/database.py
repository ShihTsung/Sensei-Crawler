import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

# 資料庫連線配置
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432")
}

def get_connection():
    """建立資料庫連線"""
    return psycopg2.connect(**DB_CONFIG)

def save_summary(result, url):
    """儲存 AI 分析後的摘要與情緒 (支援 TEXT[] 格式)"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 使用 ON CONFLICT (url) 進行更新，這需要 UNIQUE 約束才能運作
                cur.execute("""
                    INSERT INTO news_summaries (title, company, summary, sentiment, url)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO UPDATE SET
                        summary = EXCLUDED.summary,
                        sentiment = EXCLUDED.sentiment,
                        created_at = CURRENT_TIMESTAMP
                """, (result['title'], result['company'], result['summary'], result['sentiment'], url))
                conn.commit()
                print(f"✅ AI 摘要已存入資料庫: {result['title']}")
    except Exception as e:
        print(f"❌ save_summary 失敗: {e}")

def init_db():
    """初始化多維度資料表並自動補強 UNIQUE 約束"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 建立新聞摘要表 (若不存在)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS news_summaries (
                        id SERIAL PRIMARY KEY,
                        title TEXT,
                        company TEXT,
                        summary TEXT[],
                        sentiment TEXT,
                        url TEXT UNIQUE,  -- 新建表時直接包含 UNIQUE 約束
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # 2. 自動補強邏輯：針對已存在的舊表添加 UNIQUE 約束
                # 這段 SQL 會檢查 conname 是否已存在，避免重複添加導致報錯
                cursor.execute('''
                    DO $$ 
                    BEGIN 
                        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'news_summaries_url_key') THEN
                            ALTER TABLE news_summaries ADD CONSTRAINT news_summaries_url_key UNIQUE (url);
                        END IF;
                    END $$;
                ''')
                
                # 3. 升級版公司主檔表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS companies (
                        stock_id VARCHAR(10) PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL,
                        industry_type VARCHAR(100),
                        major_business TEXT,
                        chairman VARCHAR(100),
                        address TEXT,
                        market_type VARCHAR(20),
                        sector_group VARCHAR(50),
                        market_cap_category VARCHAR(20),
                        stock_style_tags TEXT[],
                        is_special_trade BOOLEAN DEFAULT FALSE,
                        ai_sector VARCHAR(100),
                        ai_relevance NUMERIC(3, 2) DEFAULT 0,
                        ai_analysis_note TEXT,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
        print("✅ PostgreSQL 資料表初始化與 UNIQUE 約束校準成功")
    except Exception as e:
        print(f"❌ 資料庫初始化失敗: {e}")

def upsert_companies(company_list):
    """更新公司主檔數據"""
    sql = """
        INSERT INTO companies (
            stock_id, company_name, industry_type, major_business, 
            chairman, address, market_type, sector_group
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_id) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            industry_type = EXCLUDED.industry_type,
            major_business = EXCLUDED.major_business,
            sector_group = EXCLUDED.sector_group,
            market_type = EXCLUDED.market_type;
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, company_list)
                conn.commit()
        return True
    except Exception as e:
        print(f"❌ 資料庫更新失敗: {e}")
        return False