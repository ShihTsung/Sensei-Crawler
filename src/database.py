import psycopg2
from psycopg2.extras import execute_values
import os
import socket
from dotenv import load_dotenv
from functools import wraps

def auto_env_config(func):
    """
    動態環境裝飾器：執行任何資料庫操作前，先確保抓到正確的電腦配置
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        hostname = socket.gethostname()
        # 偵測 Mac Mini 或 Windows
        env_file = ".env.mac" if "PeterChendeMac-mini" in hostname else ".env.windows"
        
        if os.path.exists(env_file):
            load_dotenv(env_file, override=True)
        else:
            load_dotenv()
        return func(*args, **kwargs)
    return wrapper

@auto_env_config
def get_connection():
    """建立資料庫連線"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", "5432")
    )

def init_db():
    """初始化資料庫表 (補回您遺失的程式碼)"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 建立新聞摘要表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS news_summaries (
                        id SERIAL PRIMARY KEY,
                        title TEXT,
                        company VARCHAR(100),
                        summary TEXT,
                        sentiment VARCHAR(20),
                        url TEXT UNIQUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
        print("✅ PostgreSQL 資料表初始化成功")
    except Exception as e:
        print(f"❌ 資料庫初始化失敗: {e}")

# 讓您可以直接執行 python src/database.py 來初始化
if __name__ == "__main__":
    init_db()