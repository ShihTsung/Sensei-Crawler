import psycopg2
import os
import socket
from dotenv import load_dotenv
from functools import wraps

def auto_env_config(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        load_dotenv()
        if os.path.exists('/.dockerenv'):
            os.environ["DB_HOST"] = "db"
        else:
            hostname = socket.gethostname()
            if "PeterChendeMac-mini" in hostname or "PeterMacBook-Air" in hostname:
                load_dotenv(".env.mac", override=True)
            else:
                load_dotenv(".env.windows", override=True)
        return func(*args, **kwargs)
    return wrapper

@auto_env_config
def get_connection():
    """建立資料庫連線"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "sensei_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "2rligaoi"),
        port=os.getenv("DB_PORT", "5432")
    )

def init_db():
    """自動化建表：如果表格不存在就自動建立"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 建立行情表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_prices (
                        stock_id VARCHAR(20), stock_name VARCHAR(100), date VARCHAR(20),
                        open_price NUMERIC, high_price NUMERIC, low_price NUMERIC,
                        close_price NUMERIC, volume BIGINT, PRIMARY KEY (stock_id, date)
                    );
                ''')
                # 建立籌碼表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_institutional (
                        stock_id VARCHAR(20), date VARCHAR(20),
                        foreign_buy BIGINT, trust_buy BIGINT, dealer_buy BIGINT,
                        PRIMARY KEY (stock_id, date)
                    );
                ''')
                conn.commit()
        print("✅ 資料庫初始化/檢查完成")
    except Exception as e:
        print(f"❌ 初始化失敗: {e}")

if __name__ == "__main__":
    init_db()