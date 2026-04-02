import psycopg2
import os
import socket
from dotenv import load_dotenv
from functools import wraps

def auto_env_config(func):
    """自動偵測環境並載入對應的資料庫設定"""
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
    """自動化建表：整合行情、籌碼、董監、十大股東與集保週資料"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 1. 行情表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_prices (
                        stock_id VARCHAR(20), stock_name VARCHAR(100), date VARCHAR(20),
                        trade_volume BIGINT, transaction_count INT, trade_value NUMERIC,
                        open_price NUMERIC, high_price NUMERIC, low_price NUMERIC, close_price NUMERIC,
                        price_change_dir VARCHAR(10), price_change NUMERIC,
                        last_buy_price NUMERIC, last_buy_volume INT,
                        last_sell_price NUMERIC, last_sell_volume INT,
                        pe_ratio NUMERIC, PRIMARY KEY (stock_id, date)
                    );
                ''')
                # 2. 籌碼表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_institutional (
                        stock_id VARCHAR(20), date VARCHAR(20),
                        foreign_buy BIGINT, foreign_sell BIGINT, foreign_net BIGINT,
                        trust_buy BIGINT, trust_sell BIGINT, trust_net BIGINT,
                        dealer_net BIGINT, PRIMARY KEY (stock_id, date)
                    );
                ''')
                # 3. 董監持股表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_insider_holding (
                        stock_id VARCHAR(10), date VARCHAR(20),
                        title VARCHAR(50), name VARCHAR(100),
                        held_shares BIGINT, pledged_shares BIGINT, pledge_rate NUMERIC,
                        PRIMARY KEY (stock_id, date, name)
                    );
                ''')
                # 4. 前十大股東表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_top10_shareholders (
                        stock_id VARCHAR(10), year_period VARCHAR(20),
                        rank INT, name VARCHAR(100),
                        held_shares BIGINT, held_rate NUMERIC,
                        PRIMARY KEY (stock_id, year_period, name)
                    );
                ''')
                # 5. 集保週資料表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_weekly_concentration (
                        stock_id VARCHAR(10), date VARCHAR(20),
                        level INT, holders INT, shares BIGINT, rate NUMERIC,
                        PRIMARY KEY (stock_id, date, level)
                    );
                ''')
                conn.commit()
        print("✅ 所有資料表結構檢查完成")
    except Exception as e:
        print(f"❌ 初始化失敗: {e}")

if __name__ == "__main__":
    init_db()