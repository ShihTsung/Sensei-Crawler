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
    """自動化建表：升級為全欄位版本"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # 建立行情表 (新增全量欄位)
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_prices (
                        stock_id VARCHAR(20), stock_name VARCHAR(100), date VARCHAR(20),
                        trade_volume BIGINT,      -- 成交股數
                        transaction_count INT,    -- 成交筆數
                        trade_value NUMERIC,      -- 成交金額
                        open_price NUMERIC,       -- 開盤價
                        high_price NUMERIC,       -- 最高價
                        low_price NUMERIC,        -- 最低價
                        close_price NUMERIC,      -- 收盤價
                        price_change_dir VARCHAR(10), -- 漲跌符號
                        price_change NUMERIC,     -- 漲跌價差
                        last_buy_price NUMERIC,   -- 最後揭示買價
                        last_buy_volume INT,      -- 最後揭示買量
                        last_sell_price NUMERIC,  -- 最後揭示賣價
                        last_sell_volume INT,     -- 最後揭示賣量
                        pe_ratio NUMERIC,         -- 本益比
                        PRIMARY KEY (stock_id, date)
                    );
                ''')
                # 建立籌碼表 (細分買進/賣出)
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_institutional (
                        stock_id VARCHAR(20), date VARCHAR(20),
                        foreign_buy BIGINT, foreign_sell BIGINT, foreign_net BIGINT, -- 外資
                        trust_buy BIGINT, trust_sell BIGINT, trust_net BIGINT,       -- 投信
                        dealer_net BIGINT, -- 自營商買賣超
                        PRIMARY KEY (stock_id, date)
                    );
                ''')
                conn.commit()
        print("✅ 資料庫全量欄位檢查完成")
    except Exception as e:
        print(f"❌ 初始化失敗: {e}")

if __name__ == "__main__":
    init_db()