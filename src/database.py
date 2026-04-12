import psycopg2
import os
from dotenv import load_dotenv

# Docker 內：DB_HOST 由 docker-compose env_file 注入（值為 "db"）
# 本機直接跑：.env 裡設 DB_HOST=localhost
load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "sensei_db"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "port":     os.getenv("DB_PORT", "5432"),
}

def get_connection():
    """建立資料庫連線"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "sensei_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
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
                # 6. 盤中快照表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_intraday (
                        stock_id      VARCHAR(10),
                        snapshot_time TIMESTAMP,
                        close_price   NUMERIC,
                        PRIMARY KEY   (stock_id, snapshot_time)
                    );
                ''')
                # 7. 公司基本資料表
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS companies (
                        stock_id     TEXT PRIMARY KEY,
                        company_name TEXT,
                        market_type  TEXT,
                        ai_relevance NUMERIC DEFAULT 0.0,
                        industry     TEXT,
                        current_price NUMERIC,
                        ai_analysis_note TEXT
                    );
                ''')
                # 補欄位（舊資料庫升級用）
                for col, typedef in [
                    ("current_price",    "NUMERIC"),
                    ("ai_analysis_note", "TEXT"),
                ]:
                    cur.execute(f"""
                        ALTER TABLE companies ADD COLUMN IF NOT EXISTS {col} {typedef};
                    """)
                # ── INDEX ────────────────────────────────────────────
                cur.execute('CREATE INDEX IF NOT EXISTS idx_twse_prices_date ON twse_prices (date);')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_twse_institutional_date ON twse_institutional (date);')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_twse_intraday_time ON twse_intraday (snapshot_time);')
                conn.commit()
        print("✅ 所有資料表結構檢查完成")
    except Exception as e:
        print(f"❌ 初始化失敗: {e}")

def upsert_companies(rows):
    """
    批次寫入公司基本資料。
    rows: list of (stock_id, company_name, industry, market_type)
          或 (stock_id, company_name, ind_code, *extra_ignored, market_type, sector_group)
    只取前 4 個欄位寫入，其餘忽略以維持向下相容。
    回傳 True 表示成功。
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    stock_id, company_name, industry, market_type = row[0], row[1], row[2], row[6] if len(row) > 4 else row[3]
                    if not stock_id:
                        continue
                    cur.execute("""
                        INSERT INTO companies (stock_id, company_name, industry, market_type)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (stock_id) DO UPDATE SET
                            company_name = EXCLUDED.company_name,
                            industry     = EXCLUDED.industry,
                            market_type  = EXCLUDED.market_type
                    """, (stock_id, company_name, industry, market_type))
            conn.commit()
        return True
    except Exception as e:
        print(f"❌ upsert_companies 失敗: {e}")
        return False

if __name__ == "__main__":
    init_db()