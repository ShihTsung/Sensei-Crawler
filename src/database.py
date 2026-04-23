import logging
import os
from contextlib import contextmanager

from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

_pool: pool.ThreadedConnectionPool | None = None


def _get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "sensei_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
            port=os.getenv("DB_PORT", "5432"),
            connect_timeout=10,
        )
    return _pool


@contextmanager
def get_connection():
    """從 connection pool 取得連線，離開時自動歸還。"""
    p = _get_pool()
    conn = p.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


def init_db():
    """建立所有資料表（idempotent）。"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
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
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_institutional (
                        stock_id VARCHAR(20), date VARCHAR(20),
                        foreign_buy BIGINT, foreign_sell BIGINT, foreign_net BIGINT,
                        trust_buy BIGINT, trust_sell BIGINT, trust_net BIGINT,
                        dealer_net BIGINT, PRIMARY KEY (stock_id, date)
                    );
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_insider_holding (
                        stock_id VARCHAR(10), date VARCHAR(20),
                        title VARCHAR(50), name VARCHAR(100),
                        held_shares BIGINT, pledged_shares BIGINT, pledge_rate NUMERIC,
                        PRIMARY KEY (stock_id, date, name)
                    );
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_top10_shareholders (
                        stock_id VARCHAR(10), year_period VARCHAR(20),
                        rank INT, name VARCHAR(100),
                        held_shares BIGINT, held_rate NUMERIC,
                        PRIMARY KEY (stock_id, year_period, name)
                    );
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_weekly_concentration (
                        stock_id VARCHAR(10), date VARCHAR(20),
                        level INT, holders INT, shares BIGINT, rate NUMERIC,
                        PRIMARY KEY (stock_id, date, level)
                    );
                ''')
                # snapshot_time 用 TIMESTAMPTZ 保留時區資訊
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS twse_intraday (
                        stock_id      VARCHAR(10),
                        snapshot_time TIMESTAMPTZ,
                        close_price   NUMERIC,
                        PRIMARY KEY   (stock_id, snapshot_time)
                    );
                ''')
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
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS current_price NUMERIC;")
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS ai_analysis_note TEXT;")
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS news_summaries (
                        id         SERIAL PRIMARY KEY,
                        title      TEXT,
                        company    TEXT,
                        summary    TEXT[],
                        sentiment  TEXT,
                        url        TEXT UNIQUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                ''')

                # 舊資料庫升級：TIMESTAMP → TIMESTAMPTZ
                cur.execute("""
                    SELECT data_type FROM information_schema.columns
                    WHERE table_name='twse_intraday' AND column_name='snapshot_time';
                """)
                row = cur.fetchone()
                if row and row[0].lower() == "timestamp without time zone":
                    cur.execute(
                        "ALTER TABLE twse_intraday ALTER COLUMN snapshot_time TYPE TIMESTAMPTZ "
                        "USING snapshot_time AT TIME ZONE 'Asia/Taipei';"
                    )
                    logger.info("已將 twse_intraday.snapshot_time 升級為 TIMESTAMPTZ")

                cur.execute('CREATE INDEX IF NOT EXISTS idx_twse_prices_date ON twse_prices (date);')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_twse_institutional_date ON twse_institutional (date);')
                cur.execute('CREATE INDEX IF NOT EXISTS idx_twse_intraday_time ON twse_intraday (snapshot_time);')
                conn.commit()
        logger.info("所有資料表結構檢查完成")
    except Exception as e:
        logger.error("初始化失敗: %s", e)


def is_valid_stock_id(sid: str) -> bool:
    """台灣股票代碼驗證：4~6 碼純數字"""
    return bool(sid) and sid.isdigit() and 4 <= len(sid) <= 6


def upsert_companies(rows):
    """批次寫入公司基本資料，回傳 True 表示成功。"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    stock_id, company_name, industry, market_type = (
                        row[0], row[1], row[2], row[6] if len(row) > 4 else row[3]
                    )
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
        logger.error("upsert_companies 失敗: %s", e)
        return False


if __name__ == "__main__":
    init_db()
