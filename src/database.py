import logging
import os
from contextlib import contextmanager

import psycopg2.extensions
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
            maxconn=int(os.getenv("DB_MAX_CONN", "10")),
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
    """從 connection pool 取得連線，離開時自動歸還。
    例外或 transaction 異常時會關閉連線後再歸還，避免污染池內其他連線。"""
    p = _get_pool()
    conn = p.getconn()
    failed = False
    try:
        yield conn
    except Exception:
        failed = True
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        # 連線已斷或仍在 aborted transaction → 關掉再還回池
        bad_state = (
            conn.closed
            or conn.info.transaction_status
               != psycopg2.extensions.TRANSACTION_STATUS_IDLE
        )
        p.putconn(conn, close=failed or bad_state)


def init_db():
    """建立所有資料表（idempotent）。失敗會直接拋例外。"""
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
            cur.execute('''
                CREATE TABLE IF NOT EXISTS stock_category (
                    stock_id      VARCHAR(10) PRIMARY KEY,
                    category_name VARCHAR(50),
                    company_name  VARCHAR(100)
                );
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS company_info (
                    stock_id     VARCHAR(10) PRIMARY KEY,
                    tax_id       VARCHAR(20),
                    company_name VARCHAR(100),
                    market_type  VARCHAR(10),
                    address      VARCHAR(300),
                    phone        VARCHAR(50),
                    fax          VARCHAR(50),
                    chairman     VARCHAR(50),
                    ceo          VARCHAR(50),
                    spokesperson VARCHAR(50),
                    capital      BIGINT,
                    listed_date  VARCHAR(20),
                    website      VARCHAR(200),
                    updated_at   TIMESTAMP DEFAULT NOW()
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


def is_valid_stock_id(sid: str) -> bool:
    """台灣股票代碼驗證：4~6 碼純數字"""
    return bool(sid) and sid.isdigit() and 4 <= len(sid) <= 6


if __name__ == "__main__":
    init_db()
