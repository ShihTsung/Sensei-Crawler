"""
intraday_sync.py
盤中每小時快照：09:00 ~ 13:30（台北時間），每整點執行一次。

雲端部署說明：
  - Docker service：直接 `python intraday_sync.py` 跑 schedule loop
  - GCP Cloud Scheduler：只呼叫 run_sync()，不需 schedule loop
"""

import logging
import time
from datetime import datetime

import pytz
import requests
import schedule
import yfinance as yf
from psycopg2.extras import execute_values

from database import get_connection, init_db
from http_utils import with_retry
from sync_tdcc import sync_tdcc_weekly

logger = logging.getLogger(__name__)

TW_TZ        = pytz.timezone("Asia/Taipei")
MARKET_OPEN  = (9,  0)
MARKET_CLOSE = (13, 30)
BATCH_SIZE   = 200
HEADERS      = {"User-Agent": "Mozilla/5.0"}


def is_trading_time() -> bool:
    now = datetime.now(TW_TZ)
    if now.weekday() > 4:
        return False
    t = (now.hour, now.minute)
    return MARKET_OPEN <= t <= MARKET_CLOSE


@with_retry()
def _fetch_twse() -> list:
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    return requests.get(url, headers=HEADERS, timeout=20).json()


@with_retry()
def _fetch_tpex() -> list:
    url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_realtime_quotes"
    return requests.get(url, headers=HEADERS, timeout=20).json()


def fetch_ticker_map() -> dict[str, str]:
    """回傳 {stock_id: yfinance_suffix}，例如 {"2330": "TW", "6505": "TWO"}"""
    mapping: dict[str, str] = {}

    try:
        for item in _fetch_twse():
            sid = str(item.get("Code", "")).strip()
            if sid.isdigit() and 4 <= len(sid) <= 6:
                mapping[sid] = "TW"
        logger.info("上市 %d 檔", sum(1 for v in mapping.values() if v == 'TW'))
    except Exception as e:
        logger.warning("上市清單抓取失敗: %s", e)

    try:
        for item in _fetch_tpex():
            sid = str(item.get("SecuritiesCompanyCode", "")).strip()
            if sid.isdigit() and 4 <= len(sid) <= 6:
                mapping[sid] = "TWO"
        logger.info("上櫃 %d 檔", sum(1 for v in mapping.values() if v == 'TWO'))
    except Exception as e:
        logger.warning("上櫃清單抓取失敗: %s", e)

    return mapping


def fetch_batch(yf_tickers: list[str]) -> dict[str, float]:
    prices: dict[str, float] = {}
    for i in range(0, len(yf_tickers), BATCH_SIZE):
        chunk = yf_tickers[i: i + BATCH_SIZE]
        try:
            raw = yf.download(chunk, period="1d", interval="1h",
                              progress=False, auto_adjust=True, threads=True)
            if raw.empty:
                continue
            close = raw["Close"] if "Close" in raw.columns else raw.get("close")
            if close is None:
                continue
            if hasattr(close, "columns"):
                last_row = close.iloc[-1]
                for ticker in chunk:
                    val = last_row.get(ticker)
                    if val and str(val) != "nan":
                        prices[ticker] = float(val)
            else:
                val = close.iloc[-1]
                if val and str(val) != "nan":
                    prices[chunk[0]] = float(val)
        except Exception as e:
            logger.warning("批次 %d 失敗: %s", i // BATCH_SIZE + 1, e)
    return prices


def save_snapshot(snapshot_time: datetime, prices: dict[str, float]) -> int:
    if not prices:
        return 0
    rows = [(ticker.split(".")[0], snapshot_time, price) for ticker, price in prices.items()]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            execute_values(cur, """
                INSERT INTO twse_intraday (stock_id, snapshot_time, close_price)
                VALUES %s
                ON CONFLICT (stock_id, snapshot_time) DO UPDATE
                    SET close_price = EXCLUDED.close_price
            """, rows)
            conn.commit()
    return len(rows)


_ticker_map: dict[str, str] = {}


def run_sync():
    global _ticker_map

    if not is_trading_time():
        logger.info("%s 非交易時間，跳過", datetime.now(TW_TZ).strftime('%H:%M'))
        return

    now_tw = datetime.now(TW_TZ)
    logger.info("%s 開始盤中快照", now_tw.strftime('%Y-%m-%d %H:%M'))

    if not _ticker_map or now_tw.hour == 9:
        logger.info("更新股票清單...")
        _ticker_map = fetch_ticker_map()

    if not _ticker_map:
        logger.error("股票清單為空，放棄")
        return

    yf_tickers = [f"{sid}.{sfx}" for sid, sfx in _ticker_map.items()]
    logger.info("抓取 %d 檔行情（分批 %d 檔）...", len(yf_tickers), BATCH_SIZE)

    prices = fetch_batch(yf_tickers)
    # 保留時區資訊，存為 TIMESTAMPTZ
    count = save_snapshot(now_tw, prices)
    elapsed = (datetime.now(TW_TZ) - now_tw).seconds
    logger.info("完成：%d 筆，耗時 %d 秒", count, elapsed)


def run_tdcc_sync():
    logger.info("%s 開始集保週資料同步", datetime.now(TW_TZ).strftime('%Y-%m-%d %H:%M'))
    try:
        sync_tdcc_weekly()
    except Exception as e:
        logger.error("集保同步失敗: %s", e)


def start_scheduler():
    init_db()
    logger.info("盤中快照排程啟動（09:00 ~ 13:30 每整點）")
    logger.info("集保週資料排程：每週五 18:00 自動同步")

    for hour in range(9, 14):
        schedule.every().day.at(f"{hour:02d}:00").do(run_sync)
    schedule.every().day.at("13:30").do(run_sync)
    schedule.every().friday.at("18:00").do(run_tdcc_sync)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    start_scheduler()
