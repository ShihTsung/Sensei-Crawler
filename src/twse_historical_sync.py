import logging
import re
import time
from datetime import datetime, timedelta

import requests
from psycopg2.extras import execute_values

from database import get_connection
from http_utils import with_retry

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def clean(val):
    if val is None:
        return None
    val = re.sub(r'<[^>]+>', '', str(val))
    val = val.replace(',', '').strip()
    return None if val in ['--', '---', ''] else val


@with_retry()
def _fetch_prices(date_str: str) -> dict:
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALL"
    return requests.get(url, headers=HEADERS, timeout=30).json()


@with_retry()
def _fetch_chips(date_str: str) -> dict:
    url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
    return requests.get(url, headers=HEADERS, timeout=30).json()


def find_last_trading_date(max_lookback=10):
    dt = datetime.now() + timedelta(hours=8)
    if dt.hour < 15:
        dt -= timedelta(days=1)
    while dt.weekday() > 4:
        dt -= timedelta(days=1)

    for _ in range(max_lookback):
        while dt.weekday() > 4:
            dt -= timedelta(days=1)
        date_str = dt.strftime("%Y%m%d")
        logger.info("嘗試查詢 %s 行情資料...", date_str)
        try:
            data = _fetch_prices(date_str)
            if data.get('stat') == 'OK' and 'tables' in data:
                items = next(
                    (t['data'] for t in data['tables']
                     if t.get('data') and len(t['data']) > 100 and len(t['data'][0]) >= 15),
                    None
                )
                if items:
                    logger.info("找到交易日: %s（%d 筆行情）", date_str, len(items))
                    return date_str, items
            logger.warning("%s 無行情資料（可能為休市日），往前推一天...", date_str)
        except Exception as e:
            logger.warning("%s 請求失敗: %s，往前推一天...", date_str, e)
        dt -= timedelta(days=1)

    return None, None


def sync_historical():
    date_str, items = find_last_trading_date()
    if not date_str:
        logger.error("找不到最近的交易日資料，放棄同步。")
        return

    logger.info("準備處理歷史交易日: %s", date_str)

    try:
        price_rows = [
            (row[0].strip(), row[1].strip(), date_str,
             clean(row[2]), clean(row[3]), clean(row[4]),
             clean(row[5]), clean(row[6]), clean(row[7]), clean(row[8]),
             clean(row[9]), clean(row[10]), clean(row[11]), clean(row[12]),
             clean(row[13]), clean(row[14]), clean(row[15]))
            for row in items
            if len(row) >= 16 and row[0].strip().isdigit() and 4 <= len(row[0].strip()) <= 5
        ]

        BATCH = 200
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = 0")
                for i in range(0, len(price_rows), BATCH):
                    execute_values(cur, """
                        INSERT INTO twse_prices (
                            stock_id, stock_name, date, trade_volume, transaction_count, trade_value,
                            open_price, high_price, low_price, close_price, price_change_dir, price_change,
                            last_buy_price, last_buy_volume, last_sell_price, last_sell_volume, pe_ratio
                        ) VALUES %s
                        ON CONFLICT (stock_id, date) DO UPDATE SET close_price = EXCLUDED.close_price
                    """, price_rows[i:i + BATCH])
                    conn.commit()
                logger.info("行情導入完成 (%d 筆)", len(price_rows))

        logger.info("等待 8 秒後抓取籌碼...")
        time.sleep(8)

        c_data = _fetch_chips(date_str)
        if c_data.get('stat') == 'OK':
            c_items = c_data.get('data') or (
                c_data.get('tables')[0].get('data') if c_data.get('tables') else None
            )
            if c_items:
                chip_rows = [
                    (row[0].strip(), date_str,
                     clean(row[2]), clean(row[3]), clean(row[4]),
                     clean(row[8]), clean(row[9]), clean(row[10]), clean(row[11]))
                    for row in c_items
                    if row[0].strip().isdigit() and 4 <= len(row[0].strip()) <= 6
                ]
                with get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SET statement_timeout = 0")
                        for i in range(0, len(chip_rows), BATCH):
                            execute_values(cur, """
                                INSERT INTO twse_institutional (
                                    stock_id, date, foreign_buy, foreign_sell, foreign_net,
                                    trust_buy, trust_sell, trust_net, dealer_net
                                ) VALUES %s
                                ON CONFLICT (stock_id, date) DO UPDATE SET foreign_net = EXCLUDED.foreign_net
                            """, chip_rows[i:i + BATCH])
                            conn.commit()
                        logger.info("籌碼導入完成 (%d 筆)", len(chip_rows))
            else:
                logger.warning("證交所回應 OK 但找不到籌碼數據內容。")
        else:
            logger.error("籌碼請求失敗，原因: %s", c_data.get('stat'))

    except Exception as e:
        logger.exception("程式崩潰: %s", e)


if __name__ == "__main__":
    sync_historical()
