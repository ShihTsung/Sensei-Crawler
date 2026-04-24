import logging
import re

import requests
from psycopg2.extras import execute_values

from http_utils import with_retry

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
BATCH = 200


def clean(val):
    if val is None:
        return None
    val = re.sub(r'<[^>]+>', '', str(val))
    val = val.replace(',', '').strip()
    return None if val in ['--', '---', ''] else val


@with_retry()
def fetch_prices(date_str: str) -> dict:
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALL"
    return requests.get(url, headers=HEADERS, timeout=30).json()


@with_retry()
def fetch_chips(date_str: str) -> dict:
    url = f"https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALL"
    return requests.get(url, headers=HEADERS, timeout=30).json()


def parse_price_rows(items: list, date_str: str) -> list:
    return [
        (row[0].strip(), row[1].strip(), date_str,
         clean(row[2]), clean(row[3]), clean(row[4]),
         clean(row[5]), clean(row[6]), clean(row[7]), clean(row[8]),
         clean(row[9]), clean(row[10]), clean(row[11]), clean(row[12]),
         clean(row[13]), clean(row[14]), clean(row[15]))
        for row in items
        if len(row) >= 16 and row[0].strip().isdigit() and 4 <= len(row[0].strip()) <= 5
    ]


def parse_chip_rows(items: list, date_str: str) -> list:
    return [
        (row[0].strip(), date_str,
         clean(row[2]), clean(row[3]), clean(row[4]),
         clean(row[8]), clean(row[9]), clean(row[10]), clean(row[11]))
        for row in items
        if row[0].strip().isdigit() and 4 <= len(row[0].strip()) <= 6
    ]


def write_prices(conn, price_rows: list) -> None:
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


def write_chips(conn, chip_rows: list) -> None:
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


def extract_price_items(data: dict) -> list | None:
    """從 TWSE MI_INDEX 回應中取出有效的行情列表。"""
    if data.get('stat') != 'OK' or 'tables' not in data:
        return None
    return next(
        (t['data'] for t in data['tables']
         if t.get('data') and len(t['data']) > 100 and len(t['data'][0]) >= 15),
        None,
    )


def extract_chip_items(data: dict) -> list | None:
    """從 TWSE T86 回應中取出有效的籌碼列表。"""
    if data.get('stat') != 'OK':
        return None
    return data.get('data') or (
        data['tables'][0].get('data') if data.get('tables') else None
    )
