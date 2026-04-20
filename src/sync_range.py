import logging
import re
import time
from datetime import datetime, timedelta

import requests

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


def run_sync_for_date(date_str: str) -> bool:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                p_data = _fetch_prices(date_str)

                if p_data.get('stat') == 'OK' and 'tables' in p_data:
                    items = next(
                        (t['data'] for t in p_data['tables']
                         if t.get('data') and len(t['data']) > 100),
                        None
                    )
                    if items:
                        for row in items:
                            if len(row) < 15 or len(row[0].strip()) > 6:
                                continue
                            cur.execute("""
                                INSERT INTO twse_prices (
                                    stock_id, stock_name, date, trade_volume, transaction_count, trade_value,
                                    open_price, high_price, low_price, close_price, price_change_dir, price_change,
                                    last_buy_price, last_buy_volume, last_sell_price, last_sell_volume, pe_ratio
                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (stock_id, date) DO UPDATE SET close_price = EXCLUDED.close_price
                            """, (row[0].strip(), row[1].strip(), date_str,
                                  clean(row[2]), clean(row[3]), clean(row[4]),
                                  clean(row[5]), clean(row[6]), clean(row[7]), clean(row[8]),
                                  clean(row[9]), clean(row[10]), clean(row[11]), clean(row[12]),
                                  clean(row[13]), clean(row[14]), clean(row[15])))
                        conn.commit()
                        logger.info("%s 行情導入完成", date_str)
                    else:
                        return False
                else:
                    return False

                time.sleep(8)
                c_data = _fetch_chips(date_str)
                if c_data.get('stat') == 'OK':
                    c_items = c_data.get('data') or (
                        c_data.get('tables')[0].get('data') if c_data.get('tables') else None
                    )
                    if c_items:
                        for row in c_items:
                            sid = row[0].strip()
                            if not sid.isdigit() or not (4 <= len(sid) <= 6):
                                continue
                            cur.execute("""
                                INSERT INTO twse_institutional (
                                    stock_id, date, foreign_buy, foreign_sell, foreign_net,
                                    trust_buy, trust_sell, trust_net, dealer_net
                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (stock_id, date) DO UPDATE SET foreign_net = EXCLUDED.foreign_net
                            """, (sid, date_str,
                                  clean(row[2]), clean(row[3]), clean(row[4]),
                                  clean(row[8]), clean(row[9]), clean(row[10]), clean(row[11])))
                        conn.commit()
                        logger.info("%s 籌碼導入完成", date_str)
                return True
    except Exception as e:
        logger.error("%s 執行出錯: %s", date_str, e)
        return False


def sync_march_data(start: datetime = None, end: datetime = None):
    if start is None:
        today = datetime.today()
        first_of_this_month = today.replace(day=1)
        end_of_last_month = first_of_this_month - timedelta(days=1)
        start = end_of_last_month.replace(day=1)
    if end is None:
        today = datetime.today()
        first_of_this_month = today.replace(day=1)
        end = first_of_this_month - timedelta(days=1)

    current_date = start
    logger.info("啟動補抓任務：從 %s 到 %s",
                current_date.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))

    while current_date <= end:
        if current_date.weekday() < 5:
            date_str = current_date.strftime("%Y%m%d")
            success = run_sync_for_date(date_str)
            if success:
                time.sleep(10)
            else:
                logger.info("%s 無交易數據，跳過。", date_str)
        current_date += timedelta(days=1)


if __name__ == "__main__":
    sync_march_data()
