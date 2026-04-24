import logging
import time
from datetime import datetime, timedelta

from database import get_connection
from twse_sync_core import (
    extract_chip_items,
    extract_price_items,
    fetch_chips,
    fetch_prices,
    parse_chip_rows,
    parse_price_rows,
    write_chips,
    write_prices,
)

logger = logging.getLogger(__name__)


def run_sync_for_date(date_str: str) -> bool:
    try:
        p_data = fetch_prices(date_str)
        items = extract_price_items(p_data)
        if not items:
            return False

        price_rows = parse_price_rows(items, date_str)
        with get_connection() as conn:
            write_prices(conn, price_rows)
        logger.info("%s 行情導入完成（%d 筆）", date_str, len(price_rows))

        time.sleep(8)

        c_data = fetch_chips(date_str)
        c_items = extract_chip_items(c_data)
        if c_items:
            chip_rows = parse_chip_rows(c_items, date_str)
            with get_connection() as conn:
                write_chips(conn, chip_rows)
            logger.info("%s 籌碼導入完成（%d 筆）", date_str, len(chip_rows))

        return True

    except Exception as e:
        logger.error("%s 執行出錯: %s", date_str, e)
        return False


def sync_prev_month(start: datetime = None, end: datetime = None):
    """同步指定日期區間；預設為上個月全月。"""
    if start is None:
        today = datetime.today()
        first_of_this_month = today.replace(day=1)
        end_of_last_month = first_of_this_month - timedelta(days=1)
        start = end_of_last_month.replace(day=1)
    if end is None:
        today = datetime.today()
        end = today.replace(day=1) - timedelta(days=1)

    current_date = start
    logger.info("補抓任務：%s → %s",
                current_date.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))

    while current_date <= end:
        if current_date.weekday() < 5:
            date_str = current_date.strftime("%Y%m%d")
            if run_sync_for_date(date_str):
                time.sleep(10)
            else:
                logger.info("%s 無交易數據，跳過。", date_str)
        current_date += timedelta(days=1)


if __name__ == "__main__":
    sync_prev_month()
