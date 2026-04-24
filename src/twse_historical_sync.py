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


def find_last_trading_date(max_lookback: int = 10):
    dt = datetime.now() + timedelta(hours=8)
    if dt.hour < 15:
        dt -= timedelta(days=1)
    while dt.weekday() > 4:
        dt -= timedelta(days=1)

    for _ in range(max_lookback):
        date_str = dt.strftime("%Y%m%d")
        logger.info("嘗試查詢 %s 行情資料...", date_str)
        try:
            data = fetch_prices(date_str)
            if extract_price_items(data) is not None:
                items = extract_price_items(data)
                logger.info("找到交易日: %s（%d 筆行情）", date_str, len(items))
                return date_str, items
            logger.warning("%s 無行情資料（可能為休市日），往前推一天...", date_str)
        except Exception as e:
            logger.warning("%s 請求失敗: %s，往前推一天...", date_str, e)
        dt -= timedelta(days=1)
        while dt.weekday() > 4:
            dt -= timedelta(days=1)

    return None, None


def sync_historical():
    date_str, items = find_last_trading_date()
    if not date_str:
        logger.error("找不到最近的交易日資料，放棄同步。")
        return

    logger.info("準備處理歷史交易日: %s", date_str)

    try:
        price_rows = parse_price_rows(items, date_str)
        with get_connection() as conn:
            write_prices(conn, price_rows)
        logger.info("行情導入完成 (%d 筆)", len(price_rows))

        logger.info("等待 8 秒後抓取籌碼...")
        time.sleep(8)

        c_data = fetch_chips(date_str)
        c_items = extract_chip_items(c_data)
        if c_items:
            chip_rows = parse_chip_rows(c_items, date_str)
            with get_connection() as conn:
                write_chips(conn, chip_rows)
            logger.info("籌碼導入完成 (%d 筆)", len(chip_rows))
        else:
            logger.warning("籌碼請求失敗，原因: %s", c_data.get('stat'))

    except Exception as e:
        logger.exception("程式崩潰: %s", e)


if __name__ == "__main__":
    sync_historical()
