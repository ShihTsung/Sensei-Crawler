import logging
from io import StringIO

import pandas as pd
import requests

from database import get_connection
from http_utils import with_retry

logger = logging.getLogger(__name__)

_URL = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/csv, */*",
}


@with_retry()
def _fetch_csv() -> str:
    resp = requests.get(_URL, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = 'utf-8-sig'
    return resp.text


def sync_tdcc_weekly():
    """集保戶股權分散表（週更），欄位：資料日期, 證券代號, 持股分級, 人數, 股數, 占比%"""
    logger.info("正在連線集保開放資料...")
    try:
        text = _fetch_csv()
        df = pd.read_csv(StringIO(text))
        df.columns = ['date', 'stock_id', 'level', 'holders', 'shares', 'rate']
        logger.info("下載成功，共 %d 筆，日期: %s", len(df), df['date'].iloc[0])
    except Exception as e:
        logger.error("下載失敗: %s", e)
        return

    logger.info("開始寫入資料庫...")
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                count = skip = 0
                for _, row in df.iterrows():
                    sid = str(row['stock_id']).strip()
                    if not sid.isdigit() or not (4 <= len(sid) <= 6):
                        skip += 1
                        continue
                    try:
                        cur.execute("""
                            INSERT INTO twse_weekly_concentration
                                (stock_id, date, level, holders, shares, rate)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (stock_id, date, level) DO NOTHING
                        """, (sid, str(row['date']).strip(), int(row['level']),
                              int(row['holders']), int(row['shares']), float(row['rate'])))
                        count += 1
                    except Exception as row_err:
                        logger.warning("跳過異常資料 %s: %s", sid, row_err)
                conn.commit()
        logger.info("同步完成！寫入 %d 筆，跳過 %d 筆（非標準股票代碼）", count, skip)
    except Exception as e:
        logger.error("資料庫寫入失敗: %s", e)


if __name__ == "__main__":
    sync_tdcc_weekly()
